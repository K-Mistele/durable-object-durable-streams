/**
 * `StreamObject`: one Durable Object per stream path, holding that stream's tables and waiters.
 * Its RPC methods are `StreamOperations` taking and returning encoded values, because Cloudflare
 * RPC rejects class instances such as `Option` and `DateTime` (engine-design.md decision 20).
 *
 * Forks (engine-design.md, Forks): a fork's object calls its source's object directly, through
 * the namespace alchemy gives each Durable Object (`DurableObjectScope`), and pulls the rows it
 * copies. The calls between objects, the alarm that retries a copy, and the attempt limits live
 * here; `StreamOperations` only ever touches its own stream.
 */
import * as SqliteClient from '@effect/sql-sqlite-do/SqliteClient';
import * as Cloudflare from 'alchemy/Cloudflare';
import { Clock, DateTime, Duration, Effect, Layer, Option, Ref, Schema, Stream } from 'effect';
import { StreamPath } from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    BadRequestError,
    ContentTypeMismatchError,
    InvalidEpochSeqError,
    InvalidForkOffsetError,
    SequenceGapError,
    SourceDeletedError,
    StaleEpochError,
    StreamClosedError,
    StreamConfigConflictError,
    StreamGoneError,
    StreamNotFoundError,
    StreamSeqConflictError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import {
    AddForkRequest,
    AppendRequest,
    AppendResult,
    CreateForkRequest,
    CreateStreamRequest,
    CreateStreamResult,
    EmptyRequest,
    ForkPoint,
    ForkRow,
    ReadBodyRequest,
    ReadForkRowsRequest,
    ReadSseEventsRequest,
    ReadRange,
    RemoveForkRequest,
    type SourceToRelease,
    StartReadRequest,
    StreamMetadata,
    WaitForNewMessagesRequest,
    WaitResult,
} from '@kylelayer/effect-durable-streams-server/stream-operation-requests';
import {
    type AddForkError,
    type AppendToStreamError,
    CopyAttempt,
    type CreateForkError,
    type CreateStreamError,
    ForkClaim,
    makeStreamOperations,
    type MissingStreamError,
} from '@kylelayer/effect-durable-streams-server/stream-operations';
import { layerSql } from '@kylelayer/effect-durable-streams-server/stream-storage';
import { sendEncoded, serveEncoded } from './encoded-rpc';

/** `StreamOperations` with each request and result in its encoded form. */
export type EncodedStreamOperations = {
    readonly createStream: (
        request: typeof CreateStreamRequest.Encoded,
    ) => Effect.Effect<typeof CreateStreamResult.Encoded, CreateStreamError>;
    /** Makes this object a fork of `request.source`: answers once the copy is done (201), or with 404 if it is given up. */
    readonly createFork: (
        request: typeof CreateForkRequest.Encoded,
    ) => Effect.Effect<typeof CreateStreamResult.Encoded, CreateForkError>;
    readonly appendToStream: (
        request: typeof AppendRequest.Encoded,
    ) => Effect.Effect<typeof AppendResult.Encoded, AppendToStreamError>;
    readonly getMetadata: (
        request: typeof EmptyRequest.Encoded,
    ) => Effect.Effect<typeof StreamMetadata.Encoded, MissingStreamError>;
    readonly startRead: (
        request: typeof StartReadRequest.Encoded,
    ) => Effect.Effect<typeof ReadRange.Encoded, MissingStreamError>;
    readonly readBody: (
        request: typeof ReadBodyRequest.Encoded,
    ) => Stream.Stream<Uint8Array, StreamNotFoundError>;
    readonly readSseEvents: (
        request: typeof ReadSseEventsRequest.Encoded,
    ) => Stream.Stream<Uint8Array, StreamNotFoundError>;
    readonly waitForNewMessages: (
        request: typeof WaitForNewMessagesRequest.Encoded,
    ) => Effect.Effect<typeof WaitResult.Encoded, MissingStreamError>;
    readonly deleteStream: (
        request: typeof EmptyRequest.Encoded,
    ) => Effect.Effect<void, MissingStreamError>;
    /** From a fork's object: check the fork request and record the fork. */
    readonly addFork: (
        request: typeof AddForkRequest.Encoded,
    ) => Effect.Effect<typeof ForkPoint.Encoded, AddForkError>;
    /** From a fork's object: the rows it copies, as JSON lines. */
    readonly readForkRows: (
        request: typeof ReadForkRowsRequest.Encoded,
    ) => Stream.Stream<typeof ForkRow.Encoded, StreamNotFoundError>;
    /** From a deleted fork's object. */
    readonly removeFork: (request: typeof RemoveForkRequest.Encoded) => Effect.Effect<void>;
};

/** `errors` lists every failure a method can return, so the caller rebuilds each as its class. */
export class StreamObject extends Cloudflare.DurableObject<StreamObject, EncodedStreamOperations>()(
    'StreamObject',
    {
        errors: [
            BadRequestError,
            StreamConfigConflictError,
            StreamNotFoundError,
            StreamGoneError,
            StreamClosedError,
            ContentTypeMismatchError,
            StaleEpochError,
            InvalidEpochSeqError,
            SequenceGapError,
            StreamSeqConflictError,
            SourceDeletedError,
            InvalidForkOffsetError,
        ],
    },
) {}

/** How long the alarm waits before checking on a fork a request is still making. */
const copyCheckDelay = Duration.seconds(30);

/** How soon the alarm retries a copy attempt that failed. */
const copyRetryDelay = Duration.seconds(2);

/** `removeFork` calls are retried up to this many times, the gap doubling from a minute: about 17 hours in all. */
const maxReleaseAttempts = 10;

export const StreamObjectLive = StreamObject.make(
    Effect.gen(function* () {
        const state = yield* Cloudflare.DurableObjectState;
        const migrations = yield* Cloudflare.SqlMigrations('./migrations');
        // every StreamObject, so a fork can reach its source
        const scope = yield* Cloudflare.DurableObjectScope;
        // SAFETY: DurableObjectScope is this class's own namespace, so its objects have this
        // class's methods; alchemy types it without them, and its Git Durable Object casts it the same way.
        // oxlint-disable-next-line automation/no-type-assertion, typescript/no-unsafe-type-assertion -- see SAFETY
        const objects = scope as Cloudflare.DurableObject<EncodedStreamOperations>;

        // alchemy's Durable Object form: this Effect runs once per isolate and returns the one
        // that runs once per instance
        // oxlint-disable-next-line effecttsgo/return-effect-in-gen -- the inner Effect is the result
        return Effect.gen(function* () {
            yield* Effect.orDie(migrations.apply());
            // the Worker reaches each stream with getByName(path), so the object's name is its path
            const stream = yield* Effect.orDie(
                Schema.decodeUnknownEffect(StreamPath)(state.id.name),
            );
            // the instance scope owns the client: it lives as long as the object
            const storage = yield* Layer.build(
                layerSql.pipe(Layer.provide(SqliteClient.layer({ storage: state.raw.storage }))),
            );
            const operations = yield* makeStreamOperations({ stream }).pipe(
                Effect.provideContext(storage),
            );

            // One alarm per object. Set it no later than `at`, keeping an earlier one.
            const armAlarmAt = (at: number) =>
                Effect.gen(function* () {
                    const current = yield* Effect.promise(() => state.raw.storage.getAlarm());
                    if (current === null || current > at) {
                        yield* Effect.promise(() => state.raw.storage.setAlarm(at));
                    }
                });
            const armAlarmIn = (delay: Duration.Duration) =>
                Clock.currentTimeMillis.pipe(
                    Effect.flatMap((now) => armAlarmAt(now + Duration.toMillis(delay))),
                );

            // Set while this instance runs a copy (or makes a fork), so the alarm doesn't start a
            // second one. Memory only: if the object restarts, the copy died with it.
            const copyRunning = yield* Ref.make(false);
            const unlessCopyRunning = <A, E>(effect: Effect.Effect<A, E>) =>
                Effect.gen(function* () {
                    if (yield* Ref.getAndSet(copyRunning, true)) {
                        return Option.none<A>();
                    }
                    return yield* effect.pipe(
                        Effect.asSome,
                        Effect.ensuring(Ref.set(copyRunning, false)),
                    );
                });

            const removeForkFrom = (source: StreamPath) =>
                sendEncoded({
                    request: RemoveForkRequest,
                    result: Schema.Void,
                    value: { fork: stream },
                    send: (encoded) => objects.getByName(source).removeFork(encoded),
                });

            const scheduleRelease = ({
                source,
                attempts,
            }: {
                readonly source: StreamPath;
                readonly attempts: number;
            }) =>
                Effect.gen(function* () {
                    const delay = Duration.minutes(2 ** (attempts - 1));
                    const dueAt = (yield* Clock.currentTimeMillis) + Duration.toMillis(delay);
                    yield* operations.savePendingRelease({ source, attempts, dueAt });
                    yield* armAlarmAt(dueAt);
                });

            // tells the source this stream no longer copies from it; retried from the alarm
            const releaseSource = (source: SourceToRelease) =>
                Option.match(source, {
                    onNone: () => Effect.void,
                    onSome: (path) =>
                        removeForkFrom(path).pipe(
                            Effect.catchCause((cause) =>
                                Effect.logWarning('removeFork failed; will retry', cause).pipe(
                                    Effect.andThen(scheduleRelease({ source: path, attempts: 1 })),
                                ),
                            ),
                        ),
                });

            const retryDueReleases = Effect.gen(function* () {
                const now = yield* Clock.currentTimeMillis;
                const releases = yield* operations.loadPendingReleases();
                yield* Effect.forEach(
                    releases,
                    (release) => {
                        if (release.dueAt > now) {
                            return armAlarmAt(release.dueAt);
                        }
                        return removeForkFrom(release.source).pipe(
                            Effect.andThen(operations.deletePendingRelease(release.source)),
                            Effect.catchCause((cause) =>
                                release.attempts >= maxReleaseAttempts
                                    ? Effect.logError('removeFork failed; giving up', cause).pipe(
                                          Effect.andThen(
                                              operations.deletePendingRelease(release.source),
                                          ),
                                      )
                                    : scheduleRelease({
                                          source: release.source,
                                          attempts: release.attempts + 1,
                                      }),
                            ),
                        );
                    },
                    { discard: true },
                );
            });

            // purges an expired stream and releases its source; otherwise sets the alarm for its
            // expiry, where this runs again (a TTL slides, so the stream may have been read since)
            const checkExpiry = operations.expireIfDue().pipe(
                Effect.flatMap(({ released, checkAgainAt }) =>
                    releaseSource(released).pipe(
                        Effect.andThen(
                            Option.match(checkAgainAt, {
                                onNone: () => Effect.void,
                                onSome: (at) => armAlarmAt(DateTime.toEpochMillis(at)),
                            }),
                        ),
                    ),
                ),
            );

            const giveUpFork = operations
                .abandonFork()
                .pipe(Effect.flatMap(releaseSource), Effect.as(Option.none<CreateStreamResult>()));

            /** One attempt at copying this fork's rows from its source; Some once the fork is finished. */
            const attemptCopy = Effect.gen(function* () {
                const attempt = yield* operations.startCopyAttempt();
                return yield* CopyAttempt.$match(attempt, {
                    NothingToCopy: () => Effect.succeedNone,
                    // the request that claimed the path died before its source agreed
                    Unconfirmed: () => giveUpFork,
                    GiveUp: () => giveUpFork,
                    Copy: ({ source, after, to, lastBytes }) =>
                        Effect.gen(function* () {
                            const request = yield* Effect.orDie(
                                Schema.encodeEffect(ReadForkRowsRequest)({
                                    fork: stream,
                                    after,
                                    to,
                                    lastBytes,
                                }),
                            );
                            const rows = objects
                                .getByName(source)
                                .readForkRows(request)
                                .pipe(
                                    Stream.mapEffect((row) =>
                                        Effect.orDie(Schema.decodeEffect(ForkRow)(row)),
                                    ),
                                );
                            yield* operations.writeForkRows(rows);
                            return Option.some(yield* operations.finishFork());
                        }).pipe(
                            Effect.catchCause((cause) =>
                                Effect.logWarning(
                                    'fork copy attempt failed; will retry',
                                    cause,
                                ).pipe(
                                    Effect.andThen(armAlarmIn(copyRetryDelay)),
                                    Effect.as(Option.none<CreateStreamResult>()),
                                ),
                            ),
                        ),
                });
            });

            /** After `claimFork`: the source agrees, the fork point is saved, and the first copy attempt runs. */
            const makeFork = ({
                generation,
                request,
            }: {
                readonly generation: number;
                readonly request: CreateForkRequest;
            }) =>
                Effect.gen(function* () {
                    const made = yield* unlessCopyRunning(
                        Effect.gen(function* () {
                            const point = yield* sendEncoded({
                                request: AddForkRequest,
                                result: ForkPoint,
                                value: {
                                    fork: stream,
                                    offset: request.offset,
                                    subOffset: request.subOffset,
                                    contentType: request.contentType,
                                },
                                send: (encoded) =>
                                    objects.getByName(request.source).addFork(encoded),
                            }).pipe(
                                // the source refused, and wrote nothing
                                Effect.tapError(() => operations.abandonFork()),
                            );
                            yield* operations
                                .saveForkPoint({ generation, request, point })
                                .pipe(Effect.tapError(() => giveUpFork));
                            return yield* attemptCopy;
                        }),
                    );
                    const finished = Option.flatten(made);
                    if (Option.isSome(finished)) {
                        return finished.value;
                    }
                    // the alarm keeps copying; this request waits for it, as a repeat of it would
                    const waited = yield* operations.waitForFork(request);
                    return { ...waited, isNewStream: true };
                });

            const methods = {
                // an expired stream at the path is purged first, so a fork's source is released
                createStream: serveEncoded({
                    request: CreateStreamRequest,
                    result: CreateStreamResult,
                    operation: (request) =>
                        checkExpiry.pipe(
                            Effect.andThen(operations.createStream(request)),
                            Effect.tap(() => checkExpiry),
                        ),
                }),
                createFork: serveEncoded({
                    request: CreateForkRequest,
                    result: CreateStreamResult,
                    operation: (request) =>
                        Effect.gen(function* () {
                            // if this request dies before the fork is made, the alarm finishes or deletes it
                            yield* armAlarmIn(copyCheckDelay);
                            yield* checkExpiry;
                            const claim = yield* operations.claimFork(request);
                            const result = yield* ForkClaim.$match(claim, {
                                Exists: ({ result: existing }) => Effect.succeed(existing),
                                InProgress: () => operations.waitForFork(request),
                                Claimed: ({ generation }) => makeFork({ generation, request }),
                            });
                            yield* checkExpiry;
                            return result;
                        }),
                }),
                appendToStream: serveEncoded({
                    request: AppendRequest,
                    result: AppendResult,
                    operation: operations.appendToStream,
                }),
                getMetadata: serveEncoded({
                    request: EmptyRequest,
                    result: StreamMetadata,
                    operation: operations.getMetadata,
                }),
                startRead: serveEncoded({
                    request: StartReadRequest,
                    result: ReadRange,
                    operation: operations.startRead,
                }),
                // bytes cross Cloudflare RPC as they are
                readBody: (encoded: typeof ReadBodyRequest.Encoded) =>
                    Schema.decodeEffect(ReadBodyRequest)(encoded).pipe(
                        Effect.orDie,
                        Effect.map(operations.readBody),
                        Stream.unwrap,
                    ),
                readSseEvents: (encoded: typeof ReadSseEventsRequest.Encoded) =>
                    Schema.decodeEffect(ReadSseEventsRequest)(encoded).pipe(
                        Effect.orDie,
                        Effect.map(operations.readSseEvents),
                        Stream.unwrap,
                    ),
                waitForNewMessages: serveEncoded({
                    request: WaitForNewMessagesRequest,
                    result: WaitResult,
                    operation: operations.waitForNewMessages,
                }),
                deleteStream: serveEncoded({
                    request: EmptyRequest,
                    result: Schema.Void,
                    operation: () =>
                        operations.deleteStream({}).pipe(Effect.flatMap(releaseSource)),
                }),
                addFork: serveEncoded({
                    request: AddForkRequest,
                    result: ForkPoint,
                    operation: operations.addFork,
                }),
                readForkRows: (encoded: typeof ReadForkRowsRequest.Encoded) =>
                    Schema.decodeEffect(ReadForkRowsRequest)(encoded).pipe(
                        Effect.orDie,
                        Effect.map((request) =>
                            operations
                                .readForkRows(request)
                                .pipe(
                                    Stream.mapEffect((chunk) =>
                                        Effect.orDie(Schema.encodeEffect(ForkRow)(chunk)),
                                    ),
                                ),
                        ),
                        Stream.unwrap,
                    ),
                removeFork: serveEncoded({
                    request: RemoveForkRequest,
                    result: Schema.Void,
                    operation: (request) =>
                        operations.removeFork(request).pipe(Effect.flatMap(releaseSource)),
                }),
            } satisfies EncodedStreamOperations;

            return {
                ...methods,
                // expiry, due removeFork retries, then the next copy attempt of a fork being made
                alarm: () =>
                    Effect.gen(function* () {
                        yield* checkExpiry;
                        yield* retryDueReleases;
                        const ran = yield* unlessCopyRunning(attemptCopy);
                        if (Option.isNone(ran)) {
                            // a copy is running in this instance: check on it later
                            yield* armAlarmIn(copyCheckDelay);
                        }
                    }).pipe(Effect.catchCause((cause) => Effect.logError('alarm failed', cause))),
            };
        });
    }),
);
