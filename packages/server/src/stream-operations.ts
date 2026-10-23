import {
    Data,
    DateTime,
    Effect,
    Equivalence,
    Match,
    Option,
    Predicate,
    Random,
    Ref,
    Stream,
    SubscriptionRef,
} from 'effect';
import {
    mediaTypeOf,
    type Offset,
    type ProducerSeq,
    type StreamPath,
} from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    type BadRequestError,
    type ContentTypeMismatchError,
    type InvalidForkOffsetError,
    SourceDeletedError,
    StreamClosedError,
    StreamConfigConflictError,
    type StreamGoneError,
    StreamNotFoundError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import { joinBytes } from '@kylelayer/effect-durable-streams-protocol/bytes';
import { jsonMessages } from '@kylelayer/effect-durable-streams-protocol/json';
import { advance, zero } from '@kylelayer/effect-durable-streams-protocol/offset';
import type { ProducerState } from '@kylelayer/effect-durable-streams-protocol/producer';
import {
    closeSseDataEvent,
    openSseDataEvent,
    type SseControl,
    type SseDataEvent,
    sseControlEvent,
    sseDataEncodingFor,
    startSseDataMessage,
    writeSseData,
} from '@kylelayer/effect-durable-streams-protocol/sse';
import { type ChunkKey, CopyProgress, type StoredChunk, type StoredStream } from './stored-stream';
import {
    type AddForkRequest,
    type AppendRequest,
    type AppendResult,
    type CreateForkRequest,
    type CreateStreamRequest,
    type CreateStreamResult,
    type EmptyRequest,
    type ForkPoint,
    type ReadBodyRequest,
    type ReadForkRowsRequest,
    type ReadRange,
    type ReadSseEventsRequest,
    type RemoveForkRequest,
    type SourceToRelease,
    type StartReadRequest,
    type StreamMetadata,
    type WaitForNewMessagesRequest,
    WaitResult,
} from './stream-operation-requests';
import {
    type AppendRejection,
    buildNewStream,
    expiryOf,
    forkExpiry,
    getExistingStream,
    isRepeatedClose,
    isSameForkRequest,
    rejectConfigConflict,
    rejectContentTypeMismatch,
    rejectOutOfSequenceProducerRequest,
    rejectStreamSeqRegression,
    resolveForkPoint,
    type StoredAppendState,
    StoredStreamStatus,
    statusOf,
    type StreamConfig,
    updateStreamForAppend,
} from './stream-rules';
import {
    chunkBytes,
    type PendingRelease,
    type StreamChanges,
    StreamStorage,
} from './stream-storage';

export type CreateStreamError = BadRequestError | StreamConfigConflictError;
export type AppendToStreamError = BadRequestError | AppendRejection;
export type MissingStreamError = StreamNotFoundError | StreamGoneError;
export type AddForkError =
    | StreamNotFoundError
    | SourceDeletedError
    | ContentTypeMismatchError
    | InvalidForkOffsetError;
export type CreateForkError = AddForkError | BadRequestError | StreamConfigConflictError;

/** What a fork PUT finds at its own path. */
export type ForkClaim = Data.TaggedEnum<{
    /** Nothing was there: the path is now this fork's, waiting for its source to agree. */
    Claimed: { readonly generation: number };
    /** The same fork, already made: 200. */
    Exists: { readonly result: CreateStreamResult };
    /** The same fork, still being made: wait for it. */
    InProgress: {};
}>;
export const ForkClaim = Data.taggedEnum<ForkClaim>();

/** What a fork's next copy attempt should do. */
export type CopyAttempt = Data.TaggedEnum<{
    /** Not a fork being made. */
    NothingToCopy: {};
    /** Claimed, but the source never agreed: the request that claimed it died. */
    Unconfirmed: {};
    /** The last attempts made no progress: delete the fork. */
    GiveUp: {};
    /** Copy the source's rows after `after` and before `to`. */
    Copy: {
        readonly source: StreamPath;
        readonly after: Option.Option<ChunkKey>;
        readonly to: Offset;
        readonly lastBytes: Option.Option<number>;
    };
}>;
export const CopyAttempt = Data.taggedEnum<CopyAttempt>();

/** What `expireIfDue` did. */
export type ExpiryCheck = {
    readonly released: SourceToRelease;
    /** The stream's expiry, if it has one and hasn't reached it. */
    readonly checkAgainAt: Option.Option<DateTime.Utc>;
};

/** A fork is deleted after this many copy attempts in a row that made no progress. */
const maxFailedCopyAttempts = 3;

/**
 * The operations on one stream, bound to its path and its waiters. Every request for the stream
 * goes through them, inside the stream's Durable Object, so its writes happen in one place and its
 * long-poll waiters live next to them. A plain object, not a service: the Durable Object builds
 * one per instance with `makeStreamOperations`, and alchemy exposes each function as one of its
 * RPC methods.
 */
export type StreamOperations = {
    readonly createStream: (
        request: CreateStreamRequest,
    ) => Effect.Effect<CreateStreamResult, CreateStreamError>;
    readonly appendToStream: (
        request: AppendRequest,
    ) => Effect.Effect<AppendResult, AppendToStreamError>;
    readonly getMetadata: (
        request: EmptyRequest,
    ) => Effect.Effect<StreamMetadata, MissingStreamError>;
    readonly startRead: (request: StartReadRequest) => Effect.Effect<ReadRange, MissingStreamError>;
    /** The response body: JSON as one array, other content types as stored bytes. */
    readonly readBody: (request: ReadBodyRequest) => Stream.Stream<Uint8Array, StreamNotFoundError>;
    /**
     * SSE: the range as data events of about `sseEventBytes` each, every one ending at a message
     * boundary and followed by its control event (engine-design.md, SSE).
     */
    readonly readSseEvents: (
        request: ReadSseEventsRequest,
    ) => Stream.Stream<Uint8Array, StreamNotFoundError>;
    readonly waitForNewMessages: (
        request: WaitForNewMessagesRequest,
    ) => Effect.Effect<WaitResult, MissingStreamError>;
    /** Purges the stream, or soft-deletes it while forks copy from it. Returns the source to release, if purged. */
    readonly deleteStream: (
        request: EmptyRequest,
    ) => Effect.Effect<SourceToRelease, MissingStreamError>;

    // As a fork's source (engine-design.md, Forks)

    /** Checks a fork request against this stream and records the fork: the source half of creating it. */
    readonly addFork: (request: AddForkRequest) => Effect.Effect<ForkPoint, AddForkError>;
    /** The rows a fork copies. Reads a soft-deleted or expired stream too: its forks still need it. */
    readonly readForkRows: (
        request: ReadForkRowsRequest,
    ) => Stream.Stream<StoredChunk, StreamNotFoundError>;
    /** A fork was deleted. Purges this stream if it was waiting on its last fork; returns its own source to release then. */
    readonly removeFork: (request: RemoveForkRequest) => Effect.Effect<SourceToRelease>;

    // As a fork being made; the host calls these in order

    readonly claimFork: (
        request: CreateForkRequest,
    ) => Effect.Effect<ForkClaim, StreamConfigConflictError>;
    /** Saves what the source agreed to, and the PUT body's messages after the copied ones. */
    readonly saveForkPoint: (input: {
        readonly generation: number;
        readonly request: CreateForkRequest;
        readonly point: ForkPoint;
    }) => Effect.Effect<void, BadRequestError>;
    /** Counts an attempt, and says where it starts; a fork that keeps failing is given up. */
    readonly startCopyAttempt: () => Effect.Effect<CopyAttempt>;
    /** Writes each row as it arrives; a row already there is left as it is. */
    readonly writeForkRows: <E>(rows: Stream.Stream<StoredChunk, E>) => Effect.Effect<void, E>;
    readonly finishFork: () => Effect.Effect<CreateStreamResult>;
    /** Deletes a fork that was never finished; returns its source to release. */
    readonly abandonFork: () => Effect.Effect<SourceToRelease>;
    /** A repeat of a fork PUT while the fork is still being made: waits until it is (200) or is given up (404). */
    readonly waitForFork: (
        request: CreateForkRequest,
    ) => Effect.Effect<CreateStreamResult, StreamNotFoundError | StreamConfigConflictError>;

    /**
     * Purges the stream if it has expired and no fork copies from it (one that does stays, as
     * gone, until its last fork is deleted). Returns its source to release, and when to check
     * again: a TTL slides with each access, so the expiry may have moved.
     */
    readonly expireIfDue: () => Effect.Effect<ExpiryCheck>;

    /** `removeFork` calls to a source that failed, kept so the host can retry them. */
    readonly savePendingRelease: (release: PendingRelease) => Effect.Effect<void>;
    readonly loadPendingReleases: () => Effect.Effect<ReadonlyArray<PendingRelease>>;
    readonly deletePendingRelease: (source: StreamPath) => Effect.Effect<void>;
};

const chunkKeyEquivalence = Equivalence.make<ChunkKey>(
    (self, that) => self.offset === that.offset && self.part === that.part,
);

/** What waiters watch. `generation` is None while no stream exists. */
type WaiterState = {
    readonly generation: Option.Option<number>;
    readonly tail: Offset;
    readonly closed: boolean;
};

type AppendChanges = {
    readonly changes: StreamChanges;
    readonly result: { readonly stream: StoredStream; readonly appendResult: AppendResult };
};

const waiterStateOf = (stream: Option.Option<StoredStream>): WaiterState =>
    Option.match(stream, {
        onNone: () => ({ generation: Option.none(), tail: zero, closed: false }),
        onSome: (existing) => ({
            generation: Option.some(existing.generation),
            tail: existing.tail,
            closed: existing.closed,
        }),
    });

const toStreamMetadata = (stream: StoredStream): StreamMetadata => ({
    contentType: stream.contentType,
    tail: stream.tail,
    closed: stream.closed,
    ttlSeconds: stream.ttlSeconds,
    expiresAt: stream.expiresAt,
});

const isJsonContentType = (contentType: string) => mediaTypeOf(contentType) === 'application/json';

/** A JSON body holds one message per array item; any other body is one message. */
const splitBodyIntoMessages = ({
    contentType,
    body,
    allowEmptyArray,
}: {
    readonly contentType: string;
    readonly body: Uint8Array;
    readonly allowEmptyArray: boolean;
}): Effect.Effect<ReadonlyArray<Uint8Array>, BadRequestError> => {
    if (body.length === 0) {
        return Effect.succeed([]);
    }
    if (!isJsonContentType(contentType)) {
        return Effect.succeed([body]);
    }
    return Stream.runCollect(jsonMessages(Stream.make(body), { allowEmpty: allowEmptyArray }));
};

/** A repeat of a request already applied: save only the access time, answer with the current tail. */
const acknowledgeDuplicateAppend = ({
    stream,
    acceptedProducerSeq,
    now,
}: {
    readonly stream: StoredStream;
    readonly acceptedProducerSeq: Option.Option<ProducerSeq>;
    readonly now: DateTime.Utc;
}): AppendChanges => ({
    changes: {
        firstMessageOffset: stream.tail,
        storedStream: { ...stream, lastAccessedAt: now },
        producer: Option.none(),
        messages: [],
    },
    result: {
        stream,
        appendResult: {
            nextOffset: stream.tail,
            closed: stream.closed,
            wasDuplicate: true,
            acceptedProducerSeq,
        },
    },
});

/**
 * PUT, inside the create transaction. An existing stream with the same config is left as it is
 * (200); with another config the PUT fails with 409. Otherwise the body becomes the new stream's
 * first messages (201).
 */
export const computeCreateStreamChanges = ({
    stored,
    request,
    now,
}: {
    readonly stored: Option.Option<StoredStream>;
    readonly request: CreateStreamRequest;
    readonly now: DateTime.Utc;
}) =>
    Effect.gen(function* () {
        const config: StreamConfig = {
            contentType: request.contentType,
            ttlSeconds: request.ttlSeconds,
            expiresAt: request.expiresAt,
            closed: request.closed,
        };
        const status = statusOf({ stored, now });
        if (StoredStreamStatus.$is('Live')(status)) {
            yield* rejectConfigConflict({ existing: status.stream, requested: config });
            return {
                changes: Option.none<StreamChanges>(),
                result: { isNewStream: false, stream: status.stream },
            };
        }
        // PROTOCOL.md 4.2: a soft-deleted path can't be re-created; a fork being made isn't this
        if (!StoredStreamStatus.$is('Missing')(status)) {
            return yield* StreamConfigConflictError.make({});
        }
        const messages = yield* splitBodyIntoMessages({
            contentType: config.contentType,
            body: request.body,
            allowEmptyArray: true,
        });
        const generation = yield* Random.nextIntBetween(1, 2 ** 31);
        const created = buildNewStream({ config, generation, messageCount: messages.length, now });
        return {
            changes: Option.some<StreamChanges>({
                firstMessageOffset: zero,
                storedStream: created,
                producer: Option.none(),
                messages,
            }),
            result: { isNewStream: true, stream: created },
        };
    });

/** POST, inside the append transaction: the protocol's checks in PROTOCOL.md 5.2 order, then the messages to save. */
export const computeAppendChanges = ({
    stored,
    request,
    now,
}: {
    readonly stored: StoredAppendState;
    readonly request: AppendRequest;
    readonly now: DateTime.Utc;
}): Effect.Effect<AppendChanges, AppendToStreamError> =>
    Effect.gen(function* () {
        const stream = yield* getExistingStream({ stored: stored.stream, now });

        if (stream.closed) {
            if (isRepeatedClose({ stream, request })) {
                const acceptedProducerSeq = Option.map(
                    request.producer,
                    (producer) => producer.seq,
                );
                return acknowledgeDuplicateAppend({ stream, acceptedProducerSeq, now });
            }
            return yield* StreamClosedError.make({ nextOffset: stream.tail });
        }

        yield* Match.valueTags(request, {
            AppendMessages: ({ contentType }) => rejectContentTypeMismatch({ stream, contentType }),
            CloseStream: () => Effect.void,
        });

        const appendMessages = (producerState: Option.Option<ProducerState>) =>
            Effect.gen(function* () {
                const messages = yield* Match.valueTags(request, {
                    AppendMessages: (append) =>
                        rejectStreamSeqRegression({ stream, streamSeq: append.streamSeq }).pipe(
                            Effect.andThen(
                                splitBodyIntoMessages({
                                    contentType: stream.contentType,
                                    body: append.body,
                                    allowEmptyArray: false,
                                }),
                            ),
                        ),
                    CloseStream: () => Effect.succeed<ReadonlyArray<Uint8Array>>([]),
                });
                const updated = updateStreamForAppend({
                    stream,
                    request,
                    messageCount: messages.length,
                    now,
                });
                const changes: StreamChanges = {
                    firstMessageOffset: stream.tail,
                    storedStream: updated,
                    producer: Option.zipWith(
                        request.producer,
                        producerState,
                        (producer, state) => ({
                            id: producer.id,
                            state,
                        }),
                    ),
                    messages,
                };
                const appendResult: AppendResult = {
                    nextOffset: updated.tail,
                    closed: updated.closed,
                    wasDuplicate: false,
                    acceptedProducerSeq: Option.map(request.producer, (producer) => producer.seq),
                };
                return { changes, result: { stream: updated, appendResult } };
            });

        const position = yield* rejectOutOfSequenceProducerRequest({
            storedProducer: stored.producer,
            producer: request.producer,
        });
        return yield* Match.valueTags(position, {
            AlreadyAccepted: ({ lastSeq }) =>
                Effect.succeed(
                    acknowledgeDuplicateAppend({
                        stream,
                        acceptedProducerSeq: Option.some(lastSeq),
                        now,
                    }),
                ),
            NoProducer: () => appendMessages(Option.none()),
            NextInSequence: ({ state }) => appendMessages(Option.some(state)),
        });
    });

const textEncoder = new TextEncoder();
const openBracket = textEncoder.encode('[');
const comma = textEncoder.encode(',');
const closeBracket = textEncoder.encode(']');

/** A data event ends at the first message boundary once it holds this much. */
const sseEventBytes = 4 * 1024 * 1024;

/** `readSseEvents` between rows: the data event being written, if any, and its size so far. */
type SseEventsState = {
    readonly event: Option.Option<SseDataEvent>;
    readonly eventBytes: number;
};

/**
 * What one row adds to the SSE text: bytes, and control events still to be encoded. One row's
 * output goes out as one chunk, so a data event's end and its control event stay together.
 */
type SseOutput = ReadonlyArray<Uint8Array | SseControl>;

/** Builds the operations of the stream at `stream`, loading its state for its waiters. */
export const makeStreamOperations = Effect.fn('makeStreamOperations')(function* (input: {
    readonly stream: StreamPath;
}) {
    const { stream } = input;
    const storage = yield* StreamStorage;
    const openedAt = yield* DateTime.now;
    const storedAtOpen = yield* storage.loadStream({ stream });
    const waiterState = yield* SubscriptionRef.make(
        waiterStateOf(
            Option.filter(storedAtOpen, () =>
                StoredStreamStatus.$is('Live')(statusOf({ stored: storedAtOpen, now: openedAt })),
            ),
        ),
    );
    // bumped when a fork being made is finished or given up, for PUTs waiting on it
    const forkChanges = yield* SubscriptionRef.make(0);

    // PROTOCOL.md 5.1: live readers (long-poll and SSE) keep a stream from expiring. While any
    // is waiting here, a load sees the stream as read now; each wait records an access as it ends.
    const liveReaders = yield* Ref.make(0);
    const withLiveReads = (stored: Option.Option<StoredStream>) =>
        Effect.gen(function* () {
            if ((yield* Ref.get(liveReaders)) === 0) {
                return stored;
            }
            const now = yield* DateTime.now;
            return Option.map(stored, (existing) => ({
                ...existing,
                lastAccessedAt: DateTime.max(existing.lastAccessedAt, now),
            }));
        });
    const loadStream = storage.loadStream({ stream }).pipe(Effect.flatMap(withLiveReads));

    const loadExistingStream = Effect.gen(function* () {
        const stored = yield* loadStream;
        return yield* getExistingStream({ stored, now: yield* DateTime.now });
    });

    // a read fails before sending anything if the stream was deleted or re-created since startRead
    const loadSameGeneration = (generation: number) =>
        Effect.gen(function* () {
            const stored = yield* loadStream;
            const sameGeneration = Option.filter(
                stored,
                (candidate) => candidate.generation === generation,
            );
            if (Option.isNone(sameGeneration)) {
                return yield* StreamNotFoundError.make({});
            }
            return sameGeneration.value;
        });

    // a fork that `claimFork` claimed with `generation`, and the source hasn't agreed to yet
    const loadForkBeingMade = (generation: number) =>
        Effect.gen(function* () {
            const stored = yield* loadStream;
            const claimed = Option.filter(
                stored,
                (existing) =>
                    existing.generation === generation &&
                    Option.exists(existing.copy, Predicate.isTagged('Claimed')),
            );
            if (Option.isNone(claimed)) {
                return yield* Effect.die(new Error('saveForkPoint: the claim is gone'));
            }
            return claimed.value;
        });

    const operations: StreamOperations = {
        createStream: (request) =>
            Effect.gen(function* () {
                const now = yield* DateTime.now;
                const outcome = yield* storage.createStream({
                    stream,
                    computeChanges: (stored) =>
                        withLiveReads(stored).pipe(
                            Effect.flatMap((live) =>
                                computeCreateStreamChanges({ stored: live, request, now }),
                            ),
                        ),
                });
                if (outcome.isNewStream) {
                    yield* SubscriptionRef.set(
                        waiterState,
                        waiterStateOf(Option.some(outcome.stream)),
                    );
                }
                return {
                    isNewStream: outcome.isNewStream,
                    metadata: toStreamMetadata(outcome.stream),
                };
            }),

        appendToStream: (request) =>
            Effect.gen(function* () {
                const now = yield* DateTime.now;
                const outcome = yield* storage.appendToStream({
                    stream,
                    producerId: Option.map(request.producer, (producer) => producer.id),
                    computeChanges: (stored) =>
                        withLiveReads(stored.stream).pipe(
                            Effect.flatMap((live) =>
                                computeAppendChanges({
                                    stored: { ...stored, stream: live },
                                    request,
                                    now,
                                }),
                            ),
                        ),
                });
                yield* SubscriptionRef.set(waiterState, waiterStateOf(Option.some(outcome.stream)));
                return outcome.appendResult;
            }),

        getMetadata: () => loadExistingStream.pipe(Effect.map(toStreamMetadata)),

        startRead: ({ from }) =>
            Effect.gen(function* () {
                const existing = yield* loadExistingStream;
                const start = Match.value(from).pipe(
                    Match.when('-1', () => zero),
                    Match.when('now', () => existing.tail),
                    Match.orElse((offset) => offset),
                );
                yield* storage.recordAccess({ stream, now: yield* DateTime.now });
                return {
                    metadata: toStreamMetadata(existing),
                    start,
                    end: existing.tail,
                    generation: existing.generation,
                };
            }),

        readBody: ({ from, to, generation }) =>
            Stream.unwrap(
                Effect.gen(function* () {
                    const existing = yield* loadSameGeneration(generation);
                    const chunks = storage.readChunks({
                        stream,
                        from,
                        to,
                        after: Option.none(),
                        generation,
                    });
                    if (!isJsonContentType(existing.contentType)) {
                        return Stream.map(chunks, (chunk) => chunk.bytes);
                    }
                    // a comma before each message after the first; parts after 0 continue a message
                    const items = Stream.flatMap(chunks, (chunk) => {
                        const startsLaterMessage = chunk.part === 0 && chunk.offset !== from;
                        return startsLaterMessage
                            ? Stream.make(comma, chunk.bytes)
                            : Stream.make(chunk.bytes);
                    });
                    return Stream.make(openBracket).pipe(
                        Stream.concat(items),
                        Stream.concat(Stream.make(closeBracket)),
                    );
                }),
            ),

        readSseEvents: ({ from, to, generation, closed, cursor }) =>
            Stream.unwrap(
                Effect.gen(function* () {
                    const existing = yield* loadSameGeneration(generation);
                    const encoding = sseDataEncodingFor(existing.contentType);
                    // a control event mid-read points at the next message; the last one at `to`
                    const controlBefore = (offset: Offset): SseControl => ({
                        streamNextOffset: offset,
                        streamCursor: cursor,
                    });
                    const lastControl: SseControl = closed
                        ? { streamNextOffset: to, streamClosed: true }
                        : { streamNextOffset: to, streamCursor: cursor, upToDate: true };

                    const writeChunk = (
                        state: SseEventsState,
                        chunk: StoredChunk,
                    ): readonly [SseEventsState, ReadonlyArray<SseOutput>] => {
                        const output: Array<Uint8Array | SseControl> = [];
                        const startsMessage = chunk.part === 0;
                        // a full event ends here, between two messages
                        const endsEvent = startsMessage && state.eventBytes >= sseEventBytes;
                        const continuing = endsEvent ? Option.none() : state.event;
                        if (Option.isSome(state.event) && Option.isNone(continuing)) {
                            output.push(
                                closeSseDataEvent(state.event.value),
                                controlBefore(chunk.offset),
                            );
                        }
                        const [opened, openText] = Option.match(continuing, {
                            onNone: () => openSseDataEvent(encoding),
                            onSome: (event) => [event, new Uint8Array(0)] as const,
                        });
                        const [started, separator] = startsMessage
                            ? startSseDataMessage(opened)
                            : ([opened, new Uint8Array(0)] as const);
                        const [written, data] = writeSseData(started, chunk.bytes);
                        output.push(openText, separator, data);
                        const eventBytes = Option.isSome(continuing) ? state.eventBytes : 0;
                        return [
                            {
                                event: Option.some(written),
                                eventBytes: eventBytes + chunk.bytes.byteLength,
                            },
                            [output],
                        ];
                    };

                    const finish = (state: SseEventsState): ReadonlyArray<SseOutput> => [
                        Option.match(state.event, {
                            onNone: () => [lastControl],
                            onSome: (event) => [closeSseDataEvent(event), lastControl],
                        }),
                    ];

                    return storage
                        .readChunks({
                            stream,
                            from,
                            to,
                            after: Option.none(),
                            generation,
                        })
                        .pipe(
                            Stream.mapAccum(
                                (): SseEventsState => ({ event: Option.none(), eventBytes: 0 }),
                                writeChunk,
                                { onHalt: finish },
                            ),
                            Stream.mapEffect((output) =>
                                Effect.forEach(output, (piece) =>
                                    piece instanceof Uint8Array
                                        ? Effect.succeed(piece)
                                        : sseControlEvent(piece).pipe(Effect.orDie),
                                ).pipe(Effect.map(joinBytes)),
                            ),
                        );
                }),
            ),

        // checked before it counts as a live reader, which would make the stream look read now;
        // counted while it waits, and its end counts as a read
        waitForNewMessages: ({ after, timeout }) =>
            Effect.acquireUseRelease(
                loadExistingStream.pipe(
                    Effect.tap(() => Ref.update(liveReaders, (count) => count + 1)),
                ),
                (existing) => {
                    const isSameStream = (state: WaiterState) =>
                        Option.contains(state.generation, existing.generation);
                    const hasChanged = (state: WaiterState) =>
                        !isSameStream(state) || state.tail > after || state.closed;
                    return Effect.gen(function* () {
                        const changed = yield* SubscriptionRef.changes(waiterState).pipe(
                            Stream.filter(hasChanged),
                            Stream.runHead,
                            Effect.timeoutOption(timeout),
                            Effect.map(Option.flatten),
                        );
                        if (Option.isNone(changed)) {
                            return WaitResult.cases.TimedOut.make({});
                        }
                        if (!isSameStream(changed.value)) {
                            return yield* StreamNotFoundError.make({});
                        }
                        if (changed.value.tail > after) {
                            return WaitResult.cases.NewMessages.make({});
                        }
                        return WaitResult.cases.StreamClosed.make({});
                    });
                },
                () =>
                    DateTime.now.pipe(
                        Effect.flatMap((now) => storage.recordAccess({ stream, now })),
                        Effect.andThen(Ref.update(liveReaders, (count) => count - 1)),
                    ),
            ),

        deleteStream: () =>
            Effect.gen(function* () {
                const released = yield* storage.withTransaction(
                    Effect.gen(function* () {
                        const existing = yield* loadExistingStream;
                        if (existing.hasForks) {
                            yield* storage.updateStream({
                                stream,
                                storedStream: { ...existing, softDeleted: true },
                            });
                            return Option.none<StreamPath>();
                        }
                        yield* storage.deleteStream({ stream });
                        return Option.map(existing.fork, (fork) => fork.source);
                    }),
                );
                yield* SubscriptionRef.set(waiterState, waiterStateOf(Option.none()));
                return released;
            }),

        addFork: (request) =>
            storage.withTransaction(
                Effect.gen(function* () {
                    const stored = yield* loadStream;
                    const source = yield* StoredStreamStatus.$match(
                        statusOf({ stored, now: yield* DateTime.now }),
                        {
                            Missing: () => Effect.fail(StreamNotFoundError.make({})),
                            Copying: () => Effect.fail(StreamNotFoundError.make({})),
                            Gone: () => Effect.fail(SourceDeletedError.make({})),
                            Live: ({ stream: live }) => Effect.succeed(live),
                        },
                    );
                    const offset = Option.getOrElse(request.offset, () => source.tail);
                    // a binary sub-offset counts bytes into the message at `offset`
                    const messageBytes =
                        request.subOffset > 0 && offset < source.tail
                            ? yield* storage.messageBytes({ stream, offset })
                            : Option.none<number>();
                    const point = yield* resolveForkPoint({ source, request, messageBytes });
                    yield* storage.addFork({ stream, fork: request.fork });
                    return point;
                }),
            ),

        readForkRows: ({ after, to, lastBytes }) =>
            Stream.unwrap(
                Effect.gen(function* () {
                    const stored = yield* loadStream;
                    if (Option.isNone(stored)) {
                        return yield* StreamNotFoundError.make({});
                    }
                    const rows = storage.readChunks({
                        stream,
                        from: zero,
                        to,
                        after,
                        generation: stored.value.generation,
                    });
                    return Option.match(lastBytes, {
                        onNone: () => rows,
                        // a binary sub-offset keeps the first `keep` bytes of the last message.
                        // Every part but a message's last is chunkBytes long, so part p starts
                        // at p * chunkBytes, even when a resumed copy starts mid-message.
                        onSome: (keep) => {
                            const lastMessage = advance(to, -1);
                            return Stream.flatMap(rows, (chunk) => {
                                const kept = keep - chunk.part * chunkBytes;
                                if (chunk.offset !== lastMessage) {
                                    return Stream.make(chunk);
                                }
                                return kept <= 0
                                    ? Stream.empty
                                    : Stream.make({
                                          ...chunk,
                                          bytes: chunk.bytes.subarray(0, kept),
                                      });
                            });
                        },
                    });
                }),
            ),

        removeFork: ({ fork }) =>
            Effect.gen(function* () {
                const released = yield* storage.withTransaction(
                    Effect.gen(function* () {
                        yield* storage.removeFork({ stream, fork });
                        const stored = yield* loadStream;
                        const status = statusOf({ stored, now: yield* DateTime.now });
                        // waiting only on its forks: soft-deleted, or expired, and now none left
                        if (!StoredStreamStatus.$is('Gone')(status) || status.stream.hasForks) {
                            return Option.none<StreamPath>();
                        }
                        yield* storage.deleteStream({ stream });
                        return Option.map(status.stream.fork, (own) => own.source);
                    }),
                );
                return released;
            }),

        claimFork: (request) =>
            storage.withTransaction(
                Effect.gen(function* () {
                    const now = yield* DateTime.now;
                    const stored = yield* loadStream;
                    return yield* StoredStreamStatus.$match(statusOf({ stored, now }), {
                        Missing: () =>
                            Effect.gen(function* () {
                                const generation = yield* Random.nextIntBetween(1, 2 ** 31);
                                const claimed: StoredStream = {
                                    ...buildNewStream({
                                        config: {
                                            // replaced by the source's once it agrees
                                            contentType: Option.getOrElse(
                                                request.contentType,
                                                () => 'application/octet-stream',
                                            ),
                                            ttlSeconds: request.ttlSeconds,
                                            expiresAt: request.expiresAt,
                                            closed: request.closed,
                                        },
                                        generation,
                                        messageCount: 0,
                                        now,
                                    }),
                                    fork: Option.some({
                                        source: request.source,
                                        offset: request.offset,
                                        subOffset: request.subOffset,
                                    }),
                                    copy: Option.some(CopyProgress.cases.Claimed.make({})),
                                };
                                yield* storage.insertStream({ stream, storedStream: claimed });
                                return ForkClaim.Claimed({ generation });
                            }),
                        Copying: ({ stream: copying }) =>
                            isSameForkRequest({ stream: copying, request })
                                ? Effect.succeed(ForkClaim.InProgress())
                                : Effect.fail(StreamConfigConflictError.make({})),
                        Gone: () => Effect.fail(StreamConfigConflictError.make({})),
                        Live: ({ stream: existing }) =>
                            isSameForkRequest({ stream: existing, request })
                                ? Effect.succeed(
                                      ForkClaim.Exists({
                                          result: {
                                              isNewStream: false,
                                              metadata: toStreamMetadata(existing),
                                          },
                                      }),
                                  )
                                : Effect.fail(StreamConfigConflictError.make({})),
                    });
                }),
            ),

        saveForkPoint: ({ generation, request, point }) =>
            storage.withTransaction(
                Effect.gen(function* () {
                    const claimed = yield* loadForkBeingMade(generation);
                    const messages = yield* splitBodyIntoMessages({
                        contentType: point.contentType,
                        body: request.body,
                        allowEmptyArray: true,
                    });
                    const now = yield* DateTime.now;
                    yield* storage.updateStream({
                        stream,
                        storedStream: {
                            ...claimed,
                            ...forkExpiry({ point, request }),
                            contentType: point.contentType,
                            tail: advance(point.to, messages.length),
                            createdAt: now,
                            lastAccessedAt: now,
                            copy: Option.some(
                                CopyProgress.cases.Copying.make({
                                    to: point.to,
                                    lastBytes: point.lastBytes,
                                    startedAttempts: 0,
                                    lastStart: Option.none(),
                                    failures: 0,
                                }),
                            ),
                        },
                    });
                    yield* storage.insertMessages({
                        stream,
                        firstMessageOffset: point.to,
                        messages,
                    });
                }),
            ),

        startCopyAttempt: () =>
            storage.withTransaction(
                Effect.gen(function* () {
                    const stored = yield* loadStream;
                    const fork = Option.flatMap(stored, (existing) => existing.fork);
                    const copy = Option.flatMap(stored, (existing) => existing.copy);
                    if (Option.isNone(stored) || Option.isNone(fork) || Option.isNone(copy)) {
                        return CopyAttempt.NothingToCopy();
                    }
                    if (!Predicate.isTagged(copy.value, 'Copying')) {
                        return CopyAttempt.Unconfirmed();
                    }
                    const progress = copy.value;
                    const after = yield* storage.lastChunkKeyBefore({
                        stream,
                        before: progress.to,
                    });
                    // counted before the attempt runs: an out-of-memory kill never reports back
                    const madeNoProgress =
                        progress.startedAttempts > 0 &&
                        Option.makeEquivalence(chunkKeyEquivalence)(after, progress.lastStart);
                    const failures = progress.failures + (madeNoProgress ? 1 : 0);
                    if (failures >= maxFailedCopyAttempts) {
                        return CopyAttempt.GiveUp();
                    }
                    yield* storage.updateStream({
                        stream,
                        storedStream: {
                            ...stored.value,
                            copy: Option.some({
                                ...progress,
                                startedAttempts: progress.startedAttempts + 1,
                                lastStart: after,
                                failures,
                            }),
                        },
                    });
                    return CopyAttempt.Copy({
                        source: fork.value.source,
                        after,
                        to: progress.to,
                        lastBytes: progress.lastBytes,
                    });
                }),
            ),

        writeForkRows: (rows) =>
            Stream.runForEach(rows, (chunk) =>
                storage.insertCopiedChunks({ stream, chunks: [chunk] }),
            ),

        finishFork: () =>
            Effect.gen(function* () {
                const finished = yield* storage.withTransaction(
                    Effect.gen(function* () {
                        const stored = yield* loadStream;
                        if (Option.isNone(stored) || Option.isNone(stored.value.copy)) {
                            return yield* Effect.die(new Error('finishFork: no fork being made'));
                        }
                        const ready: StoredStream = {
                            ...stored.value,
                            copy: Option.none(),
                            lastAccessedAt: yield* DateTime.now,
                        };
                        yield* storage.updateStream({ stream, storedStream: ready });
                        return ready;
                    }),
                );
                yield* SubscriptionRef.set(waiterState, waiterStateOf(Option.some(finished)));
                yield* SubscriptionRef.update(forkChanges, (count) => count + 1);
                return { isNewStream: true, metadata: toStreamMetadata(finished) };
            }),

        abandonFork: () =>
            Effect.gen(function* () {
                const released = yield* storage.withTransaction(
                    Effect.gen(function* () {
                        const stored = yield* loadStream;
                        const abandoned = Option.filter(stored, (existing) =>
                            Option.isSome(existing.copy),
                        );
                        if (Option.isNone(abandoned)) {
                            return Option.none<StreamPath>();
                        }
                        yield* storage.deleteStream({ stream });
                        return Option.map(abandoned.value.fork, (fork) => fork.source);
                    }),
                );
                yield* SubscriptionRef.update(forkChanges, (count) => count + 1);
                return released;
            }),

        waitForFork: (request) =>
            SubscriptionRef.changes(forkChanges).pipe(
                Stream.mapEffect(() =>
                    Effect.gen(function* () {
                        const stored = yield* loadStream;
                        return yield* StoredStreamStatus.$match(
                            statusOf({ stored, now: yield* DateTime.now }),
                            {
                                Missing: () => Effect.fail(StreamNotFoundError.make({})),
                                Copying: () => Effect.succeedNone,
                                Gone: () => Effect.fail(StreamConfigConflictError.make({})),
                                Live: ({ stream: existing }) =>
                                    isSameForkRequest({ stream: existing, request })
                                        ? Effect.succeedSome<CreateStreamResult>({
                                              isNewStream: false,
                                              metadata: toStreamMetadata(existing),
                                          })
                                        : Effect.fail(StreamConfigConflictError.make({})),
                            },
                        );
                    }),
                ),
                Stream.filter(Option.isSome),
                Stream.runHead,
                Effect.flatMap(
                    Option.match({
                        onNone: () => Effect.fail(StreamNotFoundError.make({})),
                        onSome: (done) => Effect.succeed(done.value),
                    }),
                ),
            ),

        expireIfDue: () =>
            Effect.gen(function* () {
                const nothing = {
                    purged: false,
                    released: Option.none<StreamPath>(),
                    checkAgainAt: Option.none<DateTime.Utc>(),
                };
                const { purged, ...check } = yield* storage.withTransaction(
                    Effect.gen(function* () {
                        const stored = yield* loadStream;
                        // a fork still being made gets its expiry once it is finished
                        if (Option.isNone(stored) || Option.isSome(stored.value.copy)) {
                            return nothing;
                        }
                        const existing = stored.value;
                        const expiry = expiryOf(existing);
                        if (Option.isNone(expiry)) {
                            return nothing;
                        }
                        if (DateTime.isLessThan(yield* DateTime.now, expiry.value)) {
                            return { ...nothing, checkAgainAt: expiry };
                        }
                        // its forks still need it: removeFork purges it after the last
                        if (existing.hasForks) {
                            return nothing;
                        }
                        yield* storage.deleteStream({ stream });
                        return {
                            ...nothing,
                            purged: true,
                            released: Option.map(existing.fork, (fork) => fork.source),
                        };
                    }),
                );
                if (purged) {
                    yield* SubscriptionRef.set(waiterState, waiterStateOf(Option.none()));
                }
                return check;
            }),

        savePendingRelease: (release) => storage.savePendingRelease({ stream, release }),
        loadPendingReleases: () => storage.loadPendingReleases({ stream }),
        deletePendingRelease: (source) => storage.deletePendingRelease({ stream, source }),
    };
    return operations;
});
