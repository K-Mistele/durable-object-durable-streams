/**
 * `StreamObject`: one Durable Object per stream path, holding that stream's tables and waiters.
 * Its RPC methods are `StreamOperations` taking and returning encoded values, because Cloudflare
 * RPC rejects class instances such as `Option` and `DateTime` (engine-design.md decision 20).
 */
import * as SqliteClient from '@effect/sql-sqlite-do/SqliteClient';
import * as Cloudflare from 'alchemy/Cloudflare';
import { Effect, Layer, Schema, Stream } from 'effect';
import { StreamPath } from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    BadRequestError,
    ContentTypeMismatchError,
    InvalidEpochSeqError,
    SequenceGapError,
    StaleEpochError,
    StreamClosedError,
    StreamConfigConflictError,
    StreamGoneError,
    StreamNotFoundError,
    StreamSeqConflictError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import {
    AppendRequest,
    AppendResult,
    CreateStreamRequest,
    CreateStreamResult,
    EmptyRequest,
    ReadBodyRequest,
    ReadRange,
    StartReadRequest,
    StreamMetadata,
    WaitForNewMessagesRequest,
    WaitResult,
} from '@kylelayer/effect-durable-streams-server/stream-operation-requests';
import {
    type AppendToStreamError,
    type CreateStreamError,
    makeStreamOperations,
    type MissingStreamError,
} from '@kylelayer/effect-durable-streams-server/stream-operations';
import { layerSql } from '@kylelayer/effect-durable-streams-server/stream-storage';

/** `StreamOperations` with each request and result in its encoded form. */
export type EncodedStreamOperations = {
    readonly createStream: (
        request: typeof CreateStreamRequest.Encoded,
    ) => Effect.Effect<typeof CreateStreamResult.Encoded, CreateStreamError>;
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
    readonly waitForNewMessages: (
        request: typeof WaitForNewMessagesRequest.Encoded,
    ) => Effect.Effect<typeof WaitResult.Encoded, MissingStreamError>;
    readonly deleteStream: (
        request: typeof EmptyRequest.Encoded,
    ) => Effect.Effect<void, MissingStreamError>;
};

/** `errors` lists every failure a method can return, so the Worker side rebuilds each as its class. */
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
        ],
    },
) {}

/**
 * An operation that decodes its request and encodes its result. Both sides are our own code, so a
 * value that fails its schema is a bug: a defect, not a typed failure.
 */
const serveEncoded =
    <Request, RequestEncoded, Result, ResultEncoded, E>({
        request,
        result,
        operation,
    }: {
        readonly request: Schema.Codec<Request, RequestEncoded>;
        readonly result: Schema.Codec<Result, ResultEncoded>;
        readonly operation: (request: Request) => Effect.Effect<Result, E>;
    }) =>
    (encoded: RequestEncoded): Effect.Effect<ResultEncoded, E> =>
        Schema.decodeEffect(request)(encoded).pipe(
            Effect.orDie,
            Effect.flatMap(operation),
            Effect.flatMap((value) => Effect.orDie(Schema.encodeEffect(result)(value))),
        );

export const StreamObjectLive = StreamObject.make(
    Effect.gen(function* () {
        const state = yield* Cloudflare.DurableObjectState;
        const migrations = yield* Cloudflare.SqlMigrations('./migrations');

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

            return {
                createStream: serveEncoded({
                    request: CreateStreamRequest,
                    result: CreateStreamResult,
                    operation: operations.createStream,
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
                waitForNewMessages: serveEncoded({
                    request: WaitForNewMessagesRequest,
                    result: WaitResult,
                    operation: operations.waitForNewMessages,
                }),
                deleteStream: serveEncoded({
                    request: EmptyRequest,
                    result: Schema.Void,
                    operation: operations.deleteStream,
                }),
            } satisfies EncodedStreamOperations;
        });
    }),
);
