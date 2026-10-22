import { Array as Arr, Context, DateTime, Effect, Layer, Option, Schema, Stream } from 'effect';
import { SqlClient, SqlSchema } from 'effect/sql';
import {
    Offset,
    ProducerEpoch,
    ProducerId,
    ProducerSeq,
    StreamPath,
} from '@kylelayer/effect-durable-streams-protocol/codec';
import { StreamNotFoundError } from '@kylelayer/effect-durable-streams-protocol/errors';
import type { ProducerState } from '@kylelayer/effect-durable-streams-protocol/producer';
import { StoredChunk, StoredStream } from './stored-stream';
import type { StoredAppendState } from './stream-rules';

/** What one transaction saves: the stream's row, the producer's state, and one message per entry of `messages` from `firstMessageOffset`. */
export type StreamChanges = {
    readonly firstMessageOffset: Offset;
    readonly storedStream: StoredStream;
    readonly producer: Option.Option<{ readonly id: ProducerId; readonly state: ProducerState }>;
    readonly messages: ReadonlyArray<Uint8Array>;
};

/**
 * The stream tables. `createStream` and `appendToStream` call `computeChanges` inside their
 * transaction with what is stored, then save the changes it returns. If `computeChanges` fails,
 * the transaction rolls back and nothing is written.
 */
export class StreamStorage extends Context.Service<
    StreamStorage,
    {
        readonly loadStream: (input: {
            readonly stream: StreamPath;
        }) => Effect.Effect<Option.Option<StoredStream>>;
        /** On `Some(changes)`, deletes any old rows of the path (an expired stream), then inserts the stream. */
        readonly createStream: <A, E>(input: {
            readonly stream: StreamPath;
            readonly computeChanges: (
                stored: Option.Option<StoredStream>,
            ) => Effect.Effect<
                { readonly changes: Option.Option<StreamChanges>; readonly result: A },
                E
            >;
        }) => Effect.Effect<A, E>;
        readonly appendToStream: <A, E>(input: {
            readonly stream: StreamPath;
            readonly producerId: Option.Option<ProducerId>;
            readonly computeChanges: (
                stored: StoredAppendState,
            ) => Effect.Effect<{ readonly changes: StreamChanges; readonly result: A }, E>;
        }) => Effect.Effect<A, E>;
        /** Chunks with `from <= offset < to`, in order, a page at a time. Fails once the stream's generation is no longer `generation` (deleted or re-created). */
        readonly readChunks: (input: {
            readonly stream: StreamPath;
            readonly from: Offset;
            readonly to: Offset;
            readonly generation: number;
        }) => Stream.Stream<StoredChunk, StreamNotFoundError>;
        readonly recordAccess: (input: {
            readonly stream: StreamPath;
            readonly now: DateTime.Utc;
        }) => Effect.Effect<void>;
        readonly deleteStream: (input: { readonly stream: StreamPath }) => Effect.Effect<void>;
    }
>()('@kylelayer/effect-durable-streams-server/stream-storage/StreamStorage') {}

/** DO SQLite caps a BLOB at 2MB. */
const chunkBytes = 1024 * 1024;

/** One query per page: sqlite-do holds its connection lock until a query's stream ends, so one long read would block appends. */
const pageRows = 256;

/** DO SQLite allows 100 bound parameters per query, and a chunk row binds 4. */
const chunkRowsPerInsert = 25;

const StreamRow = StoredStream.pipe(
    Schema.encodeKeys({
        contentType: 'content_type',
        closedBy: 'closed_by',
        ttlSeconds: 'ttl_seconds',
        expiresAt: 'expires_at',
        createdAt: 'created_at',
        lastAccessedAt: 'last_accessed_at',
        lastStreamSeq: 'last_stream_seq',
    }),
);

const ChunkRow = StoredChunk.pipe(Schema.encodeKeys({ offset: 'msg_offset', bytes: 'data' }));

const ProducerRow = Schema.Struct({
    epoch: Schema.toType(ProducerEpoch),
    lastSeq: Schema.toType(ProducerSeq),
}).pipe(Schema.encodeKeys({ lastSeq: 'last_seq' }));

const StoredOffset = Schema.toType(Offset);

export const layerSql = Layer.effect(
    StreamStorage,
    Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        // FOR UPDATE holds the row for the transaction on Postgres; SQLite runs one transaction at a time
        const selectStream = SqlSchema.findOneOption({
            Request: StreamPath,
            Result: StreamRow,
            execute: (stream) =>
                sql`SELECT generation, content_type, tail, closed, closed_by, ttl_seconds, expires_at,
                        created_at, last_accessed_at, last_stream_seq
                    FROM streams WHERE stream = ${stream}
                    ${sql.onDialectOrElse({ pg: () => sql.literal('FOR UPDATE'), orElse: () => sql.literal('') })}`,
        });

        const selectProducer = SqlSchema.findOneOption({
            Request: Schema.Struct({ stream: StreamPath, id: ProducerId }),
            Result: ProducerRow,
            execute: ({ stream, id }) =>
                sql`SELECT epoch, last_seq FROM producers WHERE stream = ${stream} AND id = ${id}`,
        });

        const selectGeneration = SqlSchema.findOneOption({
            Request: StreamPath,
            Result: Schema.Struct({ generation: Schema.Int }),
            execute: (stream) => sql`SELECT generation FROM streams WHERE stream = ${stream}`,
        });

        const selectChunkPage = SqlSchema.findAll({
            Request: Schema.Struct({
                stream: StreamPath,
                from: StoredOffset,
                to: StoredOffset,
                after: Schema.OptionFromNullOr(
                    Schema.Struct({ offset: StoredOffset, part: Schema.Int }),
                ),
            }),
            Result: ChunkRow,
            execute: ({ stream, from, to, after }) =>
                sql`SELECT msg_offset, part, data FROM chunks
                    WHERE stream = ${stream} AND msg_offset >= ${from} AND msg_offset < ${to}
                    ${after === null ? sql.literal('') : sql`AND (msg_offset, part) > (${after.offset}, ${after.part})`}
                    ORDER BY msg_offset, part
                    LIMIT ${pageRows}`,
        });

        const deleteStreamRows = (stream: StreamPath) =>
            Effect.all(
                [
                    sql`DELETE FROM chunks WHERE stream = ${stream}`,
                    sql`DELETE FROM producers WHERE stream = ${stream}`,
                    sql`DELETE FROM streams WHERE stream = ${stream}`,
                ],
                { discard: true },
            );

        // workerd binds a BLOB only from an ArrayBuffer
        const toBlobParameter = (bytes: Uint8Array<ArrayBuffer>) =>
            sql.onDialectOrElse({ sqlite: () => bytes.buffer, orElse: () => bytes });

        // the caller saves the stream row; this saves the producer and the chunk rows
        const saveProducerAndChunks = ({
            stream,
            changes,
        }: {
            readonly stream: StreamPath;
            readonly changes: StreamChanges;
        }) =>
            Effect.gen(function* () {
                if (Option.isSome(changes.producer)) {
                    const { id, state } = changes.producer.value;
                    yield* sql`INSERT INTO producers ${sql.insert({ stream, id, epoch: state.epoch, last_seq: state.lastSeq })}
                        ON CONFLICT (stream, id) DO UPDATE SET epoch = excluded.epoch, last_seq = excluded.last_seq`;
                }
                const chunkRows = Arr.flatMap(changes.messages, (message, index) =>
                    Arr.makeBy(Math.max(1, Math.ceil(message.length / chunkBytes)), (part) => ({
                        stream,
                        msg_offset: changes.firstMessageOffset + index,
                        part,
                        data: toBlobParameter(
                            message.slice(part * chunkBytes, (part + 1) * chunkBytes),
                        ),
                    })),
                );
                yield* Effect.forEach(
                    Arr.chunksOf(chunkRows, chunkRowsPerInsert),
                    (batch) => sql`INSERT INTO chunks ${sql.insert(batch)}`,
                    { discard: true },
                );
            });

        return StreamStorage.of({
            loadStream: ({ stream }) => selectStream(stream).pipe(Effect.orDie),

            createStream: ({ stream, computeChanges }) =>
                sql
                    .withTransaction(
                        Effect.gen(function* () {
                            const stored = yield* selectStream(stream).pipe(Effect.orDie);
                            const { changes, result } = yield* computeChanges(stored);
                            if (Option.isSome(changes)) {
                                yield* Effect.gen(function* () {
                                    yield* deleteStreamRows(stream);
                                    const row = yield* Schema.encodeEffect(StreamRow)(
                                        changes.value.storedStream,
                                    );
                                    yield* sql`INSERT INTO streams ${sql.insert({ stream, ...row })}`;
                                    yield* saveProducerAndChunks({
                                        stream,
                                        changes: changes.value,
                                    });
                                }).pipe(Effect.orDie);
                            }
                            return result;
                        }),
                    )
                    .pipe(Effect.catchTag('SqlError', Effect.die)),

            appendToStream: ({ stream, producerId, computeChanges }) =>
                sql
                    .withTransaction(
                        Effect.gen(function* () {
                            const storedStream = yield* selectStream(stream).pipe(Effect.orDie);
                            const storedProducer = yield* Option.match(producerId, {
                                onNone: () => Effect.succeedNone,
                                onSome: (id) => selectProducer({ stream, id }),
                            }).pipe(Effect.orDie);
                            const { changes, result } = yield* computeChanges({
                                stream: storedStream,
                                producer: storedProducer,
                            });
                            yield* Effect.gen(function* () {
                                const row = yield* Schema.encodeEffect(StreamRow)(
                                    changes.storedStream,
                                );
                                yield* sql`UPDATE streams SET ${sql.update(row)} WHERE stream = ${stream}`;
                                yield* saveProducerAndChunks({ stream, changes });
                            }).pipe(Effect.orDie);
                            return result;
                        }),
                    )
                    .pipe(Effect.catchTag('SqlError', Effect.die)),

            readChunks: ({ stream, from, to, generation }) =>
                Stream.paginate(
                    Option.none<{ readonly offset: Offset; readonly part: number }>(),
                    (after) =>
                        Effect.gen(function* () {
                            const page = yield* selectChunkPage({ stream, from, to, after }).pipe(
                                Effect.orDie,
                            );
                            // checked after the page: if the stream was deleted or re-created
                            // before or during the query, the generation shows it
                            const stored = yield* selectGeneration(stream).pipe(Effect.orDie);
                            if (!Option.exists(stored, (row) => row.generation === generation)) {
                                return yield* StreamNotFoundError.make({});
                            }
                            if (page.length < pageRows) {
                                return [page, Option.none()] as const;
                            }
                            const nextPageAfter = Option.map(Arr.last(page), (chunk) =>
                                Option.some({ offset: chunk.offset, part: chunk.part }),
                            );
                            return [page, nextPageAfter] as const;
                        }),
                ),

            recordAccess: ({ stream, now }) =>
                Effect.gen(function* () {
                    const accessedAt = yield* Schema.encodeEffect(Schema.DateTimeUtcFromString)(
                        now,
                    );
                    yield* sql`UPDATE streams SET last_accessed_at = ${accessedAt} WHERE stream = ${stream}`;
                }).pipe(Effect.orDie),

            deleteStream: ({ stream }) =>
                sql.withTransaction(deleteStreamRows(stream)).pipe(Effect.orDie),
        });
    }),
);
