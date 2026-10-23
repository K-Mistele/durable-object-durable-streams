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
import { ChunkKey, StoredChunk, StoredStream, StoredStreamColumns } from './stored-stream';
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
        /**
         * Chunks with `from <= offset < to` (and after `after`, if given), in order, a page at a
         * time. Fails once the stream's generation is no longer `generation` (deleted or re-created).
         */
        readonly readChunks: (input: {
            readonly stream: StreamPath;
            readonly from: Offset;
            readonly to: Offset;
            readonly after: Option.Option<ChunkKey>;
            readonly generation: number;
        }) => Stream.Stream<StoredChunk, StreamNotFoundError>;
        /** Runs `effect` as one transaction. For forks, whose steps don't fit `createStream` or `appendToStream`. */
        readonly withTransaction: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
        /** Deletes any old rows of the path, then inserts the stream's row. */
        readonly insertStream: (input: {
            readonly stream: StreamPath;
            readonly storedStream: StoredStream;
        }) => Effect.Effect<void>;
        readonly updateStream: (input: {
            readonly stream: StreamPath;
            readonly storedStream: StoredStream;
        }) => Effect.Effect<void>;
        /** One message per entry, from `firstMessageOffset`, each cut into 1MB parts. */
        readonly insertMessages: (input: {
            readonly stream: StreamPath;
            readonly firstMessageOffset: Offset;
            readonly messages: ReadonlyArray<Uint8Array>;
        }) => Effect.Effect<void>;
        /** Chunk rows copied from a fork's source; a row already there is left as it is. */
        readonly insertCopiedChunks: (input: {
            readonly stream: StreamPath;
            readonly chunks: ReadonlyArray<StoredChunk>;
        }) => Effect.Effect<void>;
        /** The length of the message at `offset`; None if there is none. */
        readonly messageBytes: (input: {
            readonly stream: StreamPath;
            readonly offset: Offset;
        }) => Effect.Effect<Option.Option<number>>;
        /** The last chunk key before offset `before`: where a fork's copy stopped. */
        readonly lastChunkKeyBefore: (input: {
            readonly stream: StreamPath;
            readonly before: Offset;
        }) => Effect.Effect<Option.Option<ChunkKey>>;
        /** Records `fork` as made from `stream`; recording it twice is harmless. */
        readonly addFork: (input: {
            readonly stream: StreamPath;
            readonly fork: StreamPath;
        }) => Effect.Effect<void>;
        readonly removeFork: (input: {
            readonly stream: StreamPath;
            readonly fork: StreamPath;
        }) => Effect.Effect<void>;
        /** Saves (or replaces) a `removeFork` call still owed to `source`. */
        readonly savePendingRelease: (input: {
            readonly stream: StreamPath;
            readonly release: PendingRelease;
        }) => Effect.Effect<void>;
        readonly loadPendingReleases: (input: {
            readonly stream: StreamPath;
        }) => Effect.Effect<ReadonlyArray<PendingRelease>>;
        readonly deletePendingRelease: (input: {
            readonly stream: StreamPath;
            readonly source: StreamPath;
        }) => Effect.Effect<void>;
        readonly recordAccess: (input: {
            readonly stream: StreamPath;
            readonly now: DateTime.Utc;
        }) => Effect.Effect<void>;
        readonly deleteStream: (input: { readonly stream: StreamPath }) => Effect.Effect<void>;
    }
>()('@kylelayer/effect-durable-streams-server/stream-storage/StreamStorage') {}

/** A `removeFork` call to `source` that failed, and when to try it again. */
export const PendingRelease = Schema.Struct({
    source: StreamPath,
    attempts: Schema.Int,
    /** Epoch milliseconds. */
    dueAt: Schema.Int,
});
export type PendingRelease = typeof PendingRelease.Type;

/** A message is stored in parts of this size, the last part shorter. DO SQLite caps a BLOB at 2MB. */
export const chunkBytes = 1024 * 1024;

/**
 * A read sends its rows in pages, each fetched when the reader asks for more. Between pages the
 * Durable Object waits on the reader, so other requests run. A page ends at whichever comes first:
 * pageBytes of data, or pageRows rows. Every row fits in a page, since chunkBytes < pageBytes.
 */
const pageBytes = 4 * 1024 * 1024;

/** Bounds the rows the page-end query sizes up, so a page of small messages costs at most this many size lookups. */
const pageRows = 4096;

/** DO SQLite allows 100 bound parameters per query, and a chunk row binds 4. */
const chunkRowsPerInsert = 25;

const streamColumnNames = {
    contentType: 'content_type',
    closedBy: 'closed_by',
    ttlSeconds: 'ttl_seconds',
    expiresAt: 'expires_at',
    createdAt: 'created_at',
    lastAccessedAt: 'last_accessed_at',
    lastStreamSeq: 'last_stream_seq',
    softDeleted: 'soft_deleted',
} as const;

/** A `streams` row as loaded, with `has_forks` from the `forks` table. */
const StreamRow = StoredStream.pipe(
    Schema.encodeKeys({ ...streamColumnNames, hasForks: 'has_forks' }),
);

/** A `streams` row as saved: its columns only. */
const StreamColumnsRow = StoredStreamColumns.pipe(Schema.encodeKeys(streamColumnNames));

const PendingReleaseRow = PendingRelease.pipe(Schema.encodeKeys({ dueAt: 'due_at' }));

const ChunkRow = StoredChunk.pipe(Schema.encodeKeys({ offset: 'msg_offset', bytes: 'data' }));

/** A chunk row's key: each page of a read continues after the last key sent. */
const ChunkKeyRow = ChunkKey.pipe(Schema.encodeKeys({ offset: 'msg_offset' }));

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
                        created_at, last_accessed_at, last_stream_seq, fork, copy, soft_deleted,
                        CASE WHEN EXISTS (SELECT 1 FROM forks WHERE forks.stream = streams.stream)
                            THEN 1 ELSE 0 END AS has_forks
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

        // the last row of the next page: the next pageRows rows by key, with a running total of
        // their sizes, and the last one whose total is within pageBytes. LENGTH reads a BLOB's
        // size from the row header, so this loads no data.
        const selectPageEnd = SqlSchema.findOneOption({
            Request: Schema.Struct({
                stream: StreamPath,
                from: StoredOffset,
                to: StoredOffset,
                after: Schema.OptionFromNullOr(ChunkKeyRow),
            }),
            Result: ChunkKeyRow,
            execute: ({ stream, from, to, after }) =>
                sql`SELECT msg_offset, part FROM (
                        SELECT msg_offset, part,
                            SUM(row_bytes) OVER (ORDER BY msg_offset, part) AS bytes_so_far
                        FROM (
                            SELECT msg_offset, part, LENGTH(data) AS row_bytes FROM chunks
                            WHERE stream = ${stream} AND msg_offset >= ${from} AND msg_offset < ${to}
                            ${after === null ? sql.literal('') : sql`AND (msg_offset, part) > (${after.msg_offset}, ${after.part})`}
                            ORDER BY msg_offset, part
                            LIMIT ${pageRows}
                        ) AS next_rows
                    ) AS sized_rows
                    WHERE bytes_so_far <= ${pageBytes}
                    ORDER BY msg_offset DESC, part DESC
                    LIMIT 1`,
        });

        // the rows after `after`, up to and including `last`
        const selectChunkPage = SqlSchema.findAll({
            Request: Schema.Struct({
                stream: StreamPath,
                from: StoredOffset,
                after: Schema.OptionFromNullOr(ChunkKeyRow),
                last: ChunkKeyRow,
            }),
            Result: ChunkRow,
            execute: ({ stream, from, after, last }) =>
                sql`SELECT msg_offset, part, data FROM chunks
                    WHERE stream = ${stream} AND msg_offset >= ${from}
                    ${after === null ? sql.literal('') : sql`AND (msg_offset, part) > (${after.msg_offset}, ${after.part})`}
                    AND (msg_offset, part) <= (${last.msg_offset}, ${last.part})
                    ORDER BY msg_offset, part`,
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

        const insertStreamRow = ({
            stream,
            storedStream,
        }: {
            readonly stream: StreamPath;
            readonly storedStream: StoredStream;
        }) =>
            Effect.gen(function* () {
                yield* deleteStreamRows(stream);
                const row = yield* Schema.encodeEffect(StreamColumnsRow)(storedStream);
                yield* sql`INSERT INTO streams ${sql.insert({ stream, ...row })}`;
            });

        const updateStreamRow = ({
            stream,
            storedStream,
        }: {
            readonly stream: StreamPath;
            readonly storedStream: StoredStream;
        }) =>
            Effect.gen(function* () {
                const row = yield* Schema.encodeEffect(StreamColumnsRow)(storedStream);
                yield* sql`UPDATE streams SET ${sql.update(row)} WHERE stream = ${stream}`;
            });

        const insertChunkRows = ({
            chunkRows,
            ifMissing,
        }: {
            readonly chunkRows: ReadonlyArray<{
                readonly stream: StreamPath;
                readonly msg_offset: number;
                readonly part: number;
                readonly data: Uint8Array | ArrayBuffer;
            }>;
            readonly ifMissing: boolean;
        }) =>
            Effect.forEach(
                Arr.chunksOf(chunkRows, chunkRowsPerInsert),
                (batch) =>
                    sql`INSERT INTO chunks ${sql.insert(batch)}
                        ${ifMissing ? sql.literal('ON CONFLICT DO NOTHING') : sql.literal('')}`,
                { discard: true },
            );

        const insertMessageRows = ({
            stream,
            firstMessageOffset,
            messages,
        }: {
            readonly stream: StreamPath;
            readonly firstMessageOffset: Offset;
            readonly messages: ReadonlyArray<Uint8Array>;
        }) =>
            insertChunkRows({
                chunkRows: Arr.flatMap(messages, (message, index) =>
                    Arr.makeBy(Math.max(1, Math.ceil(message.length / chunkBytes)), (part) => ({
                        stream,
                        msg_offset: firstMessageOffset + index,
                        part,
                        data: toBlobParameter(
                            message.slice(part * chunkBytes, (part + 1) * chunkBytes),
                        ),
                    })),
                ),
                ifMissing: false,
            });

        const selectMessageBytes = SqlSchema.findOneOption({
            Request: Schema.Struct({ stream: StreamPath, offset: StoredOffset }),
            // SUM over no rows is NULL: no message at `offset`
            Result: Schema.Struct({ bytes: Schema.NullOr(Schema.Int) }),
            execute: ({ stream, offset }) =>
                sql`SELECT SUM(LENGTH(data)) AS bytes FROM chunks
                    WHERE stream = ${stream} AND msg_offset = ${offset}`,
        });

        const selectLastChunkKeyBefore = SqlSchema.findOneOption({
            Request: Schema.Struct({ stream: StreamPath, before: StoredOffset }),
            Result: ChunkKeyRow,
            execute: ({ stream, before }) =>
                sql`SELECT msg_offset, part FROM chunks
                    WHERE stream = ${stream} AND msg_offset < ${before}
                    ORDER BY msg_offset DESC, part DESC
                    LIMIT 1`,
        });

        const selectPendingReleases = SqlSchema.findAll({
            Request: StreamPath,
            Result: PendingReleaseRow,
            execute: (stream) =>
                sql`SELECT source, attempts, due_at FROM pending_releases WHERE stream = ${stream}`,
        });

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
                yield* insertMessageRows({
                    stream,
                    firstMessageOffset: changes.firstMessageOffset,
                    messages: changes.messages,
                });
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
                                    yield* insertStreamRow({
                                        stream,
                                        storedStream: changes.value.storedStream,
                                    });
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
                                yield* updateStreamRow({
                                    stream,
                                    storedStream: changes.storedStream,
                                });
                                yield* saveProducerAndChunks({ stream, changes });
                            }).pipe(Effect.orDie);
                            return result;
                        }),
                    )
                    .pipe(Effect.catchTag('SqlError', Effect.die)),

            readChunks: ({ stream, from, to, after: start, generation }) =>
                Stream.paginate(start, (after) =>
                    Effect.gen(function* () {
                        const last = yield* selectPageEnd({ stream, from, to, after }).pipe(
                            Effect.orDie,
                        );
                        const page = yield* Option.match(last, {
                            onNone: () => Effect.succeed(Arr.empty<StoredChunk>()),
                            onSome: (last) =>
                                selectChunkPage({ stream, from, after, last }).pipe(Effect.orDie),
                        });
                        // checked after the queries: if the stream was deleted or re-created
                        // before or during them, the generation shows it
                        const stored = yield* selectGeneration(stream).pipe(Effect.orDie);
                        if (!Option.exists(stored, (row) => row.generation === generation)) {
                            return yield* StreamNotFoundError.make({});
                        }
                        // last is None once no rows are left: this page is empty and the read ends
                        return [page, Option.map(last, Option.some)] as const;
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

            withTransaction: (effect) =>
                sql.withTransaction(effect).pipe(Effect.catchTag('SqlError', Effect.die)),

            insertStream: (input) => insertStreamRow(input).pipe(Effect.orDie),

            updateStream: (input) => updateStreamRow(input).pipe(Effect.orDie),

            insertMessages: (input) => insertMessageRows(input).pipe(Effect.orDie),

            insertCopiedChunks: ({ stream, chunks }) =>
                insertChunkRows({
                    chunkRows: Arr.map(chunks, (chunk) => ({
                        stream,
                        msg_offset: chunk.offset,
                        part: chunk.part,
                        data: toBlobParameter(chunk.bytes.slice()),
                    })),
                    ifMissing: true,
                }).pipe(Effect.orDie),

            messageBytes: ({ stream, offset }) =>
                selectMessageBytes({ stream, offset }).pipe(
                    Effect.map(Option.flatMap((row) => Option.fromNullOr(row.bytes))),
                    Effect.orDie,
                ),

            lastChunkKeyBefore: ({ stream, before }) =>
                selectLastChunkKeyBefore({ stream, before }).pipe(Effect.orDie),

            addFork: ({ stream, fork }) =>
                sql`INSERT INTO forks ${sql.insert({ stream, fork })} ON CONFLICT DO NOTHING`.pipe(
                    Effect.orDie,
                    Effect.andThen(Effect.void),
                ),

            removeFork: ({ stream, fork }) =>
                sql`DELETE FROM forks WHERE stream = ${stream} AND fork = ${fork}`.pipe(
                    Effect.orDie,
                    Effect.andThen(Effect.void),
                ),

            savePendingRelease: ({ stream, release }) =>
                Effect.gen(function* () {
                    const row = yield* Schema.encodeEffect(PendingReleaseRow)(release);
                    yield* sql`INSERT INTO pending_releases ${sql.insert({ stream, ...row })}
                        ON CONFLICT (stream, source)
                        DO UPDATE SET attempts = excluded.attempts, due_at = excluded.due_at`;
                }).pipe(Effect.orDie),

            loadPendingReleases: ({ stream }) => selectPendingReleases(stream).pipe(Effect.orDie),

            deletePendingRelease: ({ stream, source }) =>
                sql`DELETE FROM pending_releases WHERE stream = ${stream} AND source = ${source}`.pipe(
                    Effect.orDie,
                    Effect.andThen(Effect.void),
                ),
        });
    }),
);
