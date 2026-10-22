import { Schema } from 'effect';
import { Cursor, Offset, ProducerEpoch, ProducerSeq, TrueFlag, TtlSeconds } from './codec';

export const CreateStreamResponseHeaders = Schema.Struct({
    location: Schema.optional(Schema.String),
    'content-type': Schema.String,
    'stream-next-offset': Offset,
    'stream-closed': Schema.optional(TrueFlag),
});
export type CreateStreamResponseHeaders = typeof CreateStreamResponseHeaders.Type;

// stream-next-offset is absent on a duplicate producer append
export const AppendStreamResponseHeaders = Schema.Struct({
    'stream-next-offset': Schema.optional(Offset),
    'stream-closed': Schema.optional(TrueFlag),
    'producer-epoch': Schema.optional(ProducerEpoch),
    'producer-seq': Schema.optional(ProducerSeq),
});
export type AppendStreamResponseHeaders = typeof AppendStreamResponseHeaders.Type;

export const HeadStreamResponseHeaders = Schema.Struct({
    'content-type': Schema.String,
    'stream-next-offset': Offset,
    'stream-closed': Schema.optional(TrueFlag),
    'stream-ttl': Schema.optional(TtlSeconds),
    'stream-expires-at': Schema.optional(Schema.DateTimeUtcFromString),
    etag: Schema.String,
    'cache-control': Schema.String,
});
export type HeadStreamResponseHeaders = typeof HeadStreamResponseHeaders.Type;

// 200 for catch-up and long-poll reads
export const ReadStreamResponseHeaders = Schema.Struct({
    'content-type': Schema.String,
    'stream-next-offset': Offset,
    'stream-up-to-date': Schema.optional(TrueFlag),
    'stream-closed': Schema.optional(TrueFlag),
    'stream-cursor': Schema.optional(Cursor),
    etag: Schema.optional(Schema.String),
    'cache-control': Schema.optional(Schema.String),
});
export type ReadStreamResponseHeaders = typeof ReadStreamResponseHeaders.Type;

// 204 when a long-poll times out or finds the stream closed
export const LongPollNoContentResponseHeaders = Schema.Struct({
    'stream-next-offset': Offset,
    'stream-up-to-date': Schema.optional(TrueFlag),
    'stream-closed': Schema.optional(TrueFlag),
    'stream-cursor': Schema.optional(Cursor),
});
export type LongPollNoContentResponseHeaders = typeof LongPollNoContentResponseHeaders.Type;

export const SseResponseHeaders = Schema.Struct({
    'stream-sse-data-encoding': Schema.optional(Schema.Literal('base64')),
});
export type SseResponseHeaders = typeof SseResponseHeaders.Type;

// 409 on append. HttpApi allows one header-carrying response per status, so both 409 cases share it:
// a closed stream sends Stream-Closed + Stream-Next-Offset, a producer seq gap sends the expected and received seq
export const AppendConflictResponseHeaders = Schema.Struct({
    'stream-closed': Schema.optional(TrueFlag),
    'stream-next-offset': Schema.optional(Offset),
    'producer-expected-seq': Schema.optional(ProducerSeq),
    'producer-received-seq': Schema.optional(ProducerSeq),
});
export type AppendConflictResponseHeaders = typeof AppendConflictResponseHeaders.Type;

// 403 when a producer uses an old epoch; carries the current epoch
export const StaleEpochResponseHeaders = Schema.Struct({
    'producer-epoch': ProducerEpoch,
});
export type StaleEpochResponseHeaders = typeof StaleEpochResponseHeaders.Type;
