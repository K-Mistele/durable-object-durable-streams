import { Schema } from 'effect';
import { Cursor, Offset, ProducerEpoch, ProducerSeq, TrueFlag, TtlSeconds } from './codec';

export const CreateStreamResponseHeaders = Schema.Struct({
    location: Schema.optionalKey(Schema.String),
    'content-type': Schema.String,
    'stream-next-offset': Offset,
    'stream-closed': Schema.optionalKey(TrueFlag),
});
export type CreateStreamResponseHeaders = typeof CreateStreamResponseHeaders.Type;

// stream-next-offset is absent on a duplicate producer append
export const AppendStreamResponseHeaders = Schema.Struct({
    'stream-next-offset': Schema.optionalKey(Offset),
    'stream-closed': Schema.optionalKey(TrueFlag),
    'producer-epoch': Schema.optionalKey(ProducerEpoch),
    'producer-seq': Schema.optionalKey(ProducerSeq),
});
export type AppendStreamResponseHeaders = typeof AppendStreamResponseHeaders.Type;

export const HeadStreamResponseHeaders = Schema.Struct({
    'content-type': Schema.String,
    'stream-next-offset': Offset,
    'stream-closed': Schema.optionalKey(TrueFlag),
    'stream-ttl': Schema.optionalKey(TtlSeconds),
    'stream-expires-at': Schema.optionalKey(Schema.DateTimeUtcFromString),
    etag: Schema.String,
    'cache-control': Schema.String,
});
export type HeadStreamResponseHeaders = typeof HeadStreamResponseHeaders.Type;

// 200 for catch-up and long-poll reads
export const ReadStreamResponseHeaders = Schema.Struct({
    'content-type': Schema.String,
    'stream-next-offset': Offset,
    'stream-up-to-date': Schema.optionalKey(TrueFlag),
    'stream-closed': Schema.optionalKey(TrueFlag),
    'stream-cursor': Schema.optionalKey(Cursor),
    etag: Schema.optionalKey(Schema.String),
    'cache-control': Schema.optionalKey(Schema.String),
});
export type ReadStreamResponseHeaders = typeof ReadStreamResponseHeaders.Type;

// 204 when a long-poll times out or finds the stream closed
export const LongPollNoContentResponseHeaders = Schema.Struct({
    'stream-next-offset': Offset,
    'stream-up-to-date': Schema.optionalKey(TrueFlag),
    'stream-closed': Schema.optionalKey(TrueFlag),
    'stream-cursor': Schema.optionalKey(Cursor),
});
export type LongPollNoContentResponseHeaders = typeof LongPollNoContentResponseHeaders.Type;

export const SseResponseHeaders = Schema.Struct({
    'stream-sse-data-encoding': Schema.optionalKey(Schema.Literal('base64')),
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
