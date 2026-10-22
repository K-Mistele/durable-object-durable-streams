/**
 * The request and result of each `StreamOperations` operation. The HTTP handler parses headers and
 * body into these, so a request that doesn't fit one fails there with 400 and nothing past it
 * re-checks. They cross the Durable Object encoded, so each encodes to plain values: Option as
 * null, dates as ISO strings, offsets in wire form, durations as milliseconds, bytes as raw
 * `Uint8Array`. Cloudflare RPC rejects class instances.
 */
import { Schema } from 'effect';
import {
    Cursor,
    Offset,
    OffsetParam,
    ProducerSeq,
    StreamSeq,
    TtlSeconds,
} from '@kylelayer/effect-durable-streams-protocol/codec';
import { ProducerRequestId } from './stored-stream';

const NonEmptyBytes = Schema.Uint8Array.check(
    Schema.makeFilter((bytes) => bytes.length > 0 || 'Expected a non-empty body'),
);

/** What HEAD and the read and write response headers report about a stream. */
export const StreamMetadata = Schema.Struct({
    contentType: Schema.String,
    tail: Offset,
    closed: Schema.Boolean,
    ttlSeconds: Schema.OptionFromNullOr(TtlSeconds),
    expiresAt: Schema.OptionFromNullOr(Schema.DateTimeUtcFromString),
});
export type StreamMetadata = typeof StreamMetadata.Type;

/** For `getMetadata` and `deleteStream`, which need only the stream path. */
export const EmptyRequest = Schema.Struct({});
export type EmptyRequest = typeof EmptyRequest.Type;

/** PUT. `contentType` is already resolved: a missing or malformed header means `application/octet-stream`. */
export const CreateStreamRequest = Schema.Struct({
    contentType: Schema.String,
    ttlSeconds: Schema.OptionFromNullOr(TtlSeconds),
    expiresAt: Schema.OptionFromNullOr(Schema.DateTimeUtcFromString),
    closed: Schema.Boolean,
    /** The stream's first messages; may be empty. */
    body: Schema.Uint8Array,
});
export type CreateStreamRequest = typeof CreateStreamRequest.Type;

/** `isNewStream` false means the stream already existed with this config: 200 instead of 201. */
export const CreateStreamResult = Schema.Struct({
    isNewStream: Schema.Boolean,
    metadata: StreamMetadata,
});
export type CreateStreamResult = typeof CreateStreamResult.Type;

/** POST. A body needs a Content-Type; an empty body is only allowed as a close (PROTOCOL.md 5.2). */
export const AppendRequest = Schema.TaggedUnion({
    AppendMessages: {
        contentType: Schema.String,
        body: NonEmptyBytes,
        closeAfterAppend: Schema.Boolean,
        streamSeq: Schema.OptionFromNullOr(StreamSeq),
        producer: Schema.OptionFromNullOr(ProducerRequestId),
    },
    CloseStream: {
        producer: Schema.OptionFromNullOr(ProducerRequestId),
    },
});
export type AppendRequest = typeof AppendRequest.Type;
export type AppendMessages = typeof AppendRequest.cases.AppendMessages.Type;

export const AppendResult = Schema.Struct({
    nextOffset: Offset,
    closed: Schema.Boolean,
    /** The request was already applied; nothing new was written. */
    wasDuplicate: Schema.Boolean,
    /** The producer's highest accepted seq, sent back as Producer-Seq. */
    acceptedProducerSeq: Schema.OptionFromNullOr(ProducerSeq),
});
export type AppendResult = typeof AppendResult.Type;

/** GET, first call: where the read starts and ends, for the response headers. */
export const StartReadRequest = Schema.Struct({ from: OffsetParam });
export type StartReadRequest = typeof StartReadRequest.Type;

/** `start` is `from` resolved ("-1" → 0, "now" → the tail); `end` is the tail. */
export const ReadRange = Schema.Struct({
    metadata: StreamMetadata,
    start: Offset,
    end: Offset,
    generation: Schema.Int,
});
export type ReadRange = typeof ReadRange.Type;

/** GET, second call: the response body for a `ReadRange`. */
export const ReadBodyRequest = Schema.Struct({
    from: Offset,
    to: Offset,
    generation: Schema.Int,
});
export type ReadBodyRequest = typeof ReadBodyRequest.Type;

/**
 * SSE: a `ReadRange` as SSE text. `closed` and `cursor` fill the control events: `closed` is the
 * range's, so the last control event says streamClosed exactly when the handler stops reading.
 */
export const ReadSseEventsRequest = Schema.Struct({
    from: Offset,
    to: Offset,
    generation: Schema.Int,
    closed: Schema.Boolean,
    cursor: Cursor,
});
export type ReadSseEventsRequest = typeof ReadSseEventsRequest.Type;

/** Long-poll and SSE: wait until a message lands after `after`, the stream closes, or `timeout` passes. */
export const WaitForNewMessagesRequest = Schema.Struct({
    after: Offset,
    timeout: Schema.DurationFromMillis,
});
export type WaitForNewMessagesRequest = typeof WaitForNewMessagesRequest.Type;

export const WaitResult = Schema.TaggedUnion({
    NewMessages: {},
    StreamClosed: {},
    TimedOut: {},
});
export type WaitResult = typeof WaitResult.Type;
