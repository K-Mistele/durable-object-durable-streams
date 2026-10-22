/**
 * The rows `StreamStorage` reads and writes. Each schema encodes to SQL column values
 * (integers, 0/1, ISO text); none of these cross the Durable Object.
 */
import { Schema } from 'effect';
import {
    Offset,
    ProducerEpoch,
    ProducerId,
    ProducerSeq,
    StreamSeq,
    TtlSeconds,
} from '@kylelayer/effect-durable-streams-protocol/codec';

const StoredOffset = Schema.toType(Offset);

/** Names one producer request by its Producer-Id, Producer-Epoch and Producer-Seq headers. */
export const ProducerRequestId = Schema.Struct({
    id: ProducerId,
    epoch: ProducerEpoch,
    seq: ProducerSeq,
});
export type ProducerRequestId = typeof ProducerRequestId.Type;

/** A row of the `streams` table. */
export const StoredStream = Schema.Struct({
    /** Random, new each time the path is created; a reader holding an old one gets 404. */
    generation: Schema.Int,
    contentType: Schema.String,
    /** The number of messages in the stream, which is also the next message's offset. */
    tail: StoredOffset,
    closed: Schema.BooleanFromBit,
    /** The producer request that closed the stream; its exact retry gets 204 (PROTOCOL.md 5.2.1). */
    closedBy: Schema.OptionFromNullOr(Schema.fromJsonString(ProducerRequestId)),
    ttlSeconds: Schema.OptionFromNullOr(Schema.toType(TtlSeconds)),
    expiresAt: Schema.OptionFromNullOr(Schema.DateTimeUtcFromString),
    createdAt: Schema.DateTimeUtcFromString,
    lastAccessedAt: Schema.DateTimeUtcFromString,
    lastStreamSeq: Schema.OptionFromNullOr(StreamSeq),
});
export type StoredStream = typeof StoredStream.Type;

/** A row of the `chunks` table: one part of one message, parts numbered from 0. */
export const StoredChunk = Schema.Struct({
    offset: StoredOffset,
    part: Schema.Int,
    bytes: Schema.Uint8Array,
});
export type StoredChunk = typeof StoredChunk.Type;
