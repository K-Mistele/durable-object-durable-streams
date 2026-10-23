/**
 * The rows `StreamStorage` reads and writes. Each schema encodes to SQL column values
 * (integers, 0/1, ISO text, JSON text); none of these cross the Durable Object.
 */
import { Schema } from 'effect';
import {
    Offset,
    ProducerEpoch,
    ProducerId,
    ProducerSeq,
    StreamPath,
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

/** A chunk row's key. Reads continue after one; a fork's copy resumes after one. */
export const ChunkKey = Schema.Struct({ offset: StoredOffset, part: Schema.Int });
export type ChunkKey = typeof ChunkKey.Type;

/** The PUT that made a stream a fork, as requested, so a repeat of it can be recognized (PROTOCOL.md 4.2). */
export const StoredFork = Schema.Struct({
    source: StreamPath,
    /** None: the source's tail at the time. */
    offset: Schema.OptionFromNullOr(StoredOffset),
    /** 0 when the header was absent, which the spec treats the same. */
    subOffset: Schema.Int,
});
export type StoredFork = typeof StoredFork.Type;

/** A fork still being created. Every request but a repeat of its PUT answers 404 until it is done. */
export const CopyProgress = Schema.TaggedUnion({
    /** The row exists, but the source has not yet agreed to the fork. */
    Claimed: {},
    Copying: {
        /** Copy the source's messages before this offset. */
        to: StoredOffset,
        /** Binary sub-offset: message `to - 1` stops after this many bytes. */
        lastBytes: Schema.OptionFromNullOr(Schema.Int),
        startedAttempts: Schema.Int,
        /** Where the last attempt started; the next one starting at the same key means it made no progress. */
        lastStart: Schema.OptionFromNullOr(ChunkKey),
        /** Attempts that made no progress. */
        failures: Schema.Int,
    },
});
export type CopyProgress = typeof CopyProgress.Type;

const storedStreamFields = {
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
    fork: Schema.OptionFromNullOr(Schema.fromJsonString(StoredFork)),
    copy: Schema.OptionFromNullOr(Schema.fromJsonString(CopyProgress)),
    /** Deleted while forks still copy from it: 410 to its own requests (PROTOCOL.md 4.2). */
    softDeleted: Schema.BooleanFromBit,
};

/** The columns of a `streams` row that a write saves. */
export const StoredStreamColumns = Schema.Struct(storedStreamFields);

/** A row of the `streams` table, as loaded. */
export const StoredStream = Schema.Struct({
    ...storedStreamFields,
    /** Some fork was made from this stream and not yet deleted (the `forks` table, not a column). */
    hasForks: Schema.BooleanFromBit,
});
export type StoredStream = typeof StoredStream.Type;

/** A row of the `chunks` table: one part of one message, parts numbered from 0. */
export const StoredChunk = Schema.Struct({
    offset: StoredOffset,
    part: Schema.Int,
    bytes: Schema.Uint8Array,
});
export type StoredChunk = typeof StoredChunk.Type;
