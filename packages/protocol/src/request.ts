import { Array, Effect, Predicate, Schema, SchemaGetter, pipe } from 'effect';
import {
    NonNegativeIntFromString,
    Offset,
    OffsetParam,
    ProducerEpoch,
    ProducerId,
    ProducerSeq,
    TrueFlag,
    StreamPath,
    StreamSeq,
    TtlSeconds,
} from './codec';

export const CreateStreamRequestHeaders = Schema.Struct({
    'content-type': Schema.optionalKey(Schema.String),
    'content-length': Schema.optionalKey(NonNegativeIntFromString),
    'stream-ttl': Schema.optionalKey(TtlSeconds),
    'stream-expires-at': Schema.optionalKey(Schema.DateTimeUtcFromString),
    'stream-closed': Schema.optionalKey(TrueFlag),
    'stream-forked-from': Schema.optionalKey(StreamPath),
    'stream-fork-offset': Schema.optionalKey(Offset),
    'stream-fork-sub-offset': Schema.optionalKey(NonNegativeIntFromString),
}).check(
    Schema.makeFilter(
        (h) =>
            h['stream-ttl'] === undefined ||
            h['stream-expires-at'] === undefined ||
            'Cannot specify both Stream-TTL and Stream-Expires-At',
    ),
    Schema.makeFilter(
        (h) =>
            h['stream-fork-offset'] === undefined ||
            h['stream-forked-from'] !== undefined ||
            'Stream-Fork-Offset requires Stream-Forked-From',
    ),
    Schema.makeFilter(
        (h) =>
            h['stream-fork-sub-offset'] === undefined ||
            h['stream-forked-from'] !== undefined ||
            'Stream-Fork-Sub-Offset requires Stream-Forked-From',
    ),
);
export type CreateStreamRequestHeaders = typeof CreateStreamRequestHeaders.Type;

export const AppendStreamRequestHeaders = Schema.Struct({
    'content-type': Schema.optionalKey(Schema.String),
    'content-length': Schema.optionalKey(NonNegativeIntFromString),
    'stream-seq': Schema.optionalKey(StreamSeq),
    'stream-closed': Schema.optionalKey(TrueFlag),
    'producer-id': Schema.optionalKey(ProducerId),
    'producer-epoch': Schema.optionalKey(ProducerEpoch),
    'producer-seq': Schema.optionalKey(ProducerSeq),
}).check(
    Schema.makeFilter((h) => {
        const sent = pipe(
            Array.make(h['producer-id'], h['producer-epoch'], h['producer-seq']),
            Array.countBy(Predicate.isNotUndefined),
        );
        return sent === 0 || sent === 3 || 'Producer headers must be sent together';
    }),
);
export type AppendStreamRequestHeaders = typeof AppendStreamRequestHeaders.Type;

export const ReadStreamRequestHeaders = Schema.Struct({
    'if-none-match': Schema.optionalKey(Schema.String),
});
export type ReadStreamRequestHeaders = typeof ReadStreamRequestHeaders.Type;

// cursor stays a string: a bad client cursor is ignored and replaced, not rejected
export const ReadStreamQuery = Schema.Union([
    Schema.Struct({
        offset: OffsetParam,
        live: Schema.Literal('long-poll'),
        cursor: Schema.optionalKey(Schema.String),
    }),
    Schema.Struct({
        offset: OffsetParam,
        live: Schema.Literal('sse'),
        cursor: Schema.optionalKey(Schema.String),
    }),
    // any other `live` value is a catch-up read, as upstream; long-poll and SSE without an
    // offset must not fall through to here
    Schema.Struct({
        offset: OffsetParam.pipe(Schema.withDecodingDefaultKey(Effect.succeed('-1'))),
        live: Schema.optionalKey(
            Schema.String.check(
                Schema.makeFilter(
                    (live) =>
                        (live !== 'long-poll' && live !== 'sse') ||
                        `live=${live} requires an offset`,
                ),
            ).pipe(
                // decoded to one literal, so handlers can tell it from long-poll and SSE
                Schema.decodeTo(Schema.Literal('catch-up'), {
                    decode: SchemaGetter.transform(() => 'catch-up' as const),
                    encode: SchemaGetter.transform((live) => live),
                }),
            ),
        ),
    }),
]);
export type ReadStreamQuery = typeof ReadStreamQuery.Type;
