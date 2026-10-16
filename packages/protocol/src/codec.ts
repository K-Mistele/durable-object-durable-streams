import { Schema, SchemaGetter } from 'effect';

export const Offset = Schema.String.check(Schema.isPattern(/^\d{16}_\d{16}$/)).pipe(
    Schema.brand('Offset'),
);
export type Offset = typeof Offset.Type;

export const OffsetParam = Schema.Union([Schema.Literals(['-1', 'now']), Offset]);
export type OffsetParam = typeof OffsetParam.Type;

// "0" | "[1-9]\d*" → safe integer (Schema.Int is Number.isSafeInteger)
export const NonNegativeIntFromString = Schema.String.check(
    Schema.isPattern(/^(0|[1-9]\d*)$/),
).pipe(
    Schema.decodeTo(Schema.Int, {
        decode: SchemaGetter.transform(Number),
        encode: SchemaGetter.transform(String),
    }),
);

export const ProducerEpoch = NonNegativeIntFromString.pipe(Schema.brand('ProducerEpoch'));
export type ProducerEpoch = typeof ProducerEpoch.Type;

export const ProducerSeq = NonNegativeIntFromString.pipe(Schema.brand('ProducerSeq'));
export type ProducerSeq = typeof ProducerSeq.Type;

export const ProducerId = Schema.NonEmptyString.pipe(Schema.brand('ProducerId'));
export type ProducerId = typeof ProducerId.Type;

export const StreamSeq = Schema.NonEmptyString.pipe(Schema.brand('StreamSeq'));
export type StreamSeq = typeof StreamSeq.Type;

export const TtlSeconds = NonNegativeIntFromString;
export type TtlSeconds = typeof TtlSeconds.Type;

export const Cursor = NonNegativeIntFromString.pipe(Schema.brand('Cursor'));
export type Cursor = typeof Cursor.Type;

// Path of a stream on this server, e.g. "/v1/stream/orders"
export const StreamPath = Schema.String.check(Schema.isPattern(/^\//)).pipe(
    Schema.brand('StreamPath'),
);
export type StreamPath = typeof StreamPath.Type;

// PROTOCOL.md 4.1: "true" in any case means present; any other value means absent, never an error
export const TrueFlag = Schema.String.pipe(
    Schema.decodeTo(Schema.Boolean, {
        decode: SchemaGetter.transform((value) => value.toLowerCase() === 'true'),
        encode: SchemaGetter.transform(String),
    }),
);

// Content-Type is stored and echoed as sent; compare by media type: "Application/JSON; charset=utf-8" → "application/json"
export const mediaTypeOf = (contentType: string): string =>
    (contentType.split(';', 1)[0] ?? '').trim().toLowerCase();
