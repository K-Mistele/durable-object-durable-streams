import { Match, Schema } from 'effect';
import { HttpApiSchema } from 'effect/http-api';
import { Offset, ProducerEpoch, ProducerSeq } from './codec';
import { AppendConflictResponseHeaders, StaleEpochResponseHeaders } from './response';

// 400: a request the protocol rejects after headers decoded, e.g. empty POST body, invalid JSON
export class BadRequestError extends Schema.TaggedError<
    BadRequestError,
    { readonly brand: unique symbol }
>()('BadRequestError', { reason: Schema.String }, { httpApiStatus: 400 }) {}

// 400: a producer starting a new epoch at a seq other than 0
export class InvalidEpochSeqError extends Schema.TaggedError<
    InvalidEpochSeqError,
    { readonly brand: unique symbol }
>()('InvalidEpochSeqError', {}, { httpApiStatus: 400 }) {}

// 400: fork offset past the source tail, or sub-offset past the next data boundary
export class InvalidForkOffsetError extends Schema.TaggedError<
    InvalidForkOffsetError,
    { readonly brand: unique symbol }
>()('InvalidForkOffsetError', {}, { httpApiStatus: 400 }) {}

// 404: no stream at this path, or no fork source at Stream-Forked-From
export class StreamNotFoundError extends Schema.TaggedError<
    StreamNotFoundError,
    { readonly brand: unique symbol }
>()('StreamNotFoundError', {}, { httpApiStatus: 404 }) {}

// 410: soft-deleted stream (still referenced by forks)
export class StreamGoneError extends Schema.TaggedError<
    StreamGoneError,
    { readonly brand: unique symbol }
>()('StreamGoneError', {}, { httpApiStatus: 410 }) {}

// 409: PUT on an existing stream whose config differs
export class StreamConfigConflictError extends Schema.TaggedError<
    StreamConfigConflictError,
    { readonly brand: unique symbol }
>()('StreamConfigConflictError', {}, { httpApiStatus: 409 }) {}

// 409: append or fork content type differs from the stream's
export class ContentTypeMismatchError extends Schema.TaggedError<
    ContentTypeMismatchError,
    { readonly brand: unique symbol }
>()('ContentTypeMismatchError', {}, { httpApiStatus: 409 }) {}

// 409: Stream-Seq not greater than the last one
export class StreamSeqConflictError extends Schema.TaggedError<
    StreamSeqConflictError,
    { readonly brand: unique symbol }
>()('StreamSeqConflictError', {}, { httpApiStatus: 409 }) {}

// 409: fork source is soft-deleted
export class SourceDeletedError extends Schema.TaggedError<
    SourceDeletedError,
    { readonly brand: unique symbol }
>()('SourceDeletedError', {}, { httpApiStatus: 409 }) {}

// 413: buffered body over the configured cap
export class PayloadTooLargeError extends Schema.TaggedError<
    PayloadTooLargeError,
    { readonly brand: unique symbol }
>()('PayloadTooLargeError', {}, { httpApiStatus: 413 }) {}

// 409 + Stream-Closed + Stream-Next-Offset: append to a closed stream
export class StreamClosedError extends Schema.TaggedError<
    StreamClosedError,
    { readonly brand: unique symbol }
>()('StreamClosedError', { nextOffset: Schema.optional(Offset) }) {}

// 409 + Producer-Expected-Seq + Producer-Received-Seq: producer skipped a seq
export class SequenceGapError extends Schema.TaggedError<
    SequenceGapError,
    { readonly brand: unique symbol }
>()('SequenceGapError', {
    expectedSeq: ProducerSeq,
    receivedSeq: ProducerSeq,
}) {}

// Both 409s carry headers, and HttpApi allows one header-carrying response per status, so they share one schema
export const AppendConflictErrorResponse = Schema.Union([StreamClosedError, SequenceGapError]).pipe(
    HttpApiSchema.encodeToWithHeaders(
        { body: HttpApiSchema.Empty(409), headers: AppendConflictResponseHeaders.fields },
        {
            decode: ({ headers }) => {
                const expectedSeq = headers['producer-expected-seq'];
                const receivedSeq = headers['producer-received-seq'];
                return expectedSeq !== undefined && receivedSeq !== undefined
                    ? SequenceGapError.make({ expectedSeq, receivedSeq })
                    : StreamClosedError.make({ nextOffset: headers['stream-next-offset'] });
            },
            encode: Match.valueTags({
                StreamClosedError: (error) => ({
                    body: undefined,
                    headers: { 'stream-closed': true, 'stream-next-offset': error.nextOffset },
                }),
                SequenceGapError: (error) => ({
                    body: undefined,
                    headers: {
                        'producer-expected-seq': error.expectedSeq,
                        'producer-received-seq': error.receivedSeq,
                    },
                }),
            }),
        },
    ),
);

// 403 + Producer-Epoch (the current one): producer used an old epoch
export class StaleEpochError extends Schema.TaggedError<
    StaleEpochError,
    { readonly brand: unique symbol }
>()('StaleEpochError', {
    currentEpoch: ProducerEpoch,
}) {}

export const StaleEpochErrorResponse = StaleEpochError.pipe(
    HttpApiSchema.encodeToWithHeaders(
        { body: HttpApiSchema.Empty(403), headers: StaleEpochResponseHeaders.fields },
        {
            decode: ({ headers }) =>
                StaleEpochError.make({ currentEpoch: headers['producer-epoch'] }),
            encode: (error) => ({
                body: undefined,
                headers: { 'producer-epoch': error.currentEpoch },
            }),
        },
    ),
);
