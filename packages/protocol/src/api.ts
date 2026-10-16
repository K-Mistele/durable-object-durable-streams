import { Schema } from 'effect';
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from 'effect/http-api';
import {
    AppendConflictErrorResponse,
    BadRequestError,
    ContentTypeMismatchError,
    InvalidEpochSeqError,
    InvalidForkOffsetError,
    PayloadTooLargeError,
    SourceDeletedError,
    StaleEpochErrorResponse,
    StreamConfigConflictError,
    StreamGoneError,
    StreamNotFoundError,
    StreamSeqConflictError,
} from './errors.ts';
import {
    AppendStreamRequestHeaders,
    CreateStreamRequestHeaders,
    ReadStreamQuery,
    ReadStreamRequestHeaders,
} from './request.ts';
import {
    AppendStreamResponseHeaders,
    CreateStreamResponseHeaders,
    HeadStreamResponseHeaders,
    LongPollNoContentResponseHeaders,
    ReadStreamResponseHeaders,
} from './response.ts';

// "/*" matches the whole stream path; the router names the match "*" and drops the leading "/"
export const StreamRouteParams = Schema.Struct({ '*': Schema.String });

// Bodiless success members are tagged so the builder can tell same-shaped responses apart by status
export const StreamCreated = Schema.TaggedStruct('StreamCreated', {});
export const StreamExists = Schema.TaggedStruct('StreamExists', {});
export const Appended = Schema.TaggedStruct('Appended', {});
export const ProducerAppended = Schema.TaggedStruct('ProducerAppended', {});
export const LongPollNoContent = Schema.TaggedStruct('LongPollNoContent', {});
export const NotModified = Schema.TaggedStruct('NotModified', {});

export const CreateStream = HttpApiEndpoint.put('createStream', '/*', {
    params: StreamRouteParams,
    headers: CreateStreamRequestHeaders,
    success: [
        HttpApiSchema.WithHeaders(
            StreamCreated.pipe(
                HttpApiSchema.asNoContent({ decode: () => StreamCreated.make({}) }),
                HttpApiSchema.status(201),
            ),
            CreateStreamResponseHeaders,
        ),
        HttpApiSchema.WithHeaders(
            StreamExists.pipe(
                HttpApiSchema.asNoContent({ decode: () => StreamExists.make({}) }),
                HttpApiSchema.status(200),
            ),
            CreateStreamResponseHeaders,
        ),
    ],
    error: [
        BadRequestError,
        InvalidForkOffsetError,
        StreamNotFoundError,
        StreamConfigConflictError,
        ContentTypeMismatchError,
        SourceDeletedError,
        PayloadTooLargeError,
    ],
});

// The body is read raw from the request as a stream, so there is no payload schema
export const AppendStream = HttpApiEndpoint.post('appendStream', '/*', {
    params: StreamRouteParams,
    headers: AppendStreamRequestHeaders,
    success: [
        HttpApiSchema.WithHeaders(
            Appended.pipe(
                HttpApiSchema.asNoContent({ decode: () => Appended.make({}) }),
                HttpApiSchema.status(204),
            ),
            AppendStreamResponseHeaders,
        ),
        HttpApiSchema.WithHeaders(
            ProducerAppended.pipe(
                HttpApiSchema.asNoContent({ decode: () => ProducerAppended.make({}) }),
                HttpApiSchema.status(200),
            ),
            AppendStreamResponseHeaders,
        ),
    ],
    error: [
        BadRequestError,
        InvalidEpochSeqError,
        StaleEpochErrorResponse,
        StreamNotFoundError,
        StreamGoneError,
        AppendConflictErrorResponse,
        ContentTypeMismatchError,
        StreamSeqConflictError,
        PayloadTooLargeError,
    ],
});

// SSE responses are returned as a raw HttpServerResponse: an endpoint can declare only one stream success
export const ReadStream = HttpApiEndpoint.get('readStream', '/*', {
    params: StreamRouteParams,
    query: ReadStreamQuery,
    headers: ReadStreamRequestHeaders,
    success: [
        HttpApiSchema.WithHeaders(HttpApiSchema.StreamUint8Array(), ReadStreamResponseHeaders),
        HttpApiSchema.WithHeaders(
            LongPollNoContent.pipe(
                HttpApiSchema.asNoContent({ decode: () => LongPollNoContent.make({}) }),
                HttpApiSchema.status(204),
            ),
            LongPollNoContentResponseHeaders,
        ),
        HttpApiSchema.WithHeaders(
            NotModified.pipe(
                HttpApiSchema.asNoContent({ decode: () => NotModified.make({}) }),
                HttpApiSchema.status(304),
            ),
            { etag: Schema.String },
        ),
    ],
    error: [BadRequestError, StreamNotFoundError, StreamGoneError],
});

export const HeadStream = HttpApiEndpoint.head('headStream', '/*', {
    params: StreamRouteParams,
    success: HttpApiSchema.WithHeaders(HttpApiSchema.Empty(200), HeadStreamResponseHeaders),
    error: [StreamNotFoundError, StreamGoneError],
});

export const DeleteStream = HttpApiEndpoint.delete('deleteStream', '/*', {
    params: StreamRouteParams,
    error: [StreamNotFoundError, StreamGoneError],
});

export const StreamsGroup = HttpApiGroup.make('streams').add(
    CreateStream,
    AppendStream,
    ReadStream,
    HeadStream,
    DeleteStream,
);

export const DurableStreamsApi = HttpApi.make('DurableStreams').add(StreamsGroup);
