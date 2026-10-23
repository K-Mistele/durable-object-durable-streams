/**
 * The edge: request headers and body parsed into the `stream-operation-requests.ts` types. A
 * request that fits none of them fails here, with 400 or 413, and nothing past here re-checks it.
 */
import { Array as Arr, Data, Effect, Option } from 'effect';
import type { HttpServerRequest } from 'effect/http/HttpServerRequest';
import { StreamPath } from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    BadRequestError,
    PayloadTooLargeError,
    StreamNotFoundError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import type {
    AppendStreamRequestHeaders,
    CreateStreamRequestHeaders,
} from '@kylelayer/effect-durable-streams-protocol/request';
import type { ProducerRequestId } from './stored-stream';
import {
    AppendRequest,
    type CreateForkRequest,
    type CreateStreamRequest,
} from './stream-operation-requests';

const defaultContentType = 'application/octet-stream';

/**
 * The route's `*` match is the path without its leading "/". PROTOCOL.md 6 reserves paths with
 * a `__ds` segment for control APIs and has servers route them before stream operations. We have
 * none (no subscriptions), so such a path is never a stream: 404, as upstream's Cloudflare server.
 */
export const parseStreamPath = (params: {
    readonly '*': string;
}): Effect.Effect<StreamPath, StreamNotFoundError> => {
    const stream = StreamPath.make(`/${params['*']}`);
    return Arr.contains(stream.split('/'), '__ds')
        ? Effect.fail(StreamNotFoundError.make({}))
        : Effect.succeed(stream);
};

/**
 * The whole body, refused with 413 past `maxBytes`. A Content-Length over the cap is refused
 * before reading; a chunked body is checked once read (Workers cap request bodies themselves).
 */
export const readRequestBody = ({
    request,
    contentLength,
    maxBytes,
}: {
    readonly request: HttpServerRequest;
    /** The decoded Content-Length header; absent on a chunked body. */
    readonly contentLength: number | undefined;
    readonly maxBytes: number;
}): Effect.Effect<Uint8Array, PayloadTooLargeError> =>
    Effect.gen(function* () {
        if (contentLength !== undefined && contentLength > maxBytes) {
            return yield* PayloadTooLargeError.make({});
        }
        const buffer = yield* Effect.orDie(request.arrayBuffer);
        if (buffer.byteLength > maxBytes) {
            return yield* PayloadTooLargeError.make({});
        }
        return new Uint8Array(buffer);
    });

/** The Content-Type header, if present and well formed. */
const contentTypeOf = (headers: CreateStreamRequestHeaders): Option.Option<string> =>
    Option.filter(Option.fromUndefinedOr(headers['content-type']), (contentType) =>
        /^[\w-]+\/[\w-]+/.test(contentType),
    );

/** A PUT: a plain create, or a fork when it names a source (PROTOCOL.md 4.2). */
export type ParsedCreateRequest = Data.TaggedEnum<{
    CreateStream: { readonly request: CreateStreamRequest };
    CreateFork: { readonly request: CreateForkRequest };
}>;
export const ParsedCreateRequest = Data.taggedEnum<ParsedCreateRequest>();

/**
 * PUT. For a plain create, a missing or malformed Content-Type means `application/octet-stream`,
 * as upstream; for a fork it means the source's.
 */
export const parseCreateRequest = ({
    headers,
    body,
}: {
    readonly headers: CreateStreamRequestHeaders;
    readonly body: Uint8Array;
}): ParsedCreateRequest => {
    const settings = {
        ttlSeconds: Option.fromUndefinedOr(headers['stream-ttl']),
        expiresAt: Option.fromUndefinedOr(headers['stream-expires-at']),
        closed: headers['stream-closed'] ?? false,
        body,
    };
    const source = headers['stream-forked-from'];
    if (source === undefined) {
        return ParsedCreateRequest.CreateStream({
            request: {
                ...settings,
                contentType: Option.getOrElse(contentTypeOf(headers), () => defaultContentType),
            },
        });
    }
    return ParsedCreateRequest.CreateFork({
        request: {
            ...settings,
            source,
            offset: Option.fromUndefinedOr(headers['stream-fork-offset']),
            subOffset: headers['stream-fork-sub-offset'] ?? 0,
            contentType: contentTypeOf(headers),
        },
    });
};

/**
 * POST (PROTOCOL.md 5.2). A body needs a Content-Type; an empty body is allowed only with
 * `Stream-Closed: true`, as a close.
 */
export const parseAppendRequest = ({
    headers,
    body,
}: {
    readonly headers: AppendStreamRequestHeaders;
    readonly body: Uint8Array;
}): Effect.Effect<AppendRequest, BadRequestError> => {
    const closed = headers['stream-closed'] ?? false;
    const id = headers['producer-id'];
    const epoch = headers['producer-epoch'];
    const seq = headers['producer-seq'];
    // the header schema already requires all three or none
    const producer: Option.Option<ProducerRequestId> =
        id !== undefined && epoch !== undefined && seq !== undefined
            ? Option.some({ id, epoch, seq })
            : Option.none();

    if (body.length === 0) {
        return closed
            ? Effect.succeed(AppendRequest.cases.CloseStream.make({ producer }))
            : Effect.fail(BadRequestError.make({ reason: 'Empty body' }));
    }
    const contentType = headers['content-type'];
    if (contentType === undefined) {
        return Effect.fail(BadRequestError.make({ reason: 'Content-Type header is required' }));
    }
    return Effect.succeed(
        AppendRequest.cases.AppendMessages.make({
            contentType,
            body,
            closeAfterAppend: closed,
            streamSeq: Option.fromUndefinedOr(headers['stream-seq']),
            producer,
        }),
    );
};
