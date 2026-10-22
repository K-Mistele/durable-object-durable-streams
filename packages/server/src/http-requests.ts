/**
 * The edge: request headers and body parsed into the `stream-operation-requests.ts` types. A
 * request that fits none of them fails here, with 400 or 413, and nothing past here re-checks it.
 */
import { Effect, Option } from 'effect';
import type { HttpServerRequest } from 'effect/http/HttpServerRequest';
import { StreamPath } from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    BadRequestError,
    PayloadTooLargeError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import type {
    AppendStreamRequestHeaders,
    CreateStreamRequestHeaders,
} from '@kylelayer/effect-durable-streams-protocol/request';
import type { ProducerRequestId } from './stored-stream';
import { AppendRequest, type CreateStreamRequest } from './stream-operation-requests';

const defaultContentType = 'application/octet-stream';

/** The route's `*` match is the path without its leading "/". */
export const streamPathOf = (params: { readonly '*': string }): StreamPath =>
    StreamPath.make(`/${params['*']}`);

/**
 * The whole body, refused with 413 past `maxBytes`. A Content-Length over the cap is refused
 * before reading; a chunked body is checked once read (Workers cap request bodies themselves).
 */
export const readRequestBody = ({
    request,
    maxBytes,
}: {
    readonly request: HttpServerRequest;
    readonly maxBytes: number;
}): Effect.Effect<Uint8Array, PayloadTooLargeError> =>
    Effect.gen(function* () {
        const declaredLength = Number(request.headers['content-length'] ?? 0);
        if (declaredLength > maxBytes) {
            return yield* PayloadTooLargeError.make({});
        }
        const buffer = yield* Effect.orDie(request.arrayBuffer);
        if (buffer.byteLength > maxBytes) {
            return yield* PayloadTooLargeError.make({});
        }
        return new Uint8Array(buffer);
    });

/** PUT. A missing or malformed Content-Type means `application/octet-stream`, as upstream. */
export const parseCreateStreamRequest = ({
    headers,
    body,
}: {
    readonly headers: CreateStreamRequestHeaders;
    readonly body: Uint8Array;
}): Effect.Effect<CreateStreamRequest, BadRequestError> => {
    if (headers['stream-forked-from'] !== undefined) {
        return Effect.fail(BadRequestError.make({ reason: 'Forks are not supported yet' }));
    }
    const sentContentType = headers['content-type'] ?? '';
    const contentType = /^[\w-]+\/[\w-]+/.test(sentContentType)
        ? sentContentType
        : defaultContentType;
    return Effect.succeed({
        contentType,
        ttlSeconds: Option.fromUndefinedOr(headers['stream-ttl']),
        expiresAt: Option.fromUndefinedOr(headers['stream-expires-at']),
        closed: headers['stream-closed'] ?? false,
        body,
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
