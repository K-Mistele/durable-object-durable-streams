/**
 * The HttpApi handlers for `DurableStreamsApi`, over `Streams`. They own status codes, response
 * headers, ETags and cache headers; requests are parsed in `http-requests.ts` and GET responses
 * built in `read-responses.ts`. Hosts provide `Streams` and the HTTP platform services.
 */
import { Config, Duration, Effect, Layer, Match, Option, Predicate } from 'effect';
import * as HttpEffect from 'effect/http/HttpEffect';
import * as HttpRouter from 'effect/http/HttpRouter';
import * as HttpServerRequest from 'effect/http/HttpServerRequest';
import * as HttpServerResponse from 'effect/http/HttpServerResponse';
import { HttpApiBuilder, HttpApiSchema } from 'effect/http-api';
import {
    Appended,
    DurableStreamsApi,
    ProducerAppended,
    StreamCreated,
    StreamExists,
} from '@kylelayer/effect-durable-streams-protocol/api';
import { etagFor } from '@kylelayer/effect-durable-streams-protocol/etag';
import {
    parseAppendRequest,
    parseCreateStreamRequest,
    readRequestBody,
    streamPathOf,
} from './http-requests';
import { respondToCatchUpRead, respondToLongPollRead, respondToSseRead } from './read-responses';
import { Streams } from './streams';

/** engine-design.md decision 21. The conformance run sets the long-poll timeout to 2s. */
export const HttpSettings = Config.all({
    longPollTimeout: Config.Duration('DURABLE_STREAMS_LONG_POLL_TIMEOUT').pipe(
        Config.withDefault(Duration.seconds(30)),
    ),
    sseLifetime: Config.Duration('DURABLE_STREAMS_SSE_LIFETIME').pipe(
        Config.withDefault(Duration.seconds(60)),
    ),
    maxAppendBytes: Config.Int('DURABLE_STREAMS_MAX_APPEND_BYTES').pipe(
        Config.withDefault(32 * 1024 * 1024),
    ),
});

export const StreamsHandlers = HttpApiBuilder.group(DurableStreamsApi, 'streams', (handlers) =>
    Effect.gen(function* () {
        const settings = yield* HttpSettings;
        const streams = yield* Streams;

        return handlers
            .handle('createStream', ({ params, headers, request }) =>
                Effect.gen(function* () {
                    const stream = streamPathOf(params);
                    const body = yield* readRequestBody({
                        request,
                        maxBytes: settings.maxAppendBytes,
                    });
                    const createRequest = yield* parseCreateStreamRequest({ headers, body });
                    const { isNewStream, metadata } = yield* streams.createStream({
                        ...createRequest,
                        stream,
                    });
                    const responseHeaders = {
                        'content-type': metadata.contentType,
                        'stream-next-offset': metadata.tail,
                        'stream-closed': metadata.closed || undefined,
                    };
                    if (!isNewStream) {
                        return HttpApiSchema.withHeaders({
                            body: StreamExists.make({}),
                            headers: responseHeaders,
                        });
                    }
                    const location = HttpServerRequest.toURL(request).pipe(
                        Option.map((url) => `${url.origin}${url.pathname}`),
                        Option.getOrUndefined,
                    );
                    return HttpApiSchema.withHeaders({
                        body: StreamCreated.make({}),
                        headers: { ...responseHeaders, location },
                    });
                }),
            )
            .handle('appendStream', ({ params, headers, request }) =>
                Effect.gen(function* () {
                    const stream = streamPathOf(params);
                    const body = yield* readRequestBody({
                        request,
                        maxBytes: settings.maxAppendBytes,
                    });
                    const appendRequest = yield* parseAppendRequest({ headers, body });
                    const result = yield* streams.appendToStream({ ...appendRequest, stream });
                    // a duplicate of an append (not of the close) wrote nothing, so it names no offset
                    const isDuplicateOfAppend = result.wasDuplicate && !result.closed;
                    // PROTOCOL.md 5.2.1: 200 for an accepted producer append, 204 otherwise
                    const isAcceptedProducerAppend =
                        Option.isSome(appendRequest.producer) &&
                        !result.wasDuplicate &&
                        Predicate.isTagged(appendRequest, 'AppendMessages');
                    const responseHeaders = {
                        'stream-next-offset': isDuplicateOfAppend ? undefined : result.nextOffset,
                        'stream-closed': result.closed || undefined,
                        'producer-epoch': appendRequest.producer.pipe(
                            Option.map((producer) => producer.epoch),
                            Option.getOrUndefined,
                        ),
                        'producer-seq': Option.getOrUndefined(result.acceptedProducerSeq),
                    };
                    if (isAcceptedProducerAppend) {
                        return HttpApiSchema.withHeaders({
                            body: ProducerAppended.make({}),
                            headers: responseHeaders,
                        });
                    }
                    return HttpApiSchema.withHeaders({
                        body: Appended.make({}),
                        headers: responseHeaders,
                    });
                }),
            )
            .handle('readStream', ({ params, query, headers }) => {
                const stream = streamPathOf(params);
                const ifNoneMatch = headers['if-none-match'];
                return Match.value(query).pipe(
                    Match.when({ live: 'long-poll' }, (longPoll) =>
                        respondToLongPollRead({
                            stream,
                            requestedOffset: longPoll.offset,
                            ifNoneMatch,
                            clientCursor: longPoll.cursor,
                            timeout: settings.longPollTimeout,
                        }),
                    ),
                    Match.when({ live: 'sse' }, (sse) =>
                        respondToSseRead({
                            stream,
                            requestedOffset: sse.offset,
                            clientCursor: sse.cursor,
                            waitTimeout: settings.longPollTimeout,
                            lifetime: settings.sseLifetime,
                        }),
                    ),
                    Match.orElse((catchUp) =>
                        respondToCatchUpRead({
                            stream,
                            requestedOffset: catchUp.offset,
                            ifNoneMatch,
                        }),
                    ),
                );
            })
            .handle('headStream', ({ params }) => {
                const stream = streamPathOf(params);
                return streams.getMetadata({ stream }).pipe(
                    Effect.map((metadata) =>
                        HttpApiSchema.withHeaders({
                            body: undefined,
                            headers: {
                                'content-type': metadata.contentType,
                                'stream-next-offset': metadata.tail,
                                'stream-closed': metadata.closed || undefined,
                                'stream-ttl': Option.getOrUndefined(metadata.ttlSeconds),
                                'stream-expires-at': Option.getOrUndefined(metadata.expiresAt),
                                etag: etagFor({
                                    id: stream,
                                    start: '-1',
                                    end: metadata.tail,
                                    closed: metadata.closed,
                                }),
                                // PROTOCOL.md 5.4: a cached HEAD would report a stale tail
                                'cache-control': 'no-store',
                            },
                        }),
                    ),
                );
            })
            .handle('deleteStream', ({ params }) =>
                streams.deleteStream({ stream: streamPathOf(params) }),
            );
    }),
);

const protocolRequestHeaders = [
    'content-type',
    'authorization',
    'if-none-match',
    'stream-seq',
    'stream-ttl',
    'stream-expires-at',
    'stream-closed',
    'producer-id',
    'producer-epoch',
    'producer-seq',
    'stream-forked-from',
    'stream-fork-offset',
    'stream-fork-sub-offset',
];

const protocolResponseHeaders = [
    'stream-next-offset',
    'stream-cursor',
    'stream-up-to-date',
    'stream-closed',
    'producer-epoch',
    'producer-seq',
    'producer-expected-seq',
    'producer-received-seq',
    'etag',
    'content-type',
    'content-encoding',
    'vary',
];

/** PROTOCOL.md 10.7, on every response, errors included. */
const layerSecurityHeaders = HttpRouter.middleware(
    (httpEffect) =>
        HttpEffect.withPreResponseHandler(httpEffect, (_request, response) =>
            Effect.succeed(
                HttpServerResponse.setHeaders(response, {
                    'x-content-type-options': 'nosniff',
                    'cross-origin-resource-policy': 'cross-origin',
                }),
            ),
        ),
    { global: true },
);

/** The whole HTTP API: handlers, CORS and security headers. Needs `Streams` and the HTTP platform. */
export const layerDurableStreamsHttpApi = HttpApiBuilder.layer(DurableStreamsApi).pipe(
    Layer.provide([
        StreamsHandlers,
        layerSecurityHeaders,
        HttpRouter.cors({
            allowedMethods: ['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'],
            allowedHeaders: protocolRequestHeaders,
            exposedHeaders: protocolResponseHeaders,
        }),
    ]),
);
