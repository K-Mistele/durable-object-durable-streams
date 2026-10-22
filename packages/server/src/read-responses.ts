/**
 * GET responses: catch-up, long-poll and SSE. Each starts with `startRead` for the header values,
 * then sends `readBody` as the body; the live modes wait with `waitForNewMessages` and read again
 * (engine-design.md **Long-poll and SSE**).
 */
import { Array as Arr, type Duration, Effect, Match, Option, Stream } from 'effect';
import * as HttpServerResponse from 'effect/http/HttpServerResponse';
import { Sse } from 'effect/encoding';
import { HttpApiSchema } from 'effect/http-api';
import { LongPollNoContent, NotModified } from '@kylelayer/effect-durable-streams-protocol/api';
import type {
    Cursor,
    Offset,
    OffsetParam,
    StreamPath,
} from '@kylelayer/effect-durable-streams-protocol/codec';
import { cursorFor } from '@kylelayer/effect-durable-streams-protocol/cursor';
import { etagFor } from '@kylelayer/effect-durable-streams-protocol/etag';
import {
    isBase64,
    sseBinaryDataEvent,
    sseControlEvent,
    sseDataEvent,
} from '@kylelayer/effect-durable-streams-protocol/sse';
import type { ReadRange } from './stream-operation-requests';
import { Streams } from './streams';

/** PROTOCOL.md 10.1: catch-up reads, including the tail, stay valid once sent. */
const catchUpCacheControl = 'public, max-age=60, stale-while-revalidate=300';

const textDecoder = new TextDecoder();

type ReadTarget = {
    readonly stream: StreamPath;
    /** The `offset` query value as sent: the ETag names it, so "-1" and "0…0" differ. */
    readonly requestedOffset: OffsetParam;
};

/** 200 with the messages in `range`, or 304 when If-None-Match already names them. */
const respondWithMessages = ({
    stream,
    requestedOffset,
    range,
    ifNoneMatch,
    cursor,
    cacheControl,
}: ReadTarget & {
    readonly range: ReadRange;
    readonly ifNoneMatch: string | undefined;
    readonly cursor: Cursor | undefined;
    readonly cacheControl: string | undefined;
}) =>
    Effect.gen(function* () {
        const streams = yield* Streams;
        const closed = range.metadata.closed;
        // offset=now always answers the tail, so an ETag would only help a cache serve stale tails
        const etag =
            requestedOffset === 'now'
                ? undefined
                : etagFor({ id: stream, start: requestedOffset, end: range.end, closed });
        if (etag !== undefined && ifNoneMatch === etag) {
            return HttpApiSchema.withHeaders({ body: NotModified.make({}), headers: { etag } });
        }
        return HttpApiSchema.withHeaders({
            body: streams.readBody({
                stream,
                from: range.start,
                to: range.end,
                generation: range.generation,
            }),
            headers: {
                'content-type': range.metadata.contentType,
                'stream-next-offset': range.end,
                // no read cap: every read runs to the tail
                'stream-up-to-date': true,
                'stream-closed': closed || undefined,
                'stream-cursor': cursor,
                etag,
                'cache-control': cacheControl,
            },
        });
    });

/** A plain GET: everything from the offset to the tail. */
export const respondToCatchUpRead = ({
    stream,
    requestedOffset,
    ifNoneMatch,
}: ReadTarget & { readonly ifNoneMatch: string | undefined }) =>
    Effect.gen(function* () {
        const streams = yield* Streams;
        const range = yield* streams.startRead({ stream, from: requestedOffset });
        return yield* respondWithMessages({
            stream,
            requestedOffset,
            range,
            ifNoneMatch,
            cursor: undefined,
            cacheControl: requestedOffset === 'now' ? 'no-store' : catchUpCacheControl,
        });
    });

/** 204 for a long-poll that found no new messages (PROTOCOL.md 5.7). */
const respondWithNoNewMessages = ({
    nextOffset,
    closed,
    cursor,
}: {
    readonly nextOffset: Offset;
    readonly closed: boolean;
    readonly cursor: Cursor | undefined;
}) =>
    HttpApiSchema.withHeaders({
        body: LongPollNoContent.make({}),
        headers: {
            'stream-next-offset': nextOffset,
            'stream-up-to-date': true,
            'stream-closed': closed || undefined,
            'stream-cursor': cursor,
        },
    });

/**
 * `live=long-poll`: messages past the offset now, or wait up to `timeout` for some. A stream
 * already closed at the offset answers 204 at once.
 */
export const respondToLongPollRead = ({
    stream,
    requestedOffset,
    ifNoneMatch,
    clientCursor,
    timeout,
}: ReadTarget & {
    readonly ifNoneMatch: string | undefined;
    readonly clientCursor: string | undefined;
    readonly timeout: Duration.Duration;
}) =>
    Effect.gen(function* () {
        const streams = yield* Streams;
        const respondWithNewMessages = (range: ReadRange) =>
            Effect.gen(function* () {
                const cursor = yield* cursorFor(clientCursor);
                return yield* respondWithMessages({
                    stream,
                    requestedOffset,
                    range,
                    ifNoneMatch,
                    cursor,
                    cacheControl: undefined,
                });
            });

        const range = yield* streams.startRead({ stream, from: requestedOffset });
        if (range.start < range.end) {
            return yield* respondWithNewMessages(range);
        }
        if (range.metadata.closed) {
            return respondWithNoNewMessages({
                nextOffset: range.end,
                closed: true,
                cursor: undefined,
            });
        }
        const waitResult = yield* streams.waitForNewMessages({
            stream,
            after: range.start,
            timeout,
        });
        return yield* Match.valueTags(waitResult, {
            NewMessages: () =>
                streams
                    .startRead({ stream, from: range.start })
                    .pipe(Effect.flatMap(respondWithNewMessages)),
            StreamClosed: () =>
                cursorFor(clientCursor).pipe(
                    Effect.map((cursor) =>
                        respondWithNoNewMessages({ nextOffset: range.start, closed: true, cursor }),
                    ),
                ),
            TimedOut: () =>
                cursorFor(clientCursor).pipe(
                    Effect.map((cursor) =>
                        respondWithNoNewMessages({
                            nextOffset: range.start,
                            closed: false,
                            cursor,
                        }),
                    ),
                ),
        });
    });

/**
 * `live=sse` (PROTOCOL.md 5.8): a data event with the messages read, then a control event; then
 * wait and repeat. A wait that times out repeats the control event as a keep-alive. The response
 * ends after the closing control event, or after `lifetime` so the client reconnects from its
 * last `streamNextOffset`.
 */
export const respondToSseRead = ({
    stream,
    requestedOffset,
    clientCursor,
    waitTimeout,
    lifetime,
}: ReadTarget & {
    readonly clientCursor: string | undefined;
    readonly waitTimeout: Duration.Duration;
    readonly lifetime: Duration.Duration;
}) =>
    Effect.gen(function* () {
        const streams = yield* Streams;
        // the first read runs before the response starts, so a missing stream still gets its 404
        const firstRange = yield* streams.startRead({ stream, from: requestedOffset });
        const sendsBase64 = isBase64(firstRange.metadata.contentType);

        const eventsForRange = (range: ReadRange) =>
            Effect.gen(function* () {
                const control = range.metadata.closed
                    ? yield* sseControlEvent({ streamNextOffset: range.end, streamClosed: true })
                    : yield* sseControlEvent({
                          streamNextOffset: range.end,
                          streamCursor: yield* cursorFor(clientCursor),
                          upToDate: true,
                      });
                if (range.start === range.end) {
                    return Arr.of(control);
                }
                const bytes = yield* Stream.mkUint8Array(
                    streams.readBody({
                        stream,
                        from: range.start,
                        to: range.end,
                        generation: range.generation,
                    }),
                );
                const data = sendsBase64
                    ? sseBinaryDataEvent(bytes)
                    : sseDataEvent(textDecoder.decode(bytes));
                return Arr.make(data, control);
            });

        const eventsAfterWaiting = Stream.paginate(firstRange.end, (after: Offset) =>
            Effect.gen(function* () {
                yield* streams.waitForNewMessages({ stream, after, timeout: waitTimeout });
                const range = yield* streams.startRead({ stream, from: after });
                const events = yield* eventsForRange(range);
                const next = range.metadata.closed ? Option.none() : Option.some(range.end);
                return [events, next] as const;
            }),
        );

        const events = Stream.fromIterableEffect(eventsForRange(firstRange)).pipe(
            Stream.concat(firstRange.metadata.closed ? Stream.empty : eventsAfterWaiting),
            Stream.interruptWhen(Effect.sleep(lifetime)),
            // the status is already sent: a stream deleted mid-response just ends it
            Stream.catchTags({
                StreamNotFoundError: () => Stream.empty,
                StreamGoneError: () => Stream.empty,
            }),
            Stream.map((event) => Sse.encoder.write(event)),
            Stream.encodeText,
            Stream.orDie,
        );

        return HttpServerResponse.stream(events, {
            contentType: 'text/event-stream',
            headers: {
                'cache-control': 'no-cache',
                'stream-sse-data-encoding': sendsBase64 ? 'base64' : undefined,
            },
        });
    });
