/**
 * The Worker's `Streams`: each call goes to the stream's `StreamObject` (`getByName(path)`), with
 * the request encoded before and the result decoded after.
 */
import { Effect, Layer, Schema, Stream } from 'effect';
import {
    AppendRequest,
    AppendResult,
    CreateStreamRequest,
    CreateStreamResult,
    EmptyRequest,
    ReadBodyRequest,
    ReadRange,
    StartReadRequest,
    StreamMetadata,
    WaitForNewMessagesRequest,
    WaitResult,
} from '@kylelayer/effect-durable-streams-server/stream-operation-requests';
import { Streams } from '@kylelayer/effect-durable-streams-server/streams';
import { StreamObject, StreamObjectLive } from './stream-object';

/**
 * Encodes `value`, sends it with `send`, and decodes the reply. Both sides are our own code, so a
 * value that fails its schema is a bug: a defect, not a typed failure.
 */
const sendEncoded = <Request, RequestEncoded, Result, ResultEncoded, E>({
    request,
    result,
    value,
    send,
}: {
    readonly request: Schema.Codec<Request, RequestEncoded>;
    readonly result: Schema.Codec<Result, ResultEncoded>;
    readonly value: Request;
    readonly send: (encoded: RequestEncoded) => Effect.Effect<ResultEncoded, E>;
}): Effect.Effect<Result, E> =>
    Schema.encodeEffect(request)(value).pipe(
        Effect.orDie,
        Effect.flatMap(send),
        Effect.flatMap((encoded) => Effect.orDie(Schema.decodeEffect(result)(encoded))),
    );

/**
 * Brings `StreamObject` with it (alchemy's Layers guide): providing this layer to a Worker binds the
 * Durable Object and hosts it in that Worker.
 */
export const layerDurableObject = Layer.effect(
    Streams,
    Effect.gen(function* () {
        const objects = yield* StreamObject;

        // each request also carries `stream`; encoding drops it, since the object knows its path
        return Streams.of({
            createStream: (request) =>
                sendEncoded({
                    request: CreateStreamRequest,
                    result: CreateStreamResult,
                    value: request,
                    send: (encoded) => objects.getByName(request.stream).createStream(encoded),
                }),
            appendToStream: (request) =>
                sendEncoded({
                    request: AppendRequest,
                    result: AppendResult,
                    value: request,
                    send: (encoded) => objects.getByName(request.stream).appendToStream(encoded),
                }),
            getMetadata: (request) =>
                sendEncoded({
                    request: EmptyRequest,
                    result: StreamMetadata,
                    value: request,
                    send: (encoded) => objects.getByName(request.stream).getMetadata(encoded),
                }),
            startRead: (request) =>
                sendEncoded({
                    request: StartReadRequest,
                    result: ReadRange,
                    value: request,
                    send: (encoded) => objects.getByName(request.stream).startRead(encoded),
                }),
            // the bytes cross as they are; a failure after the first chunk ends the stream with a
            // transport error, which only cuts the response short (engine-design.md, Cloudflare wiring)
            readBody: (request) =>
                Schema.encodeEffect(ReadBodyRequest)(request).pipe(
                    Effect.orDie,
                    Effect.map((encoded) => objects.getByName(request.stream).readBody(encoded)),
                    Stream.unwrap,
                ),
            waitForNewMessages: (request) =>
                sendEncoded({
                    request: WaitForNewMessagesRequest,
                    result: WaitResult,
                    value: request,
                    send: (encoded) =>
                        objects.getByName(request.stream).waitForNewMessages(encoded),
                }),
            deleteStream: (request) =>
                sendEncoded({
                    request: EmptyRequest,
                    result: Schema.Void,
                    value: request,
                    send: (encoded) => objects.getByName(request.stream).deleteStream(encoded),
                }),
        });
    }),
).pipe(Layer.provide(StreamObjectLive));
