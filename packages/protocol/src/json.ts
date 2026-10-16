import { Array as Arr, Effect, Schema, Stream } from 'effect';
import { BadRequestError } from './errors';

const JsonText = Schema.fromJsonString(Schema.Json);

// Version 1: buffers the body. The stream-shaped signature lets a streaming parser replace it
// later without touching callers (json-append.md).
// [1,{"a":2}] → '1', '{"a":2}';  {"a":1} → '{"a":1}';  [] fails on POST, no messages on PUT
export const jsonMessages = <E, R>(
    body: Stream.Stream<Uint8Array, E, R>,
    options: { readonly allowEmpty: boolean },
): Stream.Stream<Uint8Array, E | BadRequestError, R> =>
    body.pipe(
        Stream.decodeText,
        Stream.mkString,
        Effect.flatMap((text) =>
            Schema.decodeEffect(JsonText)(text).pipe(
                Effect.mapError(() => BadRequestError.make({ reason: 'Invalid JSON' })),
            ),
        ),
        Effect.flatMap((value) => {
            const items = Arr.isArray<Schema.Json>(value) ? value : [value];
            return items.length === 0 && !options.allowEmpty
                ? Effect.fail(BadRequestError.make({ reason: 'Empty arrays are not allowed' }))
                : Effect.succeed(items);
        }),
        Effect.map((items) =>
            Stream.fromIterable(items).pipe(
                Stream.mapEffect((item) => Schema.encodeEffect(JsonText)(item).pipe(Effect.orDie)),
                Stream.encodeText,
            ),
        ),
        Stream.unwrap,
    );

// read body: messages joined into one JSON array; no messages gives "[]"
export const jsonArrayBody = <E, R>(
    messages: Stream.Stream<string, E, R>,
): Stream.Stream<Uint8Array, E, R> =>
    messages.pipe(
        Stream.intersperseAffixes({ start: '[', middle: ',', end: ']' }),
        Stream.encodeText,
    );
