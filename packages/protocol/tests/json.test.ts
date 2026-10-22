import { describe, expect, it } from '@effect/vitest';
import { Effect, Schema, Stream } from 'effect';
import { jsonArrayBody, jsonMessages } from '../src/json';

const JsonText = Schema.fromJsonString(Schema.Json);
const encodeJsonText = Schema.encodeEffect(JsonText);
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

const messagesOf = (jsonText: string) =>
    jsonMessages(Stream.make(textEncoder.encode(jsonText)), { allowEmpty: false }).pipe(
        Stream.runCollect,
    );

describe('jsonMessages and jsonArrayBody', () => {
    it.effect.prop(
        'an array splits into one message per item, and the read body joins them back',
        [Schema.NonEmptyArray(Schema.Json)],
        ([items]) =>
            Effect.gen(function* () {
                const itemsText = yield* encodeJsonText(items);
                const messages = yield* messagesOf(itemsText);
                expect(messages).toHaveLength(items.length);

                const readBody = yield* jsonArrayBody(
                    Stream.fromIterable(messages).pipe(
                        Stream.map((message) => textDecoder.decode(message)),
                    ),
                ).pipe(Stream.decodeText, Stream.mkString);
                // compare as text: JSON can't tell -0 from 0, and both sides go through it
                expect(readBody).toBe(itemsText);
            }),
    );

    it.effect.prop(
        'a value that is not an array is one message',
        [Schema.Record(Schema.String, Schema.Json)],
        ([object]) =>
            Effect.gen(function* () {
                const objectText = yield* encodeJsonText(object);
                const messages = yield* messagesOf(objectText);
                expect(messages).toHaveLength(1);
                expect(textDecoder.decode(messages[0])).toBe(objectText);
            }),
    );
});
