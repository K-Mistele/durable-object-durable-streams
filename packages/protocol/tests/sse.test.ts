import { describe, expect, it } from '@effect/vitest';
import { Arbitrary, Array as Arr, Order, Schema } from 'effect';
import { Base64, Sse } from 'effect/encoding';
import {
    closeSseDataEvent,
    openSseDataEvent,
    type SseDataEncoding,
    startSseDataMessage,
    writeSseData,
} from '../src/sse';

const utf8 = new TextEncoder();
const textDecoder = new TextDecoder();

// line breaks of each kind, and characters of 1 to 4 UTF-8 bytes, so cuts land inside them
const MessageText = Arbitrary.schema(
    Schema.NonEmptyArray(Schema.Literals(['a', '\r', '\n', '\r\n', 'é', '\u{1F600}'])),
).pipe(Arbitrary.map((characters) => Arr.join(characters, '')));

const Messages = Arbitrary.array(MessageText, { minLength: 1, maxLength: 5 });

const CutPoints = Arbitrary.array(
    Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 40 }))),
    { maxLength: 6 },
);

/** The data event written piece by piece, as text. */
const writtenInPieces = (input: {
    readonly encoding: SseDataEncoding;
    readonly messages: ReadonlyArray<string>;
    readonly cutPoints: ReadonlyArray<number>;
}) => {
    const [opened, openText] = openSseDataEvent(input.encoding);
    const pieces: Array<Uint8Array> = [openText];
    const written = Arr.reduce(input.messages, opened, (event, message) => {
        const [started, separator] = startSseDataMessage(event);
        pieces.push(separator);
        // cut the message's bytes at the cut points (modulo its length), as storage cuts parts
        const bytes = utf8.encode(message);
        const cuts = Arr.dedupe(
            Arr.sort(
                Arr.map(input.cutPoints, (cut) => cut % bytes.byteLength),
                Order.Number,
            ),
        );
        const ends = [...Arr.filter(cuts, (cut) => cut > 0), bytes.byteLength];
        const parts = Arr.map(ends, (end, index) =>
            bytes.subarray(index === 0 ? 0 : ends[index - 1], end),
        );
        return Arr.reduce(parts, started, (inner, part) => {
            const [next, bytes] = writeSseData(inner, part);
            pieces.push(bytes);
            return next;
        });
    });
    pieces.push(closeSseDataEvent(written));
    return Arr.join(
        Arr.map(pieces, (piece) => textDecoder.decode(piece, { stream: true })),
        '',
    );
};

const wholeEvent = (data: string) =>
    Sse.encoder.write(Sse.Event.make({ event: 'data', id: undefined, data }));

describe('a data event written in pieces', () => {
    it.prop(
        'text: the same as the whole payload through Sse.encoder',
        [Messages, CutPoints],
        ([messages, cutPoints]) => {
            expect(writtenInPieces({ encoding: 'text', messages, cutPoints })).toBe(
                wholeEvent(Arr.join(messages, '')),
            );
        },
    );

    it.prop(
        'JSON: the messages as one array, as Sse.encoder writes it',
        [Messages, CutPoints],
        ([messages, cutPoints]) => {
            expect(writtenInPieces({ encoding: 'json', messages, cutPoints })).toBe(
                wholeEvent(`[${Arr.join(messages, ',')}]`),
            );
        },
    );

    it.prop(
        'base64: one line, the base64 of all the bytes',
        [Messages, CutPoints],
        ([messages, cutPoints]) => {
            expect(writtenInPieces({ encoding: 'base64', messages, cutPoints })).toBe(
                wholeEvent(Base64.encode(utf8.encode(Arr.join(messages, '')))),
            );
        },
    );

    it('a CRLF cut between two parts is one line break', () => {
        expect(writtenInPieces({ encoding: 'text', messages: ['a\r\nb'], cutPoints: [2] })).toBe(
            'event: data\ndata: a\ndata: b\n\n',
        );
    });
});
