import { describe, expect, it } from '@effect/vitest';
import { Arbitrary, Array as Arr, Effect, Schema } from 'effect';
import { Offset, StreamSeq, StreamSeqOrder } from '../src/codec';

const encodeOffset = Schema.encodeEffect(Offset);

describe('Offset', () => {
    it.effect.prop('decodes to the offset it was encoded from', [Offset], ([offset]) =>
        Effect.gen(function* () {
            const wire = yield* encodeOffset(offset);
            expect(yield* Schema.decodeEffect(Offset)(wire)).toBe(offset);
        }),
    );

    // PROTOCOL.md: clients may compare offsets as strings
    it.effect.prop('wire offsets sort as strings in message order', [Offset, Offset], ([a, b]) =>
        Effect.gen(function* () {
            const wireA = yield* encodeOffset(a);
            const wireB = yield* encodeOffset(b);
            expect(wireA < wireB).toBe(a < b);
        }),
    );
});

// characters where UTF-16 code unit order and UTF-8 byte order disagree, plus plain ones
const StreamSeqText = Arbitrary.schema(
    Schema.NonEmptyArray(
        Schema.Literals(['a', 'z', '\u00e9', '\uff61', '\u{1F600}', '\u{10FFFF}']),
    ),
).pipe(Arbitrary.map((characters) => StreamSeq.make(Arr.join(characters, ''))));

describe('StreamSeqOrder', () => {
    it.prop(
        'orders Stream-Seq values by their UTF-8 bytes',
        [StreamSeqText, StreamSeqText],
        ([a, b]) => {
            expect(StreamSeqOrder(a, b)).toBe(Buffer.compare(Buffer.from(a), Buffer.from(b)));
        },
    );

    it('puts an emoji after U+FF61, as UTF-8 does and JS < does not', () => {
        const emoji = StreamSeq.make('\u{1F600}');
        const halfwidthStop = StreamSeq.make('\uff61');
        expect(emoji < halfwidthStop).toBe(true);
        expect(StreamSeqOrder(emoji, halfwidthStop)).toBe(1);
    });
});
