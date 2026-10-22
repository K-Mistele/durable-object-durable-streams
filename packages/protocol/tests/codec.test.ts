import { describe, expect, it } from '@effect/vitest';
import { Effect, Schema } from 'effect';
import { Offset } from '../src/codec';

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
