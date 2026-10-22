import { describe, expect, it } from '@effect/vitest';
import { DateTime, Effect, Schema } from 'effect';
import { TestClock } from 'effect/testing';
import { Cursor } from '../src/codec';
import { cursorFor } from '../src/cursor';

const cursorEpoch = DateTime.makeUnsafe('2024-10-09T00:00:00Z');
const intervalSeconds = 20;

// up to about ten years after the cursor epoch
const SecondsSinceCursorEpoch = Schema.Int.check(
    Schema.isBetween({ minimum: 0, maximum: 315_360_000 }),
);

describe('cursorFor', () => {
    it.effect.prop(
        'is never behind the current interval, and moves past a client cursor that is not behind',
        [SecondsSinceCursorEpoch, Schema.UndefinedOr(Cursor)],
        ([secondsSinceEpoch, clientCursor]) =>
            Effect.gen(function* () {
                yield* TestClock.setTime(
                    DateTime.toEpochMillis(
                        DateTime.add(cursorEpoch, { seconds: secondsSinceEpoch }),
                    ),
                );
                const currentInterval = Math.floor(secondsSinceEpoch / intervalSeconds);

                const sentCursor =
                    clientCursor === undefined
                        ? undefined
                        : yield* Schema.encodeEffect(Cursor)(clientCursor);
                const cursor = yield* cursorFor(sentCursor);

                expect(cursor).toBeGreaterThanOrEqual(currentInterval);
                if (clientCursor !== undefined && clientCursor >= currentInterval) {
                    expect(cursor).toBeGreaterThan(clientCursor);
                }
            }),
    );
});
