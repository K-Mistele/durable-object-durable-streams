import { DateTime, Duration, Effect, Option, Random, Schema } from 'effect';
import { Cursor } from './codec';

// PROTOCOL.md:1370–1374: 20s intervals counted from 2024-10-09T00:00:00Z; jitter 1–3600s
const epoch = DateTime.makeUnsafe('2024-10-09T00:00:00Z');
const interval = Duration.seconds(20);

// clientCursor is the raw query value; an invalid one is ignored, not rejected
export const cursorFor = (clientCursor: string | undefined) =>
    Effect.gen(function* () {
        const now = yield* DateTime.now;
        const current = Cursor.make(
            Math.floor(
                Duration.toMillis(DateTime.distance(epoch, now)) / Duration.toMillis(interval),
            ),
        );
        const client = Schema.decodeUnknownOption(Cursor)(clientCursor);
        if (Option.isNone(client) || client.value < current) return current;
        const jitterSeconds = yield* Random.nextIntBetween(1, 3600);
        return Cursor.make(client.value + Math.ceil(jitterSeconds / Duration.toSeconds(interval)));
    });
