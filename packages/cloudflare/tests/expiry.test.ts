import { expect } from '@effect/vitest';
import * as Cloudflare from 'alchemy/Cloudflare';
import * as Test from 'alchemy/Test/Vitest';
import { Array as Arr, Clock, Effect, Fiber, Option, Random, Stream } from 'effect';
import * as HttpBody from 'effect/http/HttpBody';
import * as HttpClient from 'effect/http/HttpClient';
import Stack from '../alchemy.run';

// its own stage, so the test files can deploy and run at once without sharing local state
const { test, beforeAll, deploy } = Test.make({
    providers: Cloudflare.providers(),
    dev: true,
    stage: `${Test.defaultStage()}-expiry`,
});

const stack = beforeAll(deploy(Stack));

// a new path per call, since local Durable Object storage outlives the test
const streamUrlFor = (name: string) =>
    Effect.gen(function* () {
        const { url } = yield* stack;
        if (url === undefined) {
            return yield* Effect.die('The Worker has no URL: it needs workers.dev or a domain');
        }
        const millis = yield* Clock.currentTimeMillis;
        const suffix = yield* Random.nextIntBetween(0, 1_000_000);
        return `${url.replace(/\/+$/, '')}/${name}-${millis}-${suffix}`;
    });

const binary = 'application/octet-stream';

// TTLs are whole seconds, so these wait out real time; in their own file they run beside the rest
test(
    'a stream read before its TTL runs out outlives its first expiry time',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('sliding-ttl');
        const created = yield* HttpClient.put(streamUrl, {
            headers: { 'stream-ttl': '3' },
            body: HttpBody.uint8Array(new Uint8Array([1]), binary),
        });
        // a read each second for 5 seconds keeps sliding the 3-second TTL past the alarm; 3s, not
        // 2s, leaves room for a slow CI runner between reads
        const reads = yield* Effect.forEach(Arr.range(1, 5), () =>
            Effect.sleep('1 second').pipe(
                Effect.andThen(HttpClient.get(`${streamUrl}?offset=-1`)),
                Effect.map((response) => response.status),
            ),
        );
        // then left alone, it expires
        yield* Effect.sleep('4 seconds');
        const afterIdle = yield* HttpClient.head(streamUrl);

        expect(created.status).toBe(201);
        expect(reads).toEqual([200, 200, 200, 200, 200]);
        expect(afterIdle.status).toBe(404);
    }).pipe(Effect.scoped),
    { timeout: 30_000 },
);

test(
    'a live reader keeps a stream with a TTL from expiring while it waits',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('live-reader-ttl');
        const created = yield* HttpClient.put(streamUrl, {
            headers: { 'stream-ttl': '2' },
            body: HttpBody.uint8Array(new Uint8Array(0), 'text/plain'),
        });
        // SSE waits in 2s turns (the test's long-poll timeout), with no reads or writes between
        const events = yield* HttpClient.get(`${streamUrl}?offset=now&live=sse`).pipe(
            Effect.flatMap((response) =>
                response.stream.pipe(
                    Stream.decodeText,
                    Stream.scan(
                        () => '',
                        (text: string, piece: string) => text + piece,
                    ),
                    Stream.takeUntil((text) => text.includes('after-the-ttl')),
                    Stream.runLast,
                ),
            ),
            Effect.timeout('20 seconds'),
            Effect.forkScoped,
        );
        yield* Effect.sleep('5 seconds');
        const append = yield* HttpClient.post(streamUrl, {
            body: HttpBody.text('after-the-ttl', 'text/plain'),
        });
        const received = yield* Fiber.join(events);

        expect(created.status).toBe(201);
        expect(append.status).toBe(204);
        expect(Option.exists(received, (text) => text.includes('after-the-ttl'))).toBe(true);
    }).pipe(Effect.scoped),
    { timeout: 30_000 },
);
