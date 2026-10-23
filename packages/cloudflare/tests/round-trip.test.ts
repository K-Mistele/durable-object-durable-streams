import { expect } from '@effect/vitest';
import * as Cloudflare from 'alchemy/Cloudflare';
import * as Test from 'alchemy/Test/Vitest';
import { Arbitrary, Array as Arr, Clock, Duration, Effect, Option, Random, Schema } from 'effect';
import { Base64 } from 'effect/encoding';
import * as HttpBody from 'effect/http/HttpBody';
import * as HttpClient from 'effect/http/HttpClient';
import Stack from '../alchemy.run';

// its own stage, so the test files can deploy and run at once without sharing local state
const { test, beforeAll, deploy } = Test.make({
    providers: Cloudflare.providers(),
    dev: true,
    stage: `${Test.defaultStage()}-round-trip`,
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

test(
    'PUT, POST and GET round trip through the stream object',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('round-trip');

        const created = yield* HttpClient.put(streamUrl, {
            body: HttpBody.text('hello ', 'text/plain'),
        });
        expect(created.status).toBe(201);
        expect(created.headers['stream-next-offset']).toBeDefined();

        const appended = yield* HttpClient.post(streamUrl, {
            body: HttpBody.text('world', 'text/plain'),
        });
        expect(appended.status).toBe(204);

        const read = yield* HttpClient.get(`${streamUrl}?offset=-1`);
        expect(read.status).toBe(200);
        expect(read.headers['content-type']).toBe('text/plain');
        expect(read.headers['stream-next-offset']).toBe(appended.headers['stream-next-offset']);
        expect(yield* read.text).toBe('hello world');
    }),
    { timeout: 60_000 },
);

test(
    'protocol errors from the stream object keep their status and headers',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('errors');

        const missing = yield* HttpClient.get(`${streamUrl}?offset=-1`);
        expect(missing.status).toBe(404);

        const created = yield* HttpClient.put(streamUrl, {
            headers: { 'stream-closed': 'true' },
            body: HttpBody.text('done', 'text/plain'),
        });
        expect(created.status).toBe(201);

        // StreamClosedError carries the tail, sent back as Stream-Next-Offset
        const appendAfterClose = yield* HttpClient.post(streamUrl, {
            body: HttpBody.text('more', 'text/plain'),
        });
        expect(appendAfterClose.status).toBe(409);
        expect(appendAfterClose.headers['stream-closed']).toBe('true');
        expect(appendAfterClose.headers['stream-next-offset']).toBe(
            created.headers['stream-next-offset'],
        );
    }),
    { timeout: 60_000 },
);

test(
    'a long-poll with nothing new times out after the test stage setting',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('long-poll');
        const created = yield* HttpClient.put(streamUrl, {
            body: HttpBody.text('first', 'text/plain'),
        });
        const tail = created.headers['stream-next-offset'];

        const [waited, poll] = yield* Effect.timed(
            HttpClient.get(`${streamUrl}?offset=${tail}&live=long-poll`),
        );
        const waitedMillis = Duration.toMillis(waited);

        // vite.config.ts sets DURABLE_STREAMS_LONG_POLL_TIMEOUT to 2s; the default is 30s
        expect(poll.status).toBe(204);
        expect(waitedMillis).toBeGreaterThanOrEqual(1_900);
        expect(waitedMillis).toBeLessThan(10_000);
    }),
    { timeout: 60_000 },
);

// a few small binary appends per run; every run is several HTTP calls, so keep runs low
const AppendBodies = Schema.Array(Schema.Uint8Array.check(Schema.isMinLength(1))).check(
    Schema.isMaxLength(6),
);

test(
    'a read returns every appended body, joined in order',
    Effect.gen(function* () {
        const result = yield* Arbitrary.checkEffect(
            Arbitrary.schema(AppendBodies),
            (bodies) =>
                Effect.gen(function* () {
                    const streamUrl = yield* streamUrlFor('appends');
                    const created = yield* HttpClient.put(streamUrl, {
                        body: HttpBody.uint8Array(new Uint8Array(), 'application/octet-stream'),
                    });
                    const appends = yield* Effect.forEach(bodies, (body) =>
                        HttpClient.post(streamUrl, {
                            body: HttpBody.uint8Array(body, 'application/octet-stream'),
                        }),
                    );
                    const read = yield* HttpClient.get(`${streamUrl}?offset=-1`);
                    const readBytes = new Uint8Array(yield* read.arrayBuffer);
                    const appendedBytes = new Uint8Array(
                        Arr.flatMap(bodies, (body) => Arr.fromIterable(body)),
                    );
                    const lastNextOffset = Arr.last(appends).pipe(
                        Option.map((append) => append.headers['stream-next-offset']),
                        Option.getOrElse(() => created.headers['stream-next-offset']),
                    );
                    return (
                        created.status === 201 &&
                        Arr.every(appends, (append) => append.status === 204) &&
                        read.status === 200 &&
                        Base64.encode(readBytes) === Base64.encode(appendedBytes) &&
                        read.headers['stream-next-offset'] === lastNextOffset
                    );
                }),
            { runs: 5 },
        );
        expect(Arbitrary.formatCheckFailure(result)).toBeUndefined();
    }),
    { timeout: 120_000 },
);

test(
    'a GET with an unknown live value is a catch-up read',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('unknown-live');
        yield* HttpClient.put(streamUrl, { body: HttpBody.text('hello', 'text/plain') });

        const withOffset = yield* HttpClient.get(`${streamUrl}?offset=-1&live=unknown`);
        const withoutOffset = yield* HttpClient.get(`${streamUrl}?live=unknown`);
        // long-poll and SSE still need an offset
        const longPollWithoutOffset = yield* HttpClient.get(`${streamUrl}?live=long-poll`);

        expect(withOffset.status).toBe(200);
        expect(yield* withOffset.text).toBe('hello');
        expect(withoutOffset.status).toBe(200);
        expect(yield* withoutOffset.text).toBe('hello');
        expect(longPollWithoutOffset.status).toBe(400);
    }),
    { timeout: 60_000 },
);

test(
    'a long-poll at offset -1 on an empty stream answers at once',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('long-poll-empty');
        yield* HttpClient.put(streamUrl, { body: HttpBody.text('', 'text/plain') });

        // the test Stack's long-poll timeout is 2s, so waiting would take that long
        const [elapsed, response] = yield* Effect.timed(
            HttpClient.get(`${streamUrl}?offset=-1&live=long-poll`),
        );

        expect(response.status).toBe(200);
        expect(yield* response.text).toBe('');
        expect(response.headers['stream-up-to-date']).toBe('true');
        expect(Duration.toMillis(elapsed)).toBeLessThan(1_000);
    }),
    { timeout: 60_000 },
);
