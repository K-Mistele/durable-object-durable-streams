import { expect } from '@effect/vitest';
import * as Cloudflare from 'alchemy/Cloudflare';
import * as Test from 'alchemy/Test/Vitest';
import { Clock, Duration, Effect } from 'effect';
import * as HttpBody from 'effect/http/HttpBody';
import * as HttpClient from 'effect/http/HttpClient';
import Stack from '../alchemy.run';

const { test, beforeAll, deploy } = Test.make({ providers: Cloudflare.providers(), dev: true });

const stack = beforeAll(deploy(Stack));

// a new path per run, since local Durable Object storage outlives the test
const streamUrlFor = (name: string) =>
    Effect.gen(function* () {
        const { url } = yield* stack;
        if (url === undefined) {
            return yield* Effect.die('The Worker has no URL: it needs workers.dev or a domain');
        }
        const runId = yield* Clock.currentTimeMillis;
        return `${url.replace(/\/+$/, '')}/${name}-${runId}`;
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
