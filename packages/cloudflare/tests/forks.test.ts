import { expect } from '@effect/vitest';
import * as Cloudflare from 'alchemy/Cloudflare';
import * as Test from 'alchemy/Test/Vitest';
import { Buffer } from 'node:buffer';
import { Array as Arr, Clock, Effect, Order, Random, Schedule, Stream } from 'effect';
import { joinBytes } from '@kylelayer/effect-durable-streams-protocol/bytes';
import * as HttpBody from 'effect/http/HttpBody';
import * as HttpClient from 'effect/http/HttpClient';
import Stack from '../alchemy.run';

// its own stage, so its local state is its own
const { test, beforeAll, deploy } = Test.make({
    providers: Cloudflare.providers(),
    dev: true,
    stage: `${Test.defaultStage()}-forks`,
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

const createStream = (streamUrl: string) =>
    HttpClient.put(streamUrl, { body: HttpBody.uint8Array(new Uint8Array(0), binary) }).pipe(
        Effect.map((response) => response.status),
    );

const appendBytes = (input: { readonly streamUrl: string; readonly bytes: Uint8Array }) =>
    HttpClient.post(input.streamUrl, { body: HttpBody.uint8Array(input.bytes, binary) }).pipe(
        Effect.map((response) => response.status),
    );

/** PUT with Stream-Forked-From; the source is named by its path. */
const createFork = (input: {
    readonly forkUrl: string;
    readonly sourceUrl: string;
    readonly headers?: Record<string, string>;
}) =>
    HttpClient.put(input.forkUrl, {
        headers: { 'stream-forked-from': new URL(input.sourceUrl).pathname, ...input.headers },
        body: HttpBody.uint8Array(new Uint8Array(0), binary),
    });

const readAll = (streamUrl: string) =>
    HttpClient.get(`${streamUrl}?offset=-1`).pipe(
        Effect.flatMap((response) =>
            Stream.runCollect(response.stream).pipe(
                Effect.map((chunks) => ({
                    status: response.status,
                    nextOffset: response.headers['stream-next-offset'],
                    bytes: joinBytes(chunks),
                })),
            ),
        ),
    );

/** `length` bytes, each its index mod 251, so a byte out of place shows. */
const patterned = (length: number) => Uint8Array.from({ length }, (_, index) => index % 251);

const megabyte = 1024 * 1024;

test(
    'a fork copies a large source across many pages and parts',
    Effect.gen(function* () {
        const sourceUrl = yield* streamUrlFor('fork-large-source');
        const forkUrl = yield* streamUrlFor('fork-large');
        yield* createStream(sourceUrl);
        // 30 messages of 1.2MB (2 parts each; about 9 read pages), a 2.5MB message (3 parts),
        // then 1 byte
        const messages = [
            ...Arr.map(Arr.range(0, 29), (index) => new Uint8Array(1.2 * megabyte).fill(index)),
            patterned(2.5 * megabyte),
            new Uint8Array([7]),
        ];
        const appendStatuses = yield* Effect.forEach(messages, (bytes) =>
            appendBytes({ streamUrl: sourceUrl, bytes }),
        );

        const fork = yield* createFork({ forkUrl, sourceUrl });
        const source = yield* readAll(sourceUrl);
        const forked = yield* readAll(forkUrl);

        // the fork is its own stream: an append to it leaves the source as it was
        const forkAppend = yield* appendBytes({ streamUrl: forkUrl, bytes: new Uint8Array([9]) });
        const sourceAfter = yield* readAll(sourceUrl);
        const forkAfter = yield* readAll(forkUrl);

        expect(Arr.every(appendStatuses, (status) => status === 204)).toBe(true);
        expect(fork.status).toBe(201);
        expect(fork.headers['stream-next-offset']).toBe(source.nextOffset);
        expect(forked.status).toBe(200);
        expect(Buffer.compare(forked.bytes, joinBytes(messages))).toBe(0);
        expect(forkAppend).toBe(204);
        expect(Buffer.compare(sourceAfter.bytes, joinBytes(messages))).toBe(0);
        expect(Buffer.compare(forkAfter.bytes, joinBytes([...messages, new Uint8Array([9])]))).toBe(
            0,
        );
    }).pipe(Effect.scoped),
    { timeout: 120_000 },
);

test(
    'a binary sub-offset cuts a message stored in several parts, and a fork of the fork cuts it again',
    Effect.gen(function* () {
        const sourceUrl = yield* streamUrlFor('fork-sub-offset-source');
        const forkUrl = yield* streamUrlFor('fork-sub-offset');
        const secondForkUrl = yield* streamUrlFor('fork-sub-offset-second');
        yield* createStream(sourceUrl);
        const first = patterned(10);
        const large = patterned(2.5 * megabyte);
        yield* appendBytes({ streamUrl: sourceUrl, bytes: first });
        const afterFirst = yield* HttpClient.head(sourceUrl).pipe(
            Effect.map((response) => response.headers['stream-next-offset'] ?? ''),
        );
        yield* appendBytes({ streamUrl: sourceUrl, bytes: large });

        // into the second part of the large message, then into its first
        const keep = 1.5 * megabyte + 7;
        const fork = yield* createFork({
            forkUrl,
            sourceUrl,
            headers: { 'stream-fork-offset': afterFirst, 'stream-fork-sub-offset': String(keep) },
        });
        const forked = yield* readAll(forkUrl);
        const secondFork = yield* createFork({
            forkUrl: secondForkUrl,
            sourceUrl: forkUrl,
            headers: { 'stream-fork-offset': afterFirst, 'stream-fork-sub-offset': '5' },
        });
        const secondForked = yield* readAll(secondForkUrl);

        expect(fork.status).toBe(201);
        expect(Buffer.compare(forked.bytes, joinBytes([first, large.subarray(0, keep)]))).toBe(0);
        expect(secondFork.status).toBe(201);
        expect(Buffer.compare(secondForked.bytes, joinBytes([first, large.subarray(0, 5)]))).toBe(
            0,
        );
    }).pipe(Effect.scoped),
    { timeout: 60_000 },
);

test(
    'a repeated fork PUT during the copy gets 200 once the fork is readable',
    Effect.gen(function* () {
        const sourceUrl = yield* streamUrlFor('fork-repeat-source');
        const forkUrl = yield* streamUrlFor('fork-repeat');
        yield* createStream(sourceUrl);
        // 40MB in 10 messages of 4 parts each
        const messages = Arr.map(Arr.range(0, 9), (index) =>
            new Uint8Array(4 * megabyte).fill(index),
        );
        yield* Effect.forEach(messages, (bytes) => appendBytes({ streamUrl: sourceUrl, bytes }));

        // each PUT reads the fork as soon as it is answered
        const putThenRead = createFork({ forkUrl, sourceUrl }).pipe(
            Effect.flatMap((response) =>
                readAll(forkUrl).pipe(Effect.map((read) => ({ status: response.status, read }))),
            ),
        );
        const results = yield* Effect.all([putThenRead, putThenRead], {
            concurrency: 'unbounded',
        });

        expect(
            Arr.sort(
                Arr.map(results, (result) => result.status),
                Order.Number,
            ),
        ).toEqual([200, 201]);
        expect(
            Arr.every(
                results,
                ({ read }) =>
                    read.status === 200 && Buffer.compare(read.bytes, joinBytes(messages)) === 0,
            ),
        ).toBe(true);
    }).pipe(Effect.scoped),
    { timeout: 120_000 },
);

const headStatus = (streamUrl: string) =>
    HttpClient.head(streamUrl).pipe(Effect.map((response) => response.status));

test(
    'an expired fork is purged by its alarm and releases its deleted source',
    Effect.gen(function* () {
        const sourceUrl = yield* streamUrlFor('expiring-fork-source');
        const forkUrl = yield* streamUrlFor('expiring-fork');
        yield* createStream(sourceUrl);
        yield* appendBytes({ streamUrl: sourceUrl, bytes: new Uint8Array([1]) });
        const fork = yield* createFork({ forkUrl, sourceUrl, headers: { 'stream-ttl': '1' } });
        yield* HttpClient.del(sourceUrl);
        const sourceWhileForked = yield* headStatus(sourceUrl);

        // no request reaches the fork: only its alarm can purge it and release the source
        const sourceAfter = yield* headStatus(sourceUrl).pipe(
            Effect.repeat({
                schedule: Schedule.spaced('250 millis'),
                until: (status) => status === 404,
            }),
            Effect.timeout('10 seconds'),
        );

        expect(fork.status).toBe(201);
        expect(sourceWhileForked).toBe(410);
        expect(sourceAfter).toBe(404);
    }).pipe(Effect.scoped),
    { timeout: 30_000 },
);
