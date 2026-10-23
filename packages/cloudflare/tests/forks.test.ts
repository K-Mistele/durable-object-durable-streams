import { expect } from '@effect/vitest';
import * as Cloudflare from 'alchemy/Cloudflare';
import * as Test from 'alchemy/Test/Vitest';
import { Buffer } from 'node:buffer';
import {
    Array as Arr,
    Clock,
    Effect,
    Fiber,
    Option,
    Order,
    Random,
    Schedule,
    Stream,
} from 'effect';
import { joinBytes } from '@kylelayer/effect-durable-streams-protocol/bytes';
import * as HttpBody from 'effect/http/HttpBody';
import * as HttpClient from 'effect/http/HttpClient';
import Stack from '../alchemy.run';

const { test, beforeAll, deploy } = Test.make({ providers: Cloudflare.providers(), dev: true });

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
        // 120 messages of 300KB (9 read pages), a 2.5MB message (3 parts), then 1 byte
        const messages = [
            ...Arr.map(Arr.range(0, 119), (index) => new Uint8Array(300 * 1024).fill(index)),
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
        const messages = Arr.map(Arr.range(0, 39), (index) => new Uint8Array(megabyte).fill(index));
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

test(
    'a stream read before its TTL runs out outlives its first expiry time',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('sliding-ttl');
        const created = yield* HttpClient.put(streamUrl, {
            headers: { 'stream-ttl': '2' },
            body: HttpBody.uint8Array(new Uint8Array([1]), binary),
        });
        // a read each second for 4 seconds keeps sliding the 2-second TTL past the alarm
        const reads = yield* Effect.forEach(Arr.range(1, 4), () =>
            Effect.sleep('1 second').pipe(
                Effect.andThen(readAll(streamUrl)),
                Effect.map((read) => read.status),
            ),
        );
        // then left alone, it expires
        yield* Effect.sleep('3 seconds');
        const afterIdle = yield* headStatus(streamUrl);

        expect(created.status).toBe(201);
        expect(reads).toEqual([200, 200, 200, 200]);
        expect(afterIdle).toBe(404);
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
