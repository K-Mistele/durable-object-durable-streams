import { expect } from '@effect/vitest';
import * as Cloudflare from 'alchemy/Cloudflare';
import * as Test from 'alchemy/Test/Vitest';
import { Buffer } from 'node:buffer';
import {
    Array as Arr,
    Clock,
    Duration,
    Effect,
    Fiber,
    Option,
    Random,
    Schema,
    Stream,
} from 'effect';
import { Offset } from '@kylelayer/effect-durable-streams-protocol/codec';
import { SseControl } from '@kylelayer/effect-durable-streams-protocol/sse';
import * as HttpBody from 'effect/http/HttpBody';
import * as HttpClient from 'effect/http/HttpClient';
import Stack from '../alchemy.run';

// its own stage, so its local state is its own
const { test, beforeAll, deploy } = Test.make({
    providers: Cloudflare.providers(),
    dev: true,
    stage: `${Test.defaultStage()}-reads`,
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

const createStream = (input: { readonly streamUrl: string; readonly contentType: string }) =>
    HttpClient.put(input.streamUrl, {
        body: HttpBody.uint8Array(new Uint8Array(0), input.contentType),
    }).pipe(Effect.map((response) => response.status));

const appendBytes = (input: { readonly streamUrl: string; readonly bytes: Uint8Array }) =>
    HttpClient.post(input.streamUrl, {
        body: HttpBody.uint8Array(input.bytes, 'application/octet-stream'),
    }).pipe(Effect.map((response) => response.status));

const joinBytes = (chunks: ReadonlyArray<Uint8Array>) => {
    const joined = new Uint8Array(
        Arr.reduce(chunks, 0, (total, chunk) => total + chunk.byteLength),
    );
    Arr.reduce(chunks, 0, (at, chunk) => {
        joined.set(chunk, at);
        return at + chunk.byteLength;
    });
    return joined;
};

// 60 messages of 1MB, each filled with its index: 15 pages of 4MB, in few POSTs
const messageBytes = 1024 * 1024;
const messageCount = 60;
const messageAt = (index: number) => new Uint8Array(messageBytes).fill(index % 256);

test(
    'reads the client stops pulling block no write and no other read, then finish in order',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('stalled-reads');
        yield* createStream({ streamUrl, contentType: 'application/octet-stream' });
        // one at a time, so message i lands at offset i
        const fillStatuses = yield* Effect.forEach(Arr.range(0, messageCount - 1), (index) =>
            appendBytes({ streamUrl, bytes: messageAt(index) }),
        );
        const allMessages = joinBytes(Arr.map(Arr.range(0, messageCount - 1), messageAt));

        // five full reads whose clients take the first chunk, then stop pulling
        const stalledReads = yield* Effect.forEach(Arr.range(1, 5), () =>
            Effect.gen(function* () {
                const response = yield* HttpClient.get(`${streamUrl}?offset=-1`);
                const pull = yield* Stream.toPull(response.stream);
                const first = yield* pull;
                return { pull, first };
            }),
        );
        yield* Effect.sleep('1 second');

        const [appendTime, appendStatus] = yield* Effect.timed(
            appendBytes({ streamUrl, bytes: new Uint8Array([1]) }),
        );
        const [headTime, head] = yield* Effect.timed(HttpClient.head(streamUrl));
        const [otherReadTime, otherRead] = yield* Effect.timed(
            HttpClient.get(`${streamUrl}?offset=-1`).pipe(
                Effect.flatMap((response) => Stream.runCollect(response.stream)),
                Effect.map(joinBytes),
            ),
        );

        // each stalled read still returns the stream as it was when it started
        const finished = yield* Effect.forEach(
            stalledReads,
            ({ pull, first }) =>
                Stream.runCollect(Stream.fromPull(Effect.succeed(pull))).pipe(
                    Effect.map((rest) => joinBytes([...first, ...rest])),
                ),
            { concurrency: 'unbounded' },
        );

        expect(Arr.every(fillStatuses, (status) => status === 204)).toBe(true);
        expect(appendStatus).toBe(204);
        expect(Duration.toMillis(appendTime)).toBeLessThan(1_000);
        expect(head.status).toBe(200);
        expect(Duration.toMillis(headTime)).toBeLessThan(1_000);
        expect(otherRead.byteLength).toBe(messageCount * messageBytes + 1);
        expect(Duration.toMillis(otherReadTime)).toBeLessThan(5_000);
        expect(Arr.every(finished, (bytes) => Buffer.compare(bytes, allMessages) === 0)).toBe(true);
    }).pipe(Effect.scoped),
    { timeout: 120_000 },
);

test(
    'open SSE connections block no write, and each gets the new message',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('sse-connections');
        yield* createStream({ streamUrl, contentType: 'text/plain' });

        const connections = yield* Effect.forEach(Arr.range(1, 5), () =>
            HttpClient.get(`${streamUrl}?offset=now&live=sse`),
        );
        const received = yield* Effect.forEach(connections, (connection) =>
            connection.stream.pipe(
                Stream.decodeText,
                Stream.scan(
                    () => '',
                    (text, chunk) => text + chunk,
                ),
                Stream.takeUntil((text) => text.includes('hello sse')),
                Stream.runLast,
                Effect.forkScoped,
            ),
        );
        yield* Effect.sleep('500 millis');

        const [appendTime, appended] = yield* Effect.timed(
            HttpClient.post(streamUrl, { body: HttpBody.text('hello sse', 'text/plain') }),
        );
        const texts = yield* Effect.forEach(received, Fiber.join).pipe(Effect.timeout('5 seconds'));

        expect(appended.status).toBe(204);
        expect(Duration.toMillis(appendTime)).toBeLessThan(1_000);
        expect(texts).toHaveLength(5);
        expect(
            Arr.every(texts, (text) => text.pipe(Option.exists((t) => t.includes('hello sse')))),
        ).toBe(true);
    }).pipe(Effect.scoped),
    { timeout: 60_000 },
);

test(
    'a read of more rows than one page returns every JSON message in order',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('many-rows');
        yield* createStream({ streamUrl, contentType: 'application/json' });
        // 10,000 one-row messages: three pages of at most 4096 rows
        const items = Arr.range(0, 9_999);
        const appended = yield* HttpClient.post(streamUrl, {
            body: HttpBody.jsonUnsafe(items),
        });
        const read = yield* HttpClient.get(`${streamUrl}?offset=-1`);

        expect(appended.status).toBe(204);
        expect(yield* read.json).toEqual(items);
    }),
    { timeout: 60_000 },
);

test(
    'a read whose pages end inside a message returns each message whole',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('split-messages');
        yield* createStream({ streamUrl, contentType: 'application/json' });
        // 1.5MB items are stored as a 1MB part and a 0.5MB part, so 4MB pages end mid-message
        const items = Arr.map(Arr.range(0, 5), (index) => String(index).repeat(1_500_000));
        const appended = yield* HttpClient.post(streamUrl, {
            body: HttpBody.jsonUnsafe(items),
        });
        const read = yield* HttpClient.get(`${streamUrl}?offset=-1`);

        expect(appended.status).toBe(204);
        expect(yield* read.json).toEqual(items);
    }),
    { timeout: 60_000 },
);

const closeStream = (streamUrl: string) =>
    HttpClient.post(streamUrl, { headers: { 'stream-closed': 'true' } }).pipe(
        Effect.map((response) => response.status),
    );

/** A closed stream's whole SSE response, as its events: `data` is the event's data lines joined by LF. */
const readSseEvents = (streamUrl: string) =>
    HttpClient.get(`${streamUrl}?offset=-1&live=sse`).pipe(
        Effect.flatMap((response) => response.text),
        Effect.map((text) =>
            Arr.map(
                Arr.filter(text.split('\n\n'), (block) => block.length > 0),
                (block) => {
                    const lines = block.split('\n');
                    const dataLines = Arr.filter(lines, (line) => line.startsWith('data:'));
                    return {
                        event: lines[0]?.replace(/^event: ?/, ''),
                        data: Arr.join(
                            Arr.map(dataLines, (line) => line.replace(/^data: ?/, '')),
                            '\n',
                        ),
                    };
                },
            ),
        ),
    );

const decodeControl = Schema.decodeUnknownEffect(Schema.fromJsonString(SseControl));
const decodeJsonItems = Schema.decodeUnknownEffect(
    Schema.fromJsonString(Schema.Array(Schema.String)),
);

test(
    'SSE sends a message larger than one event whole, in one data event',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('sse-large-message');
        yield* createStream({ streamUrl, contentType: 'application/octet-stream' });
        // 10MB: ten 1MB parts, more than one event's 4MB
        const message = Uint8Array.from({ length: 10 * 1024 * 1024 }, (_, i) => i % 251);
        const appended = yield* appendBytes({ streamUrl, bytes: message });
        const closed = yield* closeStream(streamUrl);

        const events = yield* readSseEvents(streamUrl);

        expect(appended).toBe(204);
        expect(closed).toBe(204);
        expect(Arr.map(events, (event) => event.event)).toEqual(['data', 'control']);
        expect(Buffer.compare(Buffer.from(events[0]?.data ?? '', 'base64'), message)).toBe(0);
        const control = yield* decodeControl(events[1]?.data);
        expect(control.streamClosed).toBe(true);
    }),
    { timeout: 60_000 },
);

test(
    'SSE sends many small messages as several data events, each followed by its control event',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('sse-small-messages');
        yield* createStream({ streamUrl, contentType: 'application/json' });
        // 6,000 messages of about 1KB: two events of about 4MB and 2MB
        const items = Arr.map(Arr.range(0, 5_999), (index) => `${index}`.padEnd(1_000, '.'));
        const statuses = yield* Effect.forEach(Arr.chunksOf(items, 2_000), (batch) =>
            HttpClient.post(streamUrl, { body: HttpBody.jsonUnsafe(batch) }).pipe(
                Effect.map((response) => response.status),
            ),
        );
        yield* closeStream(streamUrl);

        const events = yield* readSseEvents(streamUrl);
        const dataEvents = Arr.filter(events, (event) => event.event === 'data');
        const controls = yield* Effect.forEach(
            Arr.filter(events, (event) => event.event === 'control'),
            (event) => decodeControl(event.data),
        );
        const received = yield* Effect.forEach(dataEvents, (event) => decodeJsonItems(event.data));

        expect(Arr.every(statuses, (status) => status === 204)).toBe(true);
        expect(dataEvents.length).toBeGreaterThan(1);
        // data, control, data, control, …
        expect(Arr.map(events, (event) => event.event)).toEqual(
            Arr.flatMap(dataEvents, () => ['data', 'control']),
        );
        expect(Arr.flatten(received)).toEqual(items);
        // each control points just past its event's messages; only the last says closed
        const ends = Arr.scan(received, 0, (end, eventItems) => end + eventItems.length).slice(1);
        expect(Arr.map(controls, (control) => control.streamNextOffset)).toEqual(
            Arr.map(ends, (end) => Offset.make(end)),
        );
        expect(Arr.map(controls, (control) => control.streamClosed === true)).toEqual(
            Arr.map(ends, (_, index) => index === ends.length - 1),
        );
    }),
    { timeout: 120_000 },
);

test(
    'SSE turns a CRLF cut between two stored parts into one line break',
    Effect.gen(function* () {
        const streamUrl = yield* streamUrlFor('sse-crlf-parts');
        yield* createStream({ streamUrl, contentType: 'text/plain' });
        // the first 1MB part ends with CR, the second starts with LF
        const text = `${'a'.repeat(1024 * 1024 - 1)}\r\nb`;
        yield* HttpClient.post(streamUrl, { body: HttpBody.text(text, 'text/plain') });
        yield* closeStream(streamUrl);

        const events = yield* readSseEvents(streamUrl);

        expect(Arr.map(events, (event) => event.event)).toEqual(['data', 'control']);
        expect(events[0]?.data).toBe(`${'a'.repeat(1024 * 1024 - 1)}\nb`);
    }),
    { timeout: 60_000 },
);
