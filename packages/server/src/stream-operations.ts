import { DateTime, Effect, Match, Option, Random, Stream, SubscriptionRef } from 'effect';
import {
    mediaTypeOf,
    type Offset,
    type ProducerSeq,
    type StreamPath,
} from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    type BadRequestError,
    StreamClosedError,
    type StreamConfigConflictError,
    type StreamGoneError,
    StreamNotFoundError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import { joinBytes } from '@kylelayer/effect-durable-streams-protocol/bytes';
import { jsonMessages } from '@kylelayer/effect-durable-streams-protocol/json';
import { zero } from '@kylelayer/effect-durable-streams-protocol/offset';
import type { ProducerState } from '@kylelayer/effect-durable-streams-protocol/producer';
import {
    closeSseDataEvent,
    openSseDataEvent,
    type SseControl,
    type SseDataEvent,
    sseControlEvent,
    sseDataEncodingFor,
    startSseDataMessage,
    writeSseData,
} from '@kylelayer/effect-durable-streams-protocol/sse';
import type { StoredChunk, StoredStream } from './stored-stream';
import {
    type AppendRequest,
    type AppendResult,
    type CreateStreamRequest,
    type CreateStreamResult,
    type EmptyRequest,
    type ReadBodyRequest,
    type ReadRange,
    type ReadSseEventsRequest,
    type StartReadRequest,
    type StreamMetadata,
    type WaitForNewMessagesRequest,
    WaitResult,
} from './stream-operation-requests';
import {
    type AppendRejection,
    buildNewStream,
    getExistingStream,
    isExpired,
    isRepeatedClose,
    rejectConfigConflict,
    rejectContentTypeMismatch,
    rejectOutOfSequenceProducerRequest,
    rejectStreamSeqRegression,
    type StoredAppendState,
    type StreamConfig,
    updateStreamForAppend,
} from './stream-rules';
import { type StreamChanges, StreamStorage } from './stream-storage';

export type CreateStreamError = BadRequestError | StreamConfigConflictError;
export type AppendToStreamError = BadRequestError | AppendRejection;
export type MissingStreamError = StreamNotFoundError | StreamGoneError;

/**
 * The operations on one stream, bound to its path and its waiters. Every request for the stream
 * goes through them, inside the stream's Durable Object, so its writes happen in one place and its
 * long-poll waiters live next to them. A plain object, not a service: the Durable Object builds
 * one per instance with `makeStreamOperations`, and alchemy exposes each function as one of its
 * RPC methods.
 */
export type StreamOperations = {
    readonly createStream: (
        request: CreateStreamRequest,
    ) => Effect.Effect<CreateStreamResult, CreateStreamError>;
    readonly appendToStream: (
        request: AppendRequest,
    ) => Effect.Effect<AppendResult, AppendToStreamError>;
    readonly getMetadata: (
        request: EmptyRequest,
    ) => Effect.Effect<StreamMetadata, MissingStreamError>;
    readonly startRead: (request: StartReadRequest) => Effect.Effect<ReadRange, MissingStreamError>;
    /** The response body: JSON as one array, other content types as stored bytes. */
    readonly readBody: (request: ReadBodyRequest) => Stream.Stream<Uint8Array, StreamNotFoundError>;
    /**
     * SSE: the range as data events of about `sseEventBytes` each, every one ending at a message
     * boundary and followed by its control event (engine-design.md, SSE).
     */
    readonly readSseEvents: (
        request: ReadSseEventsRequest,
    ) => Stream.Stream<Uint8Array, StreamNotFoundError>;
    readonly waitForNewMessages: (
        request: WaitForNewMessagesRequest,
    ) => Effect.Effect<WaitResult, MissingStreamError>;
    readonly deleteStream: (request: EmptyRequest) => Effect.Effect<void, MissingStreamError>;
};

/** What waiters watch. `generation` is None while no stream exists. */
type WaiterState = {
    readonly generation: Option.Option<number>;
    readonly tail: Offset;
    readonly closed: boolean;
};

type AppendChanges = {
    readonly changes: StreamChanges;
    readonly result: { readonly stream: StoredStream; readonly appendResult: AppendResult };
};

const waiterStateOf = (stream: Option.Option<StoredStream>): WaiterState =>
    Option.match(stream, {
        onNone: () => ({ generation: Option.none(), tail: zero, closed: false }),
        onSome: (existing) => ({
            generation: Option.some(existing.generation),
            tail: existing.tail,
            closed: existing.closed,
        }),
    });

const toStreamMetadata = (stream: StoredStream): StreamMetadata => ({
    contentType: stream.contentType,
    tail: stream.tail,
    closed: stream.closed,
    ttlSeconds: stream.ttlSeconds,
    expiresAt: stream.expiresAt,
});

const isJsonContentType = (contentType: string) => mediaTypeOf(contentType) === 'application/json';

/** A JSON body holds one message per array item; any other body is one message. */
const splitBodyIntoMessages = ({
    contentType,
    body,
    allowEmptyArray,
}: {
    readonly contentType: string;
    readonly body: Uint8Array;
    readonly allowEmptyArray: boolean;
}): Effect.Effect<ReadonlyArray<Uint8Array>, BadRequestError> => {
    if (body.length === 0) {
        return Effect.succeed([]);
    }
    if (!isJsonContentType(contentType)) {
        return Effect.succeed([body]);
    }
    return Stream.runCollect(jsonMessages(Stream.make(body), { allowEmpty: allowEmptyArray }));
};

/** A repeat of a request already applied: save only the access time, answer with the current tail. */
const acknowledgeDuplicateAppend = ({
    stream,
    acceptedProducerSeq,
    now,
}: {
    readonly stream: StoredStream;
    readonly acceptedProducerSeq: Option.Option<ProducerSeq>;
    readonly now: DateTime.Utc;
}): AppendChanges => ({
    changes: {
        firstMessageOffset: stream.tail,
        storedStream: { ...stream, lastAccessedAt: now },
        producer: Option.none(),
        messages: [],
    },
    result: {
        stream,
        appendResult: {
            nextOffset: stream.tail,
            closed: stream.closed,
            wasDuplicate: true,
            acceptedProducerSeq,
        },
    },
});

/**
 * PUT, inside the create transaction. An existing stream with the same config is left as it is
 * (200); with another config the PUT fails with 409. Otherwise the body becomes the new stream's
 * first messages (201).
 */
export const computeCreateStreamChanges = ({
    stored,
    request,
    now,
}: {
    readonly stored: Option.Option<StoredStream>;
    readonly request: CreateStreamRequest;
    readonly now: DateTime.Utc;
}) =>
    Effect.gen(function* () {
        const config: StreamConfig = {
            contentType: request.contentType,
            ttlSeconds: request.ttlSeconds,
            expiresAt: request.expiresAt,
            closed: request.closed,
        };
        const existing = Option.filter(stored, (stream) => !isExpired({ stream, now }));
        if (Option.isSome(existing)) {
            yield* rejectConfigConflict({ existing: existing.value, requested: config });
            return {
                changes: Option.none<StreamChanges>(),
                result: { isNewStream: false, stream: existing.value },
            };
        }
        const messages = yield* splitBodyIntoMessages({
            contentType: config.contentType,
            body: request.body,
            allowEmptyArray: true,
        });
        const generation = yield* Random.nextIntBetween(1, 2 ** 31);
        const created = buildNewStream({ config, generation, messageCount: messages.length, now });
        return {
            changes: Option.some<StreamChanges>({
                firstMessageOffset: zero,
                storedStream: created,
                producer: Option.none(),
                messages,
            }),
            result: { isNewStream: true, stream: created },
        };
    });

/** POST, inside the append transaction: the protocol's checks in PROTOCOL.md 5.2 order, then the messages to save. */
export const computeAppendChanges = ({
    stored,
    request,
    now,
}: {
    readonly stored: StoredAppendState;
    readonly request: AppendRequest;
    readonly now: DateTime.Utc;
}): Effect.Effect<AppendChanges, AppendToStreamError> =>
    Effect.gen(function* () {
        const stream = yield* getExistingStream({ stored: stored.stream, now });

        if (stream.closed) {
            if (isRepeatedClose({ stream, request })) {
                const acceptedProducerSeq = Option.map(
                    request.producer,
                    (producer) => producer.seq,
                );
                return acknowledgeDuplicateAppend({ stream, acceptedProducerSeq, now });
            }
            return yield* StreamClosedError.make({ nextOffset: stream.tail });
        }

        yield* Match.valueTags(request, {
            AppendMessages: ({ contentType }) => rejectContentTypeMismatch({ stream, contentType }),
            CloseStream: () => Effect.void,
        });

        const appendMessages = (producerState: Option.Option<ProducerState>) =>
            Effect.gen(function* () {
                const messages = yield* Match.valueTags(request, {
                    AppendMessages: (append) =>
                        rejectStreamSeqRegression({ stream, streamSeq: append.streamSeq }).pipe(
                            Effect.andThen(
                                splitBodyIntoMessages({
                                    contentType: stream.contentType,
                                    body: append.body,
                                    allowEmptyArray: false,
                                }),
                            ),
                        ),
                    CloseStream: () => Effect.succeed<ReadonlyArray<Uint8Array>>([]),
                });
                const updated = updateStreamForAppend({
                    stream,
                    request,
                    messageCount: messages.length,
                    now,
                });
                const changes: StreamChanges = {
                    firstMessageOffset: stream.tail,
                    storedStream: updated,
                    producer: Option.zipWith(
                        request.producer,
                        producerState,
                        (producer, state) => ({
                            id: producer.id,
                            state,
                        }),
                    ),
                    messages,
                };
                const appendResult: AppendResult = {
                    nextOffset: updated.tail,
                    closed: updated.closed,
                    wasDuplicate: false,
                    acceptedProducerSeq: Option.map(request.producer, (producer) => producer.seq),
                };
                return { changes, result: { stream: updated, appendResult } };
            });

        const position = yield* rejectOutOfSequenceProducerRequest({
            storedProducer: stored.producer,
            producer: request.producer,
        });
        return yield* Match.valueTags(position, {
            AlreadyAccepted: ({ lastSeq }) =>
                Effect.succeed(
                    acknowledgeDuplicateAppend({
                        stream,
                        acceptedProducerSeq: Option.some(lastSeq),
                        now,
                    }),
                ),
            NoProducer: () => appendMessages(Option.none()),
            NextInSequence: ({ state }) => appendMessages(Option.some(state)),
        });
    });

const textEncoder = new TextEncoder();
const openBracket = textEncoder.encode('[');
const comma = textEncoder.encode(',');
const closeBracket = textEncoder.encode(']');

/** A data event ends at the first message boundary once it holds this much. */
const sseEventBytes = 4 * 1024 * 1024;

/** `readSseEvents` between rows: the data event being written, if any, and its size so far. */
type SseEventsState = {
    readonly event: Option.Option<SseDataEvent>;
    readonly eventBytes: number;
};

/**
 * What one row adds to the SSE text: bytes, and control events still to be encoded. One row's
 * output goes out as one chunk, so a data event's end and its control event stay together.
 */
type SseOutput = ReadonlyArray<Uint8Array | SseControl>;

/** Builds the operations of the stream at `stream`, loading its state for its waiters. */
export const makeStreamOperations = Effect.fn('makeStreamOperations')(function* (input: {
    readonly stream: StreamPath;
}) {
    const { stream } = input;
    const storage = yield* StreamStorage;
    const openedAt = yield* DateTime.now;
    const storedAtOpen = yield* storage.loadStream({ stream });
    const waiterState = yield* SubscriptionRef.make(
        waiterStateOf(
            Option.filter(
                storedAtOpen,
                (existing) => !isExpired({ stream: existing, now: openedAt }),
            ),
        ),
    );

    const loadExistingStream = Effect.gen(function* () {
        const stored = yield* storage.loadStream({ stream });
        return yield* getExistingStream({ stored, now: yield* DateTime.now });
    });

    // a read fails before sending anything if the stream was deleted or re-created since startRead
    const loadSameGeneration = (generation: number) =>
        Effect.gen(function* () {
            const stored = yield* storage.loadStream({ stream });
            const sameGeneration = Option.filter(
                stored,
                (candidate) => candidate.generation === generation,
            );
            if (Option.isNone(sameGeneration)) {
                return yield* StreamNotFoundError.make({});
            }
            return sameGeneration.value;
        });

    const operations: StreamOperations = {
        createStream: (request) =>
            Effect.gen(function* () {
                const now = yield* DateTime.now;
                const outcome = yield* storage.createStream({
                    stream,
                    computeChanges: (stored) =>
                        computeCreateStreamChanges({ stored, request, now }),
                });
                if (outcome.isNewStream) {
                    yield* SubscriptionRef.set(
                        waiterState,
                        waiterStateOf(Option.some(outcome.stream)),
                    );
                }
                return {
                    isNewStream: outcome.isNewStream,
                    metadata: toStreamMetadata(outcome.stream),
                };
            }),

        appendToStream: (request) =>
            Effect.gen(function* () {
                const now = yield* DateTime.now;
                const outcome = yield* storage.appendToStream({
                    stream,
                    producerId: Option.map(request.producer, (producer) => producer.id),
                    computeChanges: (stored) => computeAppendChanges({ stored, request, now }),
                });
                yield* SubscriptionRef.set(waiterState, waiterStateOf(Option.some(outcome.stream)));
                return outcome.appendResult;
            }),

        getMetadata: () => loadExistingStream.pipe(Effect.map(toStreamMetadata)),

        startRead: ({ from }) =>
            Effect.gen(function* () {
                const existing = yield* loadExistingStream;
                const start = Match.value(from).pipe(
                    Match.when('-1', () => zero),
                    Match.when('now', () => existing.tail),
                    Match.orElse((offset) => offset),
                );
                yield* storage.recordAccess({ stream, now: yield* DateTime.now });
                return {
                    metadata: toStreamMetadata(existing),
                    start,
                    end: existing.tail,
                    generation: existing.generation,
                };
            }),

        readBody: ({ from, to, generation }) =>
            Stream.unwrap(
                Effect.gen(function* () {
                    const existing = yield* loadSameGeneration(generation);
                    const chunks = storage.readChunks({ stream, from, to, generation });
                    if (!isJsonContentType(existing.contentType)) {
                        return Stream.map(chunks, (chunk) => chunk.bytes);
                    }
                    // a comma before each message after the first; parts after 0 continue a message
                    const items = Stream.flatMap(chunks, (chunk) => {
                        const startsLaterMessage = chunk.part === 0 && chunk.offset !== from;
                        return startsLaterMessage
                            ? Stream.make(comma, chunk.bytes)
                            : Stream.make(chunk.bytes);
                    });
                    return Stream.make(openBracket).pipe(
                        Stream.concat(items),
                        Stream.concat(Stream.make(closeBracket)),
                    );
                }),
            ),

        readSseEvents: ({ from, to, generation, closed, cursor }) =>
            Stream.unwrap(
                Effect.gen(function* () {
                    const existing = yield* loadSameGeneration(generation);
                    const encoding = sseDataEncodingFor(existing.contentType);
                    // a control event mid-read points at the next message; the last one at `to`
                    const controlBefore = (offset: Offset): SseControl => ({
                        streamNextOffset: offset,
                        streamCursor: cursor,
                    });
                    const lastControl: SseControl = closed
                        ? { streamNextOffset: to, streamClosed: true }
                        : { streamNextOffset: to, streamCursor: cursor, upToDate: true };

                    const writeChunk = (
                        state: SseEventsState,
                        chunk: StoredChunk,
                    ): readonly [SseEventsState, ReadonlyArray<SseOutput>] => {
                        const output: Array<Uint8Array | SseControl> = [];
                        const startsMessage = chunk.part === 0;
                        // a full event ends here, between two messages
                        const endsEvent = startsMessage && state.eventBytes >= sseEventBytes;
                        const continuing = endsEvent ? Option.none() : state.event;
                        if (Option.isSome(state.event) && Option.isNone(continuing)) {
                            output.push(
                                closeSseDataEvent(state.event.value),
                                controlBefore(chunk.offset),
                            );
                        }
                        const [opened, openText] = Option.match(continuing, {
                            onNone: () => openSseDataEvent(encoding),
                            onSome: (event) => [event, new Uint8Array(0)] as const,
                        });
                        const [started, separator] = startsMessage
                            ? startSseDataMessage(opened)
                            : ([opened, new Uint8Array(0)] as const);
                        const [written, data] = writeSseData(started, chunk.bytes);
                        output.push(openText, separator, data);
                        const eventBytes = Option.isSome(continuing) ? state.eventBytes : 0;
                        return [
                            {
                                event: Option.some(written),
                                eventBytes: eventBytes + chunk.bytes.byteLength,
                            },
                            [output],
                        ];
                    };

                    const finish = (state: SseEventsState): ReadonlyArray<SseOutput> => [
                        Option.match(state.event, {
                            onNone: () => [lastControl],
                            onSome: (event) => [closeSseDataEvent(event), lastControl],
                        }),
                    ];

                    return storage.readChunks({ stream, from, to, generation }).pipe(
                        Stream.mapAccum(
                            (): SseEventsState => ({ event: Option.none(), eventBytes: 0 }),
                            writeChunk,
                            { onHalt: finish },
                        ),
                        Stream.mapEffect((output) =>
                            Effect.forEach(output, (piece) =>
                                piece instanceof Uint8Array
                                    ? Effect.succeed(piece)
                                    : sseControlEvent(piece).pipe(Effect.orDie),
                            ).pipe(Effect.map(joinBytes)),
                        ),
                    );
                }),
            ),

        waitForNewMessages: ({ after, timeout }) =>
            Effect.gen(function* () {
                const existing = yield* loadExistingStream;
                const isSameStream = (state: WaiterState) =>
                    Option.contains(state.generation, existing.generation);
                const hasChanged = (state: WaiterState) =>
                    !isSameStream(state) || state.tail > after || state.closed;
                const changed = yield* SubscriptionRef.changes(waiterState).pipe(
                    Stream.filter(hasChanged),
                    Stream.runHead,
                    Effect.timeoutOption(timeout),
                    Effect.map(Option.flatten),
                );
                if (Option.isNone(changed)) {
                    return WaitResult.cases.TimedOut.make({});
                }
                if (!isSameStream(changed.value)) {
                    return yield* StreamNotFoundError.make({});
                }
                if (changed.value.tail > after) {
                    return WaitResult.cases.NewMessages.make({});
                }
                return WaitResult.cases.StreamClosed.make({});
            }),

        deleteStream: () =>
            Effect.gen(function* () {
                yield* loadExistingStream;
                yield* storage.deleteStream({ stream });
                yield* SubscriptionRef.set(waiterState, waiterStateOf(Option.none()));
            }),
    };
    return operations;
});
