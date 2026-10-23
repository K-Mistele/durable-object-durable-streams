import { describe, expect, it } from '@effect/vitest';
import { DateTime, Effect, Option } from 'effect';
import {
    Offset,
    ProducerEpoch,
    ProducerId,
    ProducerSeq,
    StreamSeq,
} from '@kylelayer/effect-durable-streams-protocol/codec';
import {
    ContentTypeMismatchError,
    InvalidEpochSeqError,
    SequenceGapError,
    StaleEpochError,
    StreamConfigConflictError,
    StreamNotFoundError,
    StreamSeqConflictError,
} from '@kylelayer/effect-durable-streams-protocol/errors';
import type { ProducerRequestId, StoredStream } from '../src/stored-stream';
import { AppendRequest } from '../src/stream-operation-requests';
import {
    getExistingStream,
    isExpired,
    isRepeatedClose,
    ProducerSequencePosition,
    rejectConfigConflict,
    rejectContentTypeMismatch,
    rejectOutOfSequenceProducerRequest,
    rejectStreamSeqRegression,
    type StreamConfig,
    updateStreamForAppend,
} from '../src/stream-rules';

const createdAt = DateTime.makeUnsafe('2026-01-01T00:00:00Z');
const thirtySecondsLater = DateTime.add(createdAt, { seconds: 30 });

const openStream: StoredStream = {
    generation: 7,
    contentType: 'application/json',
    tail: Offset.make(3),
    closed: false,
    closedBy: Option.none(),
    ttlSeconds: Option.none(),
    expiresAt: Option.none(),
    createdAt,
    lastAccessedAt: createdAt,
    lastStreamSeq: Option.none(),
    fork: Option.none(),
    copy: Option.none(),
    softDeleted: false,
    hasForks: false,
};

const producerRequestId = ({ epoch, seq }: { readonly epoch: number; readonly seq: number }) =>
    Option.some<ProducerRequestId>({
        id: ProducerId.make('p1'),
        epoch: ProducerEpoch.make(epoch),
        seq: ProducerSeq.make(seq),
    });

/** Where a request falls when the stored producer is at epoch 1, last seq 4. */
const positionAfterEpoch1Seq4 = (producer: { readonly epoch: number; readonly seq: number }) =>
    rejectOutOfSequenceProducerRequest({
        storedProducer: Option.some({ epoch: ProducerEpoch.make(1), lastSeq: ProducerSeq.make(4) }),
        producer: producerRequestId(producer),
    });

const appendMessages = AppendRequest.cases.AppendMessages.make({
    contentType: 'application/json',
    body: new TextEncoder().encode('{"a":1}'),
    closeAfterAppend: false,
    streamSeq: Option.none(),
    producer: Option.none(),
});

const closeStream = AppendRequest.cases.CloseStream.make({ producer: Option.none() });

describe('isExpired', () => {
    it('expires a sliding TTL counted from the last access', () => {
        const stream = { ...openStream, ttlSeconds: Option.some(30) };
        const now = DateTime.add(createdAt, { seconds: 29 });
        expect(isExpired({ stream, now })).toBe(false);
        expect(isExpired({ stream, now: thirtySecondsLater })).toBe(true);
    });

    it('expires at the absolute time', () => {
        const stream = { ...openStream, expiresAt: Option.some(thirtySecondsLater) };
        expect(isExpired({ stream, now: createdAt })).toBe(false);
        expect(isExpired({ stream, now: thirtySecondsLater })).toBe(true);
    });
});

describe('getExistingStream', () => {
    it.effect('returns a stored stream that has not expired', () =>
        Effect.gen(function* () {
            const stream = yield* getExistingStream({
                stored: Option.some(openStream),
                now: createdAt,
            });
            expect(stream).toEqual(openStream);
        }),
    );

    it.effect('fails with 404 when nothing is stored or the stream expired', () =>
        Effect.gen(function* () {
            const missing = yield* Effect.flip(
                getExistingStream({ stored: Option.none(), now: createdAt }),
            );
            expect(missing).toBeInstanceOf(StreamNotFoundError);
            const expired = { ...openStream, expiresAt: Option.some(createdAt) };
            const gone = yield* Effect.flip(
                getExistingStream({ stored: Option.some(expired), now: thirtySecondsLater }),
            );
            expect(gone).toBeInstanceOf(StreamNotFoundError);
        }),
    );
});

describe('rejectConfigConflict', () => {
    const config: StreamConfig = {
        contentType: 'application/json',
        ttlSeconds: Option.none(),
        expiresAt: Option.none(),
        closed: false,
    };

    it.effect('accepts the same config, comparing content types by media type', () =>
        rejectConfigConflict({
            existing: openStream,
            requested: { ...config, contentType: 'Application/JSON; charset=utf-8' },
        }),
    );

    it.effect('fails with 409 on a different TTL, expiry, closed state or content type', () =>
        Effect.gen(function* () {
            for (const requested of [
                { ...config, ttlSeconds: Option.some(60) },
                { ...config, expiresAt: Option.some(thirtySecondsLater) },
                { ...config, closed: true },
                { ...config, contentType: 'text/plain' },
            ]) {
                const error = yield* Effect.flip(
                    rejectConfigConflict({ existing: openStream, requested }),
                );
                expect(error).toBeInstanceOf(StreamConfigConflictError);
            }
        }),
    );
});

describe('isRepeatedClose', () => {
    const closedByProducer = {
        ...openStream,
        closed: true,
        closedBy: producerRequestId({ epoch: 0, seq: 4 }),
    };

    it('treats a close without producer headers as a repeat', () => {
        expect(isRepeatedClose({ stream: closedByProducer, request: closeStream })).toBe(true);
    });

    it('treats the exact producer request that closed the stream as a repeat', () => {
        const retry = {
            ...appendMessages,
            closeAfterAppend: true,
            producer: producerRequestId({ epoch: 0, seq: 4 }),
        };
        expect(isRepeatedClose({ stream: closedByProducer, request: retry })).toBe(true);
    });

    it('treats anything else as new', () => {
        const otherProducerClose = {
            ...closeStream,
            producer: producerRequestId({ epoch: 0, seq: 5 }),
        };
        expect(isRepeatedClose({ stream: closedByProducer, request: otherProducerClose })).toBe(
            false,
        );
        expect(isRepeatedClose({ stream: closedByProducer, request: appendMessages })).toBe(false);
    });
});

describe('rejectContentTypeMismatch', () => {
    it.effect('fails with 409 on a different media type', () =>
        Effect.gen(function* () {
            yield* rejectContentTypeMismatch({
                stream: openStream,
                contentType: 'application/json; charset=utf-8',
            });
            const error = yield* Effect.flip(
                rejectContentTypeMismatch({ stream: openStream, contentType: 'text/plain' }),
            );
            expect(error).toBeInstanceOf(ContentTypeMismatchError);
        }),
    );
});

describe('rejectOutOfSequenceProducerRequest', () => {
    it.effect('places requests without producer headers outside any sequence', () =>
        Effect.gen(function* () {
            const position = yield* rejectOutOfSequenceProducerRequest({
                storedProducer: Option.none(),
                producer: Option.none(),
            });
            expect(position).toEqual(ProducerSequencePosition.NoProducer());
        }),
    );

    it.effect('accepts the next seq and recognises a retry', () =>
        Effect.gen(function* () {
            expect(yield* positionAfterEpoch1Seq4({ epoch: 1, seq: 5 })).toEqual(
                ProducerSequencePosition.NextInSequence({
                    state: { epoch: ProducerEpoch.make(1), lastSeq: ProducerSeq.make(5) },
                }),
            );
            expect(yield* positionAfterEpoch1Seq4({ epoch: 1, seq: 3 })).toEqual(
                ProducerSequencePosition.AlreadyAccepted({ lastSeq: ProducerSeq.make(4) }),
            );
        }),
    );

    it.effect('fails with 403, 400 or 409 out of sequence', () =>
        Effect.gen(function* () {
            expect(
                yield* Effect.flip(positionAfterEpoch1Seq4({ epoch: 0, seq: 0 })),
            ).toBeInstanceOf(StaleEpochError);
            expect(
                yield* Effect.flip(positionAfterEpoch1Seq4({ epoch: 2, seq: 1 })),
            ).toBeInstanceOf(InvalidEpochSeqError);
            expect(
                yield* Effect.flip(positionAfterEpoch1Seq4({ epoch: 1, seq: 7 })),
            ).toBeInstanceOf(SequenceGapError);
        }),
    );
});

describe('rejectStreamSeqRegression', () => {
    const seenB = { ...openStream, lastStreamSeq: Option.some(StreamSeq.make('b')) };

    it.effect('fails with 409 at or below the last Stream-Seq', () =>
        Effect.gen(function* () {
            const error = yield* Effect.flip(
                rejectStreamSeqRegression({
                    stream: seenB,
                    streamSeq: Option.some(StreamSeq.make('b')),
                }),
            );
            expect(error).toBeInstanceOf(StreamSeqConflictError);
            yield* rejectStreamSeqRegression({
                stream: seenB,
                streamSeq: Option.some(StreamSeq.make('c')),
            });
            yield* rejectStreamSeqRegression({ stream: seenB, streamSeq: Option.none() });
        }),
    );

    // PROTOCOL.md 5.2: byte-wise order. In UTF-8 the emoji (F0 9F…) sorts after U+FF61 (EF BD A1);
    // in UTF-16 code units it sorts before (D83D < FF61)
    it.effect('compares Stream-Seq values by their UTF-8 bytes', () =>
        Effect.gen(function* () {
            const seenHalfwidthStop = {
                ...openStream,
                lastStreamSeq: Option.some(StreamSeq.make('\uff61')),
            };
            yield* rejectStreamSeqRegression({
                stream: seenHalfwidthStop,
                streamSeq: Option.some(StreamSeq.make('\u{1F600}')),
            });
            const seenEmoji = {
                ...openStream,
                lastStreamSeq: Option.some(StreamSeq.make('\u{1F600}')),
            };
            const error = yield* Effect.flip(
                rejectStreamSeqRegression({
                    stream: seenEmoji,
                    streamSeq: Option.some(StreamSeq.make('\uff61')),
                }),
            );
            expect(error).toBeInstanceOf(StreamSeqConflictError);
        }),
    );
});

describe('updateStreamForAppend', () => {
    it('advances the tail and records the close, its producer and the Stream-Seq', () => {
        const closingAppend = {
            ...appendMessages,
            closeAfterAppend: true,
            producer: producerRequestId({ epoch: 0, seq: 0 }),
            streamSeq: Option.some(StreamSeq.make('x')),
        };
        expect(
            updateStreamForAppend({
                stream: openStream,
                request: closingAppend,
                messageCount: 2,
                now: thirtySecondsLater,
            }),
        ).toEqual({
            ...openStream,
            tail: Offset.make(5),
            closed: true,
            closedBy: producerRequestId({ epoch: 0, seq: 0 }),
            lastStreamSeq: Option.some(StreamSeq.make('x')),
            lastAccessedAt: thirtySecondsLater,
        });
    });

    it('closes without moving the tail on a close request, keeping the last Stream-Seq', () => {
        const seenB = { ...openStream, lastStreamSeq: Option.some(StreamSeq.make('b')) };
        const updated = updateStreamForAppend({
            stream: seenB,
            request: closeStream,
            messageCount: 0,
            now: thirtySecondsLater,
        });
        expect(updated.tail).toEqual(Offset.make(3));
        expect(updated.closed).toBe(true);
        expect(updated.lastStreamSeq).toEqual(Option.some(StreamSeq.make('b')));
    });
});
