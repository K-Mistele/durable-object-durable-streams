import { describe, expect, it } from '@effect/vitest';
import { Array as Arr, Match, Option, Schema } from 'effect';
import { ProducerEpoch, ProducerSeq } from '../src/codec';
import { decideProducer, ProducerDecision, type ProducerState } from '../src/producer';

// small ranges, so random requests often hit retries, new epochs and gaps
const ProducerRequest = Schema.Struct({
    epoch: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 3 })),
    seq: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 4 })),
});
type ProducerRequest = typeof ProducerRequest.Type;

/** Runs the requests in order, saving the state each accepted one leaves, as the engine does. */
const acceptedStates = (requests: ReadonlyArray<ProducerRequest>) =>
    Arr.reduce(
        requests,
        { stored: Option.none<ProducerState>(), accepted: Arr.empty<ProducerState>() },
        ({ stored, accepted }, request) =>
            Match.valueTags(
                decideProducer(stored, {
                    epoch: ProducerEpoch.make(request.epoch),
                    lastSeq: ProducerSeq.make(request.seq),
                }),
                {
                    Accepted: ({ next }) => ({
                        stored: Option.some(next),
                        accepted: Arr.append(accepted, next),
                    }),
                    Duplicate: () => ({ stored, accepted }),
                    StaleEpoch: () => ({ stored, accepted }),
                    InvalidEpochSeq: () => ({ stored, accepted }),
                    SequenceGap: () => ({ stored, accepted }),
                },
            ),
    ).accepted;

describe('decideProducer', () => {
    it.prop(
        'accepts each epoch from seq 0, one seq at a time, and never goes back an epoch',
        [Schema.Array(ProducerRequest)],
        ([requests]) => {
            const accepted = acceptedStates(requests);
            Arr.forEach(accepted, (state, index) => {
                const previous = Arr.get(accepted, index - 1);
                const expectedSeq = Option.match(previous, {
                    onNone: () => 0,
                    onSome: (before) => (before.epoch === state.epoch ? before.lastSeq + 1 : 0),
                });
                expect(state.lastSeq).toBe(expectedSeq);
                Option.map(previous, (before) =>
                    expect(state.epoch).toBeGreaterThanOrEqual(before.epoch),
                );
            });
        },
    );

    it.prop(
        'a retry of the last accepted request is a duplicate',
        [Schema.Array(ProducerRequest)],
        ([requests]) => {
            Option.map(Arr.last(acceptedStates(requests)), (last) =>
                expect(decideProducer(Option.some(last), last)).toEqual(
                    ProducerDecision.Duplicate({ lastSeq: last.lastSeq }),
                ),
            );
        },
    );
});
