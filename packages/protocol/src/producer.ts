import { Data, Option } from 'effect';
import { ProducerEpoch, ProducerSeq } from './codec';

export interface ProducerState {
    readonly epoch: ProducerEpoch;
    readonly lastSeq: ProducerSeq;
}

export type ProducerDecision = Data.TaggedEnum<{
    Accepted: { readonly next: ProducerState };
    Duplicate: { readonly lastSeq: ProducerSeq };
    StaleEpoch: { readonly currentEpoch: ProducerEpoch };
    InvalidEpochSeq: {};
    SequenceGap: { readonly expectedSeq: ProducerSeq; readonly receivedSeq: ProducerSeq };
}>;
export const ProducerDecision = Data.taggedEnum<ProducerDecision>();

// PROTOCOL.md 5.2.1: stored state + the request's epoch and seq → what to do with the append.
// Changes nothing: the caller saves `next` only after the append commits.
export const decideProducer = (
    stored: Option.Option<ProducerState>,
    request: ProducerState,
): ProducerDecision =>
    Option.match(stored, {
        onNone: () =>
            request.lastSeq === 0
                ? ProducerDecision.Accepted({ next: request })
                : ProducerDecision.SequenceGap({
                      expectedSeq: ProducerSeq.make(0),
                      receivedSeq: request.lastSeq,
                  }),
        onSome: (current) => {
            if (request.epoch < current.epoch) {
                return ProducerDecision.StaleEpoch({ currentEpoch: current.epoch });
            }
            if (request.epoch > current.epoch) {
                return request.lastSeq === 0
                    ? ProducerDecision.Accepted({ next: request })
                    : ProducerDecision.InvalidEpochSeq();
            }
            if (request.lastSeq <= current.lastSeq) {
                return ProducerDecision.Duplicate({ lastSeq: current.lastSeq });
            }
            if (request.lastSeq === current.lastSeq + 1) {
                return ProducerDecision.Accepted({ next: request });
            }
            return ProducerDecision.SequenceGap({
                expectedSeq: ProducerSeq.make(current.lastSeq + 1),
                receivedSeq: request.lastSeq,
            });
        },
    });
