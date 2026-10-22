import * as Alchemy from 'alchemy';
import * as Cloudflare from 'alchemy/Cloudflare';
import { Effect } from 'effect';
import DurableStreamsWorker from './src/worker';

export default Alchemy.Stack(
    'DurableStreams',
    { providers: Cloudflare.providers(), state: Alchemy.localState() },
    Effect.gen(function* () {
        const worker = yield* DurableStreamsWorker;
        return { url: worker.url };
    }),
);
