/**
 * Upstream's conformance suite against the Stack in workerd (`dev: true`). The suite reads
 * `config.baseUrl` inside each test, so `beforeEach` sets it from the deployed Stack. The SSE tests
 * are patched to accept `data: ` (patches/, upstream PR #285). The suite's tests take vitest's
 * default timeout, 5s; `test:conformance` raises it to 15s, since the first request after deploy
 * starts the Worker and the first Durable Object, which took over 5s on a busy CI runner.
 */
import { runConformanceTests } from '@durable-streams/server-conformance-tests';
import { describe } from '@effect/vitest';
import * as Cloudflare from 'alchemy/Cloudflare';
import * as Test from 'alchemy/Test/Vitest';
import { Effect } from 'effect';
import Stack from '../alchemy.run';

// its own stage, so its local state is its own
const { beforeAll, beforeEach, deploy } = Test.make({
    providers: Cloudflare.providers(),
    dev: true,
    stage: `${Test.defaultStage()}-conformance`,
});

const stack = beforeAll(deploy(Stack));

// vite.config.ts gives the Worker the same 2s long-poll timeout
const config = { baseUrl: '', longPollTimeoutMs: 2_000 };

beforeEach(
    Effect.gen(function* () {
        const url = yield* stack.pipe(
            Effect.flatMap(({ url }) =>
                url === undefined
                    ? Effect.die('The Worker has no URL: it needs workers.dev or a domain')
                    : Effect.succeed(url),
            ),
        );
        config.baseUrl = url.replace(/\/+$/, '');
    }),
);

describe('conformance on the local Stack', () => {
    runConformanceTests(config);
});
