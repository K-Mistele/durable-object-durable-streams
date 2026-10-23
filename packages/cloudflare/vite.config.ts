import { defineConfig } from 'vite-plus';

// Only the conformance suite runs under Vitest (it registers Vitest tests itself). Our own test
// files run under alchemy-test, in one process (tools/alchemy-test/UPSTREAM.md).
export default defineConfig({
    test: {
        passWithNoTests: true,
        // read by HttpSettings when the Stack deploys; the conformance suite expects 2s
        env: { DURABLE_STREAMS_LONG_POLL_TIMEOUT: '2 seconds' },
    },
});
