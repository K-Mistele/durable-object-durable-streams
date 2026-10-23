import { defineConfig } from 'vite-plus';

export default defineConfig({
    test: {
        passWithNoTests: true,
        // One file at a time. Each file has its own stage, but alchemy's local runtime is shared:
        // in CI, one file's teardown closed another file's Worker mid-request.
        fileParallelism: false,
        // read by HttpSettings when the Stack deploys; the conformance suite expects 2s
        env: { DURABLE_STREAMS_LONG_POLL_TIMEOUT: '2 seconds' },
    },
});
