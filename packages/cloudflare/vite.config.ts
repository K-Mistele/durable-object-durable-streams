import { defineConfig } from 'vite-plus';

export default defineConfig({
    test: {
        passWithNoTests: true,
        // each file deploys the same local Stack, on the same port
        fileParallelism: false,
        // read by HttpSettings when the Stack deploys; the conformance suite expects 2s
        env: { DURABLE_STREAMS_LONG_POLL_TIMEOUT: '2 seconds' },
    },
});
