import { defineConfig } from 'vite-plus';

export default defineConfig({
    test: {
        passWithNoTests: true,
        // read by HttpSettings when the Stack deploys; the conformance suite expects 2s
        env: { DURABLE_STREAMS_LONG_POLL_TIMEOUT: '2 seconds' },
    },
});
