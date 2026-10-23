# alchemy-test

alchemy's own test runner, copied from `packages/alchemy-test` in [alchemy](https://github.com/alchemy-run/alchemy) at tag `v2.0.0-beta.83`, the alchemy version this repo installs. Upstream it is private and not on npm. Licence: Apache-2.0.

It runs every test file in one Bun process, several files at once. alchemy's local runtime (`alchemy/Test/Core.ts`, `makeSidecarHandle`) is one shared instance per process, kept alive while any file uses it; Vitest's separate worker per file breaks that, so files under `alchemy/Test/Vitest` with `dev: true` can't safely run at once. Test files use `alchemy/Test/Alchemy` (shipped in the alchemy package) and `alchemy-test`'s `describe`/`expect`.

Changed from upstream, nothing else:

- `package.json`: exports point at `src/` (upstream points `types` at a built `lib/`); dependency versions pinned to what alchemy's lockfile had at the tag (`@opentui/core` 0.5.16) and to this repo's Effect (`@effect/platform-bun` 4.0.5); build script and dev dependencies dropped.
- `scripts/` (the `lib/` build) and `test/` (upstream's tests of the runner) left out.

Don't edit `src/` or `bin/`; to update, copy them again from the alchemy tag matching the installed alchemy version.
