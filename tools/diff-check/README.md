# TypeOnce diff check

The runtime, OXLint plugin, and both added-line rules were vendored from
[`typeonce-dev/ai-automation`](https://github.com/typeonce-dev/ai-automation)
at commit `0bca096fe6fe9878cd15303a623dd2cd85915ddd`.

`diff/key-file-change` is configured for this repository's Bun lockfile, root
package policy, lint/build configuration, and future database migrations. Run
the review-time check with `bun run diff:check -- --base <git-ref>`.
