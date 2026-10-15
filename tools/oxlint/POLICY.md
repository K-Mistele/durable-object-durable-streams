# TypeOnce Oxlint policy

The built-in policy, correctness/suspicious categories, and six
`better-tailwindcss` rules come from
[`typeonce-dev/ai-automation`](https://github.com/typeonce-dev/ai-automation)
at commit `0bca096fe6fe9878cd15303a623dd2cd85915ddd`.

The Tailwind plugin reads the website's real stylesheet at
`apps/website/src/style.css`. All six Tailwind rules are errors. The upstream
built-in severities are preserved, including rules intentionally set to off;
see `vite.config.ts` for the executable policy.

The upstream generated catalog was programmatically loaded and validated at
75 unique entries: 49 custom OXLint rules, 3 typed rules, 1 workspace rule, 11
built-ins, 1 documented legacy policy, 2 categories, 6 Tailwind rules, and 2
diff-check rules. This repository retains the original 49 and installs every
remaining catalog entry.
