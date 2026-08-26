---
name: Bug report
about: Something behaved differently from what it said it would
title: ""
labels: ""
---

## What happened, and what you expected instead

## How to reproduce it

<!-- The exact command. If it involves a deployment, say which step. -->

## Environment

- OS and architecture:
- `uv --version` / `node --version`:
- Did you have AWS or Databricks credentials in the shell? (yes / no / not sure)

<!--
That last question is not idle: the code deliberately refuses ambiguous
credential situations, so an exported AWS_PROFILE or a stray AWS_ACCESS_KEY_ID
changes behaviour. If a test fails for you and not in CI, this is usually why.
-->

## Output

<!--
The server log is not scrubbed, and that is deliberate: `--background` writes
`server-<port>.log` beside the manifest, warnings and errors reach it with full
tracebacks, and the traceback is the useful part. Two things in it are yours
rather than the project's, so trim them if this issue is public.

- **Every traceback frame carries your home directory**, so pasting one
  publishes your account name and where you keep this checkout. This is the
  only identifier that has actually been found in a real local log.
- **An AWS or Postgres failure quotes the provider's own message.** A credential
  expiry says nothing interesting, but an `AccessDenied` names the full role ARN
  it denied, account ID included. The refusal text the UI shows drops that on
  purpose, because an audience is watching; the log keeps it, because whoever
  reads the log normally owns the account it names.

Neither is a secret and neither has to go for the report to be useful. If you do
find a credential in there, that is its own bug -- see SECURITY.md.
-->

```
```
