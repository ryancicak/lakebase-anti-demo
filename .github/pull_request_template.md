## What this changes

<!-- One or two sentences. Why, not just what. -->

## How it was verified

- [ ] Unit tests only
- [ ] Run against a real deployment (`bootstrap.sh` / a live bout), and I watched it work

<!--
Both answers are acceptable; an unverified answer is not. This project has
repeatedly had changes that passed the suite and then failed on stage, because
the suite fakes boto3, fakes Databricks and fakes the clock. If you ticked
"unit tests only", say so plainly -- the reviewer needs to know what has not
been observed, not a confident guess.
-->

Paste the pass count, or the command you ran:

```
```

## Blast radius

- [ ] Changes something an audience sees on screen (frontend, copy, timing, the
      order things appear in) -- describe what it now looks like
- [ ] Changes something that spends money or provisions infrastructure
      (Terraform, AWS or Databricks calls, capacity, expiry, the reaper) --
      describe the worst case if it is wrong
- [ ] Neither

<!--
Those two are called out because they are the failure modes that cost something
real: a stage-facing regression is only discovered in front of people, and a
spend-facing regression is only discovered on the bill. If either box is ticked,
expect the review to be slower.
-->

## Checklist

- [ ] `ruff check .` is clean
- [ ] No live account IDs, hostnames, ARNs or tokens in the diff
- [ ] `uv.lock` / `package-lock.json` still name only public PyPI and public npm
      (relocked, not hand-edited to an internal mirror)
