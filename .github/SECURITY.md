# Reporting a security problem

Please do not open a public issue for anything that would help somebody attack a
running deployment.

Use GitHub's private reporting instead: on this repository, go to **Security →
Advisories → Report a vulnerability**. That opens a channel visible only to the
maintainers. If it is not available, contact the repository owner directly
through their GitHub profile.

Useful things to include: what an attacker could do, the smallest set of steps
that shows it, and whether it needs credentials or network position.

Two categories worth reporting even if they feel minor, because of what this
project does:

- **A committed secret or live identifier.** An AWS account ID, a workspace
  hostname, an ARN, a bucket name or a token that reached the repository. There
  is a test that tries to prevent this
  (`tests/test_no_live_identifiers_committed.py`); if something got past it, that
  is worth knowing about twice over.
- **Anything that lets an unauthenticated caller spend money.** This project
  provisions real infrastructure. A path that provisions, extends or fails to
  reap without authorisation is a security problem, not a billing one.

There is no bounty, and no guaranteed response time -- this is a demonstration
project maintained by one person. Reports are still read.
