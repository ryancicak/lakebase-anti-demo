# NOTICE

Attributions and disclaimers for **Lakebase: The Anti-Demo**.

This file is the honest list of third-party material that ships with this project, plus
the required notices. It is not a legal document.

---

## Disclaimer

This is a personal side project. It is **not** a Databricks product, not an official
Databricks benchmark, and not a Databricks position on any competitor. Nothing here is
reviewed, endorsed, or published by Databricks, Amazon Web Services, or anyone else. The
opinions and the measurements are the author's own.

Every performance figure in this repository comes from a specific live run under the stated
configuration, with the methodology and the capacity-parity settings disclosed on screen and
in [`ROUNDS.md`](ROUNDS.md). That is a claim about the *run*, not about the evidence for it, and
the two are not the same strength everywhere: nearly every figure is backed by a sealed bout
receipt, while the two specific Round 4 and Round 6 times quoted in
[`docs/DEPLOY.md`](docs/DEPLOY.md) were established from the deployed app's own log stream,
because that app kept no receipt of either bout. Those two figures stay log-derived, and that
document is where they are marked as such; later bouts of both rounds are
receipt-backed, so the rounds themselves no longer rest on log lines.

Cost figures are weaker again, and say so where they appear. They are not uniformly weak, and the
weak half is not the one earlier revisions of this file named. The Databricks share is **posted
usage multiplied by a posted price** and the disclosure checks itself against its own projection.
The AWS share is quantities measured against a **published rate card that no invoice has ever
confirmed**, because `ce:GetCostAndUsage` is denied to the installation these figures came from —
so AWS has no posted counterpart at all, and that is where the uncertainty sits. It is a
demonstration, not a benchmark: one session, one region, one configuration, one point in time.
Prices and product behaviour change.
[`README.md`](README.md) § "What has been proven and what has not" lists which rounds have
and have not completed a live run, and flags where a run rests on log lines rather than on a
sealed receipt — read it before quoting any number from this project.

---

## Third-party software

### Press Start 2P (font) — required notice

The font binaries are bundled into the built application via the
[`@fontsource/press-start-2p`](https://www.npmjs.com/package/@fontsource/press-start-2p)
package, imported at `frontend/src/main.tsx`. Because the built app redistributes the font
files, the SIL Open Font License requires this notice to travel with it:

> Copyright 2012 The Press Start 2P Project Authors (cody@zone38.net), with Reserved Font
> Name "Press Start 2P"
>
> This Font Software is licensed under the SIL Open Font License, Version 1.1.

Full licence text: <https://openfontlicense.org/> — and it also ships verbatim in
`node_modules/@fontsource/press-start-2p/LICENSE`. The font is used **unmodified**, so the
Reserved Font Name condition is satisfied by not renaming it.

### Runtime and build dependencies

Declared in [`package.json`](frontend/package.json), [`pyproject.toml`](pyproject.toml) and
their lockfiles. Not vendored into this repository — they are fetched from npm and PyPI at
install time. The overwhelming majority are MIT, Apache-2.0, ISC or BSD. The ones worth
naming explicitly:

| Package | Licence | Note |
|---|---|---|
| `psycopg` / `psycopg-binary` | LGPL-3.0-only | The PostgreSQL driver. Used unmodified, as an installed library, dynamically imported — not statically linked and not redistributed here. |
| `certifi` | MPL-2.0 | CA bundle, used unmodified. |
| `lightningcss` | MPL-2.0 | Build-time only (via Vite), used unmodified. |
| `caniuse-lite` | CC-BY-4.0 | Build-time browser-support data, used unmodified. |

No GPL or AGPL dependency is present in either tree.

---

## Original work in this repository

Everything below was made for this project and is the author's own:

- **All artwork.** Every SVG in `brand/`, the persona portraits, the ring frames, the title
  and credits screens, the favicon, and the sprites drawn procedurally on canvas. No traced
  shapes, no third-party sprites, no stock assets.
- **All music and sound.** The chiptune themes, the bell, and the UI cues in
  `frontend/src/audio.ts` and `frontend/src/music.ts` are synthesised at runtime with the
  Web Audio API — oscillators and generated buffers. There are no audio samples or
  recordings anywhere in this project, so there is nothing to clear.
- **The pixel portrait** of the author is of the author.

All of it is released under the MIT licence in [`LICENSE`](LICENSE), which covers the code and
the original assets in this repository. The third-party notices above are separate: those
components keep their own licences and this project's licence does not alter them.

---

## Trademarks

All trademarks belong to their respective owners and are used here nominatively — to name
the actual products being measured or referred to.

- **Databricks** and **Lakebase** are trademarks of Databricks, Inc.
- **Amazon Web Services**, **Amazon Aurora**, **Amazon RDS** and **AWS** are trademarks of
  Amazon.com, Inc. or its affiliates. This project is not affiliated with, sponsored by, or
  endorsed by Amazon.
- **PostgreSQL** is a trademark of the PostgreSQL Community Association of Canada.

The 8-bit boxing presentation is an original homage to the console sports games of the late
1980s as a genre. It is not affiliated with, endorsed by, or derived from any specific
game or franchise, and it contains no third-party characters, sprites, wordmarks, audio, or
copied artwork.

**No game franchise, publisher, console or character name appears anywhere in this
repository** — not in the rendered UI, not in source, not in comments, not in filenames, not in
commit messages. That is asserted rather than assumed: it was swept for across tracked file
contents, tracked filenames and the full commit history. Every persona in `brand/personas-ringside/`
is an original character with an original name. The genre resemblance is deliberate; the names are
the line, and nothing crosses it.
