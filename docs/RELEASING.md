# Releasing

A build is tagged only after it passes the release bar below, end to end, on a
separate test installation. The bar is what the rounds must survive in front of
an audience: every round started, stopped and restarted in every way a presenter
can manage, alone and all at once, with the app restarted under live bouts, and
nothing left behind in AWS afterwards. v1.0.0 passed it with 114 of 114 chaos
scenarios before the scripts moved into this repository; they are the same
harness, with the installation-specific values taken out.

The bar is evidence for one build. Any code change after a step passes starts
the bar again on the new build: `run.sh` refuses an app that is not serving the
checkout's commit, and a pass on the previous commit says nothing about this one.

## What it costs

The whole bar takes a working day. The one command alone runs about seven and a half
hours with every round in use, on top of the test installation's standing cost
([docs/PRICING.md](PRICING.md)). The uninstall at the end stops all of it.

## 1. A test installation

Never run the bar against an installation anyone presents from: it holds every
round for hours, and its restart step redeploys the app. Make a separate one, in
its own clone, with its own app name
([a second installation in the same workspace](BOOTSTRAP.md#a-second-installation-in-the-same-workspace)):

```bash
git clone <this repository> anti-demo-release-test
cd anti-demo-release-test
git checkout <the commit under test>
cp <an existing checkout>/.env.bootstrap .
# Then set DATABRICKS_APP_NAME in .env.bootstrap to a name no app in the
# workspace has, e.g. lakebase-anti-demo-rc.
```

## 2. The lifecycle, by hand

Each step must succeed with nothing done on the side. A fix made by hand to get
past a step hides the defect the step exists to find.

1. **Fresh install.** `./bootstrap.sh --apply --deploy-app --yes`. It ends by
   printing the App URL and verifying that the app serves all six rounds.
2. **Uninstall.** Select the installation, then remove it:

   ```bash
   export ANTI_DEMO_MANIFEST="$PWD/$(ls -d .anti-demo-v*/ | sort -V | tail -1)manifest.json"
   ./antidemo cleanup --dry-run
   ./antidemo cleanup --yes
   .venv/bin/python scripts/release_bar/gone.py .
   ```

   `gone.py` must print `ALL GONE`. It counts, read-only, everything in AWS still
   tagged with the installation's run id, which it reads from the
   `cleanup-receipt.json` that cleanup leaves in place of the manifest. Check the
   app is gone too: `databricks apps get <app name> -p <profile>` must report
   that it does not exist.
3. **Reinstall into the same directory.** `./bootstrap.sh --apply --deploy-app --yes`
   again. A second install over the remains of the first is a different path
   from a fresh one.
4. **Re-run the installer on an idle installation.** Leave it untouched until
   Aurora has paused (`seconds_until_auto_pause` is 300 in `infra/aws/aurora.tf`;
   give it ten minutes), then run `./bootstrap.sh --apply --reset-ready --yes`.
   Setup racing Aurora's resume is a failure this step found just before v1.0.0.

## 3. The one command

From the root of any checkout of this repository, with the installation from
step 2 in place:

```bash
export ANTI_DEMO_APP_URL=<the App URL the installer printed>
export ANTI_DEMO_PROFILE=<the Databricks CLI profile the installer wrote>
scripts/release_bar/run.sh <the test checkout>
```

Start a follower on the app's log first, because `databricks apps logs` only
returns the last few minutes and a failure's explanation is usually there:

```bash
databricks apps logs <app name> -f -p "$ANTI_DEMO_PROFILE" > app-log.txt
```

`run.sh` writes everything to `release-bar-evidence/<UTC time>/`, which git
ignores, and exits 0 only if every step passed. `touch <evidence>/PAUSE` holds the
next chaos wave (the one in flight finishes); deleting the file lets it go on.
`STEPS=lease scripts/release_bar/run.sh ...` runs a subset into a new evidence
directory, for investigating; a release needs one run with every step.

| Step | What happens | Passes when |
|---|---|---|
| preflight | Reads `/api/version` and the board. | The app serves the checkout's commit with no uncommitted changes, and all six rounds are READY. |
| chaos | Six waves: cancel before the bell (Round 1), towel early, mid-stage and late, towel then re-arm at once, and finish with the result held on screen for five minutes. First all six rounds at once against Aurora, then against RDS, then each round alone: against Aurora, and against RDS too, since every round races an AWS lane. 124 scenarios. | Every bout ends verified or toweled (Round 1's pre-bell cancel ends canceled), every round comes back READY, a held result never changes, and a round not in play is never anything but READY. |
| backup | For each Round 1, 2, 3 and 5 AWS source (seven: Round 1 has only Aurora), takes a manual snapshot, which reads `backing-up` exactly as a source does through its daily automated backup, and runs one full bout of that round inside it; for Round 5 it also runs the installer's seal check there. Then deletes the snapshot. AWS schedules the real backups in a morning block, so a run overnight never meets one; rc10 met three on 2026-10-02 and Rounds 2, 3 and 5 refused or paused on them. | Every bout is verified, Round 5's seal check passes, both started while the source still read `backing-up` (a bout that missed the window fails, because it proves nothing), and every snapshot is gone. |
| restart | Starts Rounds 2, 3, 4, 5 and 6, redeploys the same build 90 seconds after the bell, and watches. | Every round is READY again within 45 minutes, with nobody touching it. Round 5 is the slow one: it waits out its old coordinator lease, then AWS. |
| crash | Starts a Round 4 bout and stops and starts the app 20 seconds after its bell, deploying nothing, while both lanes still race; then one Round 4 bout runs to the end. Then the same for Round 6. The redeploy above is too slow to land inside either round's bout. | Every round is READY again within 45 minutes, and each next bout is verified. Round 4's Prepare stops the Glue run the crash stranded and puts every destination back to its baseline first, which can take about five minutes. Round 6's stops the DMS task and Glue run it stranded and removes the bout's orders from both sources. The summary's scenario count includes these two bouts: 126. |
| leaks | Lists every resource tagged with the run id that Terraform did not make: RDS clones, restores and proxies, security groups and rules, secrets. Also any run of Round 4's or Round 6's Glue writers still active, and any of Round 6's DMS tasks still running. | Nothing remains within 30 minutes of every round being READY. A Glue run the restart stranded ends by its job's 30-minute timeout. |
| lease | Reads the `expires-at` lease on every resource Terraform made, then runs `terraform plan`, which applies nothing. | `/readyz` says the lease is current, it is no more than 7 hours behind `now + window`, it has moved past the expiry sealed at install once the installation is over 7 hours old, and the plan proposes no change. |

Then uninstall the test installation (step 2's commands) and require `ALL GONE`
once more.

Each step is also a script of its own in `scripts/release_bar/`, for looking
into a failure without the rest of the run. Each one's docstring has its usage.

| Script | Does |
|---|---|
| `ready.py` | Prints the board; `--wait` waits for all six rounds READY. Read-only. |
| `chaos.py` | One chaos phase, configured by `CHAOS_*` variables. |
| `restart.py` | The restart test. It waits for `run.sh` to redeploy, because `bootstrap.sh` fails its identity check when launched from Python. |
| `leftovers.py` | The leak check. Read-only. |
| `lease_check.py` | The lease check; `--plan` adds the Terraform plan. Read-only. |
| `gone.py` | After an uninstall, counts whatever is left. Read-only. |

`leftovers.py` and `gone.py` list whole resource types across the account: IAM policies and instance profiles, SQS queues, and Glue jobs and connections, for example. The install's own policies in [docs/iam](iam/) do not grant all of those listings, so the AWS key in `.env.bootstrap` needs account-wide read access for the bar.
| `summarize.py` | Writes `summary.md` from an evidence directory. |

## 4. Reading a failure

`summary.md` names each failed scenario. Behind it:

- `logs/` holds each step's output.
- `chaos/<phase>/events.jsonl` holds every request the harness made and every
  `/api/bout/all` answer, so a failure can be placed to the second even after the
  app's own log has rolled over.
- `chaos/<phase>/results.json` holds each scenario's end state, lanes and
  Round 5 runtime status.
- `restart/events.jsonl` holds every card change after the restart, timed from
  the restart.

A failure the app caused is a failure of the bar. Rerunning until it goes green
is not a pass. These, though, are known to look like app failures and are not:

- **This laptop lost its network.** Every round stalls at once, requests time
  out, and the log follower dies with "no route to host" at the same second.
  Check local connectivity before blaming a round.
- **Your public IP changed.** Laptop-to-Aurora or RDS connections in the
  lifecycle steps time out because the security groups admit the old address.
  `./antidemo setup` rebinds them. The one command connects only to the app and
  to AWS APIs, so it is unaffected.
- **Round 5 blames its sealed contract when a runner is unreachable.** Check
  that the runner instance is reachable over SSM before reading the error as a
  topology or seal defect.
- **CloudTrail shows `ec2:DescribeInternetGateways` denied** for the app's role on
  every Round 5 Proxy creation. That is RDS checking with the caller's
  credentials and retrying as its service-linked role. It happens on every bout
  and is harmless.

The harness already absorbs three races that used to read as failures, so they
should not reappear: a towel that loses the race with the finish (Round 1 against
RDS ends in about a second) is accepted as the UI accepts it; a poll sent before a
round was released that lands after it is not counted as an isolation failure;
and the OAuth token is minted again every five minutes, so a long phase cannot
die of a 401.

It also rides out the Databricks Apps front end answering in the app's place, with
`TEMPORARILY_UNAVAILABLE` or a proxy page (rc23, 2026-10-05). A GET is asked again
for up to fifteen seconds. A POST is settled from its session, as an answer cut
off on its way back is. The app's own 503 still fails the scenario.

## 5. Tag

With every step passed on one commit:

1. That commit already carries the new version in `pyproject.toml` and
   `frontend/package.json` (a test holds them equal). Bumping it afterwards is a
   change like any other, and the bar is evidence for one build.
2. Tag it: `git tag -a v<version> <commit> -m "v<version>"` and push the tag.
3. Publish a GitHub release whose notes say what changed, how it was verified
   (from `summary.md`), and what is still known to be limited.
4. If the Round 5 runner changed since the last release, say so in the upgrade
   notes: an existing installation needs `./antidemo runner refresh` before
   `./bootstrap.sh --deploy-only --yes`.
