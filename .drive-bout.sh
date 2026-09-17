#!/usr/bin/env bash
# Drive one Round 5 bout to a terminal state and report the numbers it produced.
#
# Waits for the arm to settle, starts the run, then polls until the session leaves the
# running states. Prints the setup clock per lane while setup runs and the client counters
# while the burst runs, so a failure is attributable to a phase rather than to "it failed".
set -uo pipefail
cd "$(dirname "$0")"

PORT="${1:-8081}"
SID="$(cat /tmp/r5-sid.txt)"
PY=.venv-3.12/bin/python
BASE="http://127.0.0.1:$PORT/api/sessions/$SID"

show() {
  $PY -c "
import json,datetime
d=json.load(open('/tmp/r5-drive.json'))
s=d.get('round5_setup') or {}
lanes=d.get('lanes') or {}
setup_lanes=s.get('lanes') or {}
stamp=datetime.datetime.now().strftime('%H:%M:%S')
state=d.get('state')
if s.get('state') in ('pending','running') and not any(v.get('successes') for v in lanes.values()):
    detail=' '.join(f\"{k}={v.get('state')}/{round(v.get('setup_elapsed_ms') or 0)}ms\" for k,v in setup_lanes.items())
else:
    detail=' '.join(f\"{k}={v.get('state')}/{v.get('successes')}\" for k,v in lanes.items())
print(f'{stamp} | {state} | setup={s.get(\"state\")} | {detail}')
f=d.get('failure')
if f: print('  FAILURE:', json.dumps(f)[:240])
"
}

echo "waiting for the arm to settle"
for _ in $(seq 1 30); do
  curl -s --max-time 15 "$BASE" -o /tmp/r5-drive.json || { sleep 5; continue; }
  STATE="$($PY -c "import json;print(json.load(open('/tmp/r5-drive.json')).get('state'))")"
  [ "$STATE" = "armed" ] && break
  [ "$STATE" = "failed" ] && { echo "arm failed"; show; exit 1; }
  sleep 5
done
echo "armed; starting the run"
curl -s --max-time 120 -X POST "$BASE/run" -o /dev/null -w "run HTTP %{http_code}\n"

for _ in $(seq 1 90); do
  curl -s --max-time 15 "$BASE" -o /tmp/r5-drive.json || { sleep 20; continue; }
  show
  STATE="$($PY -c "import json;print(json.load(open('/tmp/r5-drive.json')).get('state'))")"
  case "$STATE" in
    running|armed|checking) sleep 20 ;;
    *) echo "=== terminal: $STATE ==="; break ;;
  esac
done

echo
echo "=== what the bout produced ==="
$PY -c "
import json
d=json.load(open('/tmp/r5-drive.json'))
for metric in d.get('metrics') or []:
    print(f\"  {metric.get('id')}: \", {k:v for k,v in metric.items() if k!='id'})
c=d.get('comparison')
print('  comparison:', json.dumps(c)[:400] if c else None)
s=d.get('round5_setup') or {}
for lane_id, lane in (s.get('lanes') or {}).items():
    print(f\"  setup {lane_id}: {lane.get('setup_elapsed_ms')} ms  state={lane.get('state')}\")
"
