"""The one maximum time for a bout's lane, from the bell, in every round that races a task.

Ryan, 2026-10-03: "maybe even like a 800 second maximum per round or something like that just to
make it simpler for a MAXIMUM time? or 900 seconds? and then whoever completes before that 900
seconds is deemed the winner!" A lane still running at the limit loses to a lane that finished,
by at least the difference, and its clock shows the floor it ran to. A lane that errored measured
nothing, and a lane still running is never handed a win.

It applies to Rounds 2, 3, 4 and 6. Round 1 is a wake measured in seconds, with its own connect
deadline, and Round 5 races to 10,000 clients under its own protocol.
"""

#: 15 minutes. Lakebase finishes each of these rounds in well under a minute, so the limit only
#: ever decides how long a stuck AWS lane can keep the room. Normal AWS lanes, measured in full
#: bouts: Round 2 404-619 s, Round 3 716-921 s, Round 4 80-162 s, Round 6 66-100 s. Round 3's
#: slowest therefore ends at the limit, as a lower bound.
BOUT_TIME_LIMIT_SECONDS = 900.0
