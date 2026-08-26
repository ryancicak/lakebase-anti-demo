"""The guard in `conftest.py` holds, and holds only where it should.

One test, both directions, because the two claims are one claim: the guard is
worth having only if it stops an unstubbed call, and it is only shippable if it
is invisible to the ten-odd tests here that drive real botocore clients under
`Stubber` to validate call shapes against the live service model. Those tests
are the good kind -- two defects that cost real money got past hand-written
fakes and would not have got past them -- so breaking them to close this hole
would have been a bad trade.

Both hold for the same structural reason, which is why one test can pin them.
`Stubber` answers on botocore's `before-call` event; `BaseClient._make_api_call`
consults that event and only calls `Endpoint.make_request` when nothing
answered. The guard is on `make_request`. So a stubbed call never arrives and an
unstubbed one always does, one line before botocore would have signed it.
"""

from __future__ import annotations

import boto3
import pytest
from botocore.stub import Stubber

from tests.conftest import RealAwsCallAttempted

# Deliberately the shape of a call that would matter: a real region and the
# identifier a published clone would carry, so the message this asserts on is
# the message a stranger running `pytest` would actually read.
CLUSTER = "anti-demo-aurora"


def rds_client():
    return boto3.Session(
        region_name="us-west-2",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    ).client("rds")


def test_the_guard_blocks_an_unstubbed_call_and_lets_a_stubbed_one_through() -> None:
    with pytest.raises(RealAwsCallAttempted) as caught:
        rds_client().describe_db_clusters(DBClusterIdentifier=CLUSTER)

    message = str(caught.value)
    # The two things a message has to carry to be worth reading: which call, and
    # which test issued it. Without the second, a suite-wide run reports a
    # botocore failure with nothing pointing at the test that caused it.
    assert "RDS.DescribeDBClusters" in message
    assert "test_the_guard_blocks_an_unstubbed_call_and_lets_a_stubbed_one_through" in message
    assert "Stubber" in message

    stubbed = rds_client()
    with Stubber(stubbed) as stubber:
        stubber.add_response(
            "describe_db_clusters",
            {"DBClusters": []},
            {"DBClusterIdentifier": CLUSTER},
        )
        assert stubbed.describe_db_clusters(DBClusterIdentifier=CLUSTER) == {"DBClusters": []}
        stubber.assert_no_pending_responses()
