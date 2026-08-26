# The one principal every copy of this installation authenticates as.
#
# Why this exists at all. The laptop authenticates as an Identity Center
# permission-set role; the deployed Databricks App authenticates as an IAM user,
# because `validate_app_aws_environment` hard-requires static keys and no
# Databricks-native keyless path is bindable to an app runtime today. Those are
# two different principal ARNs, and `round5.control_role_trusted_principal_arn`
# seals exactly one -- which is why Round 5 cannot run in the deployed app at
# all. A single role trusted by both collapses the two into one sealed answer:
# whoever starts the process, `sts:GetCallerIdentity` returns
# `assumed-role/anti-demo-runtime/<session>`, and `principal_matches` resolves it
# to the same sealed role ARN for both.
#
# The trust document therefore carries TWO principals. That is the whole point of
# the resource, and it is also the thing that makes it fragile: naming an IAM
# user in a `Principal` element stores the user's unique ID, not its ARN, so any
# account-level sweep that deletes and recreates IAM users breaks the
# relationship permanently even after a user with an identical name and an
# identical ARN is recreated. `_anti_demo_runtime_trust_check` in
# server/lifecycle.py is what notices; `antidemo renew` is what repairs it. Neither
# is optional -- see docs/iam/README.md, "the sweep ran, bring it back".
#
# Everything here costs $0. An IAM role, three customer-managed policies and
# three attachments are all free; only the databases and the runner cost money.

locals {
  anti_demo_runtime_enabled = length(var.anti_demo_runtime_principal_arns) > 0

  # Derived, not invented. These three documents are the reviewed operator
  # permission set that `docs/iam/README.md` already describes and that an
  # operator attaches by hand today; the runtime role is simply the same set
  # attached to a role instead of to a human. Splitting them three ways is not a
  # style choice: IAM caps a single policy document at 6144 non-whitespace
  # characters, and the union of these three is roughly 8,500.
  #
  # `anti-demo-operator-4-state.json` is deliberately absent. It is the opt-in S3
  # state backend, which a default install never touches, and it carries a
  # `<STATE_BUCKET>` placeholder that nothing here could substitute -- rendering
  # it would attach a policy naming a bucket that does not exist. Attach it to
  # this role by hand if you opt into `--state-backend s3`.
  anti_demo_runtime_policy_files = local.anti_demo_runtime_enabled ? {
    "1-network"   = "anti-demo-operator-1-network.json"
    "2-databases" = "anti-demo-operator-2-databases.json"
    "3-identity"  = "anti-demo-operator-3-identity.json"
  } : {}

  # `jsonencode(jsondecode(...))` is what makes the 6144 cap comfortable: the
  # checked-in files are indented for review and IAM counts every non-whitespace
  # character, so re-encoding compactly recovers roughly a third of the budget.
  anti_demo_runtime_policies = {
    for key, filename in local.anti_demo_runtime_policy_files :
    key => jsonencode(jsondecode(replace(
      replace(
        file("${path.module}/../../docs/iam/${filename}"),
        "<AWS_ACCOUNT_ID>",
        var.aws_account_id,
      ),
      "<AWS_REGION>",
      var.aws_region,
    )))
  }
}

check "anti_demo_runtime_policy_size" {
  assert {
    condition = alltrue([
      for key, document in local.anti_demo_runtime_policies : length(document) <= 6144
    ])
    error_message = "A rendered anti-demo-runtime policy exceeds the 6144-character IAM limit. Split the offending document in docs/iam/ rather than trimming a resource constraint out of it."
  }
}

check "anti_demo_runtime_principal_accounts" {
  assert {
    condition = alltrue([
      for arn in var.anti_demo_runtime_principal_arns :
      try(split(":", arn)[4] == var.aws_account_id, false)
    ])
    error_message = "Every anti_demo_runtime_principal_arns entry must belong to aws_account_id."
  }
}

data "aws_iam_policy_document" "anti_demo_runtime_assume" {
  count = local.anti_demo_runtime_enabled ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"
      # Both principals in one statement. `_canonical_iam_policy` sorts the
      # identifier list, so the order supplied here never matters -- and the
      # sealed expectation is compared as a set, not as a sequence.
      identifiers = var.anti_demo_runtime_principal_arns
    }
  }
}

resource "aws_iam_role" "anti_demo_runtime" {
  count = local.anti_demo_runtime_enabled ? 1 : 0

  # A fixed name, deliberately, where every other IAM resource here uses
  # `name_prefix`. The operator's `~/.aws/config` carries this ARN in a
  # `role_arn` key, and the sweep is expected to remove and the installer to
  # recreate this role every fortnight. A generated suffix would mean editing
  # `~/.aws/config` after every sweep, which is exactly the recurring manual
  # step this role exists to remove. Override the name when two installations
  # must share one account without sharing a principal.
  name                 = var.anti_demo_runtime_role_name
  description          = "Single sealed principal assumed by both the operator SSO role and the deployed app's IAM user"
  assume_role_policy   = data.aws_iam_policy_document.anti_demo_runtime_assume[0].json
  max_session_duration = var.anti_demo_runtime_max_session_seconds

  tags = local.iam_role_required_tags
}

resource "aws_iam_policy" "anti_demo_runtime" {
  for_each = local.anti_demo_runtime_policy_files

  name_prefix = "${var.anti_demo_runtime_role_name}-${each.key}-"
  description = "Rendered from docs/iam/${each.value} for the anti-demo-runtime role"
  policy      = local.anti_demo_runtime_policies[each.key]

  tags = local.iam_role_required_tags
}

resource "aws_iam_role_policy_attachment" "anti_demo_runtime" {
  for_each = local.anti_demo_runtime_policy_files

  role       = aws_iam_role.anti_demo_runtime[0].name
  policy_arn = aws_iam_policy.anti_demo_runtime[each.key].arn
}
