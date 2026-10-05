locals {
  use_default_network = var.vpc_id == null && var.subnet_ids == null
  resource_name       = "${var.name_prefix}-${var.run_id}"
  v7_enabled          = var.installation_id != null
  # Round 4 (r4) races a writer pipeline that carries each Delta change into
  # its own Aurora cluster and RDS instance, so it stands both up like the other
  # AWS rounds. It needs no logical decoding, so it keeps the engine-default
  # parameter groups, and its Aurora cluster can still pause when idle. These
  # addresses match the ones fix/contest-ledger-set-integrity created, so an
  # installation made from that branch keeps them. Round 6 (r6) joins for the
  # same reason in reverse: AWS DMS captures its checkout from these databases
  # into the lakehouse (docs/design/v1.1-rounds-4-6-aws.md, section 4). Keep this
  # list in step with server/lifecycle.py:_V7_ROUND_KEYS.
  v7_round_keys = toset(["r1", "r2", "r3", "r4", "r5", "r6"])
  v7_rounds     = local.v7_enabled ? local.v7_round_keys : toset([])

  # RDS has its own key list because its fleet is no longer the same shape as
  # Aurora's. Round 1 keeps an Aurora cluster -- it is the only engine that can
  # compete in a wake-from-idle round -- but stands up no RDS instance, because
  # the RDS lane there refuses to enter on engine semantics
  # (server/capacity.py:RDS_SCORED_ROUNDS) and never gets prepared, connected to
  # or timed. A provisioned instance would bill about $1.77/day to measure
  # nothing. Round 1's *stated customer* cost is unchanged and is imputed
  # instead: see the RDS carrying line in server/cost_model.py.
  #
  # Do not fold this back into v7_round_keys. That list still drives Aurora, the
  # subnet groups and the per-round slugs, and dropping r1 from it would take
  # r1's Aurora cluster with it. Keep this list in step with
  # server/lifecycle.py:_V7_RDS_ROUND_KEYS.
  v7_rds_round_keys = toset(["r2", "r3", "r4", "r5", "r6"])
  v7_rds_rounds     = local.v7_enabled ? local.v7_rds_round_keys : toset([])

  # The rounds whose databases are read by logical replication, and so run on the
  # parameter groups in parameter_groups.tf rather than the engine defaults: Round
  # 6, whose AWS lane is AWS DMS change capture. Aurora with logical replication on
  # never auto-pauses, which is why no other round is here. Must be a subset of
  # both lists above; the check in parameter_groups.tf enforces it. The name is the
  # one fix/contest-ledger-set-integrity gave it, so its installations keep their
  # parameter groups.
  v7_lakeflow_round_keys = toset(["r6"])
  v7_lakeflow_rounds     = local.v7_enabled ? local.v7_lakeflow_round_keys : toset([])

  # A stable 80-bit digest keeps independently installed copies in the same
  # account/workspace from colliding without exposing the installation ID.
  # Including the round key in the digest and suffix makes ownership exact.
  v7_round_slugs = local.v7_enabled ? {
    for round in local.v7_round_keys :
    round => "i${substr(sha256("${trimspace(var.installation_id)}:${round}"), 0, 20)}-${round}"
  } : {}
  v7_round_resource_names = {
    for round, slug in local.v7_round_slugs :
    round => "${substr(var.name_prefix, 0, 12)}-${substr(var.run_id, 0, 8)}-${slug}"
  }

  # `expires-at` is a lease, not an ownership tag. Terraform writes it once, at
  # creation, and the serving app moves it forward while people use the
  # installation (server/lease.py). So every tagged resource carries
  # `ignore_changes = [tags["expires-at"]]` -- a moved lease is not drift -- and
  # tests/test_installation_lease.py fails if a tagged resource lacks it.
  required_tags = {
    "anti-demo-run-id" = var.run_id
    "Owner"            = trimspace(var.owner)
    "owner"            = trimspace(var.owner)
    "expires-at"       = var.expires_at
    "managed-by"       = "terraform"
  }

  # IAM treats tag keys case-insensitively and rejects Owner + owner as a
  # duplicate pair. Other AWS services retain the established tag contract.
  iam_role_required_tags = {
    "anti-demo-run-id" = var.run_id
    "owner"            = trimspace(var.owner)
    "expires-at"       = var.expires_at
    "managed-by"       = "terraform"
  }

  v7_round_tags = {
    for round, slug in local.v7_round_slugs : round => merge(local.required_tags, {
      "anti-demo-installation-slug" = slug
      "anti-demo-round"             = round
    })
  }
  v7_round_iam_tags = {
    for round, slug in local.v7_round_slugs : round => merge(local.iam_role_required_tags, {
      "anti-demo-installation-slug" = slug
      "anti-demo-round"             = round
    })
  }

  round5_resource_name = local.v7_enabled ? local.v7_round_resource_names["r5"] : local.resource_name
  round5_required_tags = local.v7_enabled ? local.v7_round_tags["r5"] : local.required_tags
  round5_iam_tags      = local.v7_enabled ? local.v7_round_iam_tags["r5"] : local.iam_role_required_tags
  round5_policy_tags   = local.v7_enabled ? local.v7_round_iam_tags["r5"] : local.required_tags
  round5_ownership_tags = local.v7_enabled ? {
    "anti-demo-installation-slug" = local.v7_round_slugs["r5"]
    "anti-demo-round"             = "r5"
  } : {}

  round5_bout_base_tags = merge(local.round5_required_tags, {
    "managed-by" = "round5-lifecycle"
  })
  round5_bout_tag_keys = sort(keys(merge(local.round5_bout_base_tags, {
    "anti-demo-bout-id"    = ""
    "anti-demo:bout-token" = ""
    # The per-bout Proxy/target-group carry an immutable operation-identity fence
    # tag (server/connection_spike_live.py _coordinator: tags["anti-demo:bout-fence"]).
    # It MUST be in this set or CreateDBProxy's dependent rds:AddTagsToResource is
    # denied (ForAllValues aws:TagKeys), which fails the competitor lane at the
    # bell. Membership here also extends the tag-hijack Deny guard to the fence,
    # matching the "verified EXACTLY on inspect/adopt/cleanup" contract.
    "anti-demo:bout-fence" = ""
  })))

  database_name   = "anti_demo"
  master_username = "antidemo_admin"
  # IAM role and instance-profile name_prefix values are capped at 38
  # characters. Keep a readable owner prefix plus a stable digest of the full
  # run-scoped resource name; the provider-added suffix preserves uniqueness.
  round5_iam_stem                    = local.v7_enabled ? local.v7_round_slugs["r5"] : "r5-${substr(var.name_prefix, 0, 8)}-${substr(sha256(local.round5_resource_name), 0, 16)}"
  round5_runner_secret_policy_prefix = local.v7_enabled ? "${local.round5_iam_stem}-secret-" : "r5-runner-rds-master-"
  round5_proxy_secret_policy_prefix  = local.v7_enabled ? "${local.round5_iam_stem}-proxy-secret-" : "r5-proxy-secrets-"
  round5_execution_policy_prefix     = local.v7_enabled ? "${local.round5_iam_stem}-exec-" : "r5-execution-"
  # Every API-created bout resource begins with one of these deterministic,
  # run-scoped prefixes. The lifecycle app appends the journaled bout ID.
  round5_bout_name_prefix = local.v7_enabled ? "${local.v7_round_slugs["r5"]}-" : "r5-${substr(var.run_id, 0, 8)}-${substr(sha256(local.resource_name), 0, 8)}-"

  active_db_subnet_groups = local.v7_enabled ? aws_db_subnet_group.by_round : {
    r1 = aws_db_subnet_group.round1[0]
  }
  active_aurora_security_groups = local.v7_enabled ? aws_security_group.aurora_by_round : {
    r1 = aws_security_group.aurora[0]
  }
  active_rds_security_groups = local.v7_enabled ? aws_security_group.rds_by_round : {
    r1 = aws_security_group.rds_control_plane_only[0]
  }
  active_aurora_clusters = local.v7_enabled ? aws_rds_cluster.aurora_by_round : {
    r1 = aws_rds_cluster.aurora[0]
  }
  active_aurora_writers = local.v7_enabled ? aws_rds_cluster_instance.aurora_writer_by_round : {
    r1 = aws_rds_cluster_instance.aurora_writer[0]
  }
  active_rds_instances = local.v7_enabled ? aws_db_instance.rds_by_round : {
    r1 = aws_db_instance.rds_control_plane_only[0]
  }
  active_required_tags = local.v7_enabled ? local.v7_round_tags : {
    r1 = local.required_tags
  }
  round5_database_key   = local.v7_enabled ? "r5" : "r1"
  round5_aurora_cluster = local.active_aurora_clusters[local.round5_database_key]
  round5_rds_instance   = local.active_rds_instances[local.round5_database_key]
  round5_aurora_sg      = local.active_aurora_security_groups[local.round5_database_key]
  round5_rds_sg         = local.active_rds_security_groups[local.round5_database_key]

  selected_vpc_id = local.use_default_network ? data.aws_vpc.default[0].id : var.vpc_id
  selected_subnet_ids = local.use_default_network ? (
    data.aws_subnets.default[0].ids
  ) : var.subnet_ids
  selected_runner_subnet_id = local.use_default_network ? try(
    sort(data.aws_subnets.default[0].ids)[0],
    null,
  ) : var.runner_subnet_id
  runner_route_table_id = coalesce(
    try(one(data.aws_route_tables.runner_explicit.ids), null),
    one(data.aws_route_tables.runner_main.ids),
  )
}

check "network_input_mode" {
  assert {
    condition = (
      (var.vpc_id == null && var.subnet_ids == null && var.runner_subnet_id == null) ||
      (var.vpc_id != null && var.subnet_ids != null && var.runner_subnet_id != null)
    )
    error_message = "Set vpc_id, subnet_ids, and runner_subnet_id together, or leave all three null for default-VPC discovery."
  }
}

check "round5_app_principal_account" {
  assert {
    condition     = try(split(":", var.round5_app_principal_arn)[4] == var.aws_account_id, false)
    error_message = "round5_app_principal_arn must belong to aws_account_id."
  }
}

check "resource_name_length" {
  assert {
    condition     = length(local.resource_name) <= 45
    error_message = "name_prefix and run_id are too long together; their combined resource prefix must be 45 characters or fewer."
  }
}
