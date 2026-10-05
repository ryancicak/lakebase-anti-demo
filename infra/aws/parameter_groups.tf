# Logical-replication parameter groups for Round 6's databases.
#
# Round 6 (analyze_live_orders_without_slowing_checkout) races each competitor's
# own change capture: AWS DMS reads the checkout from the r6 Aurora cluster or RDS
# instance over a PostgreSQL logical replication slot, and an AWS Glue job appends
# the changes to a Delta table in the lakehouse (docs/design/v1.1-rounds-4-6-aws.md,
# section 4). Logical replication needs wal_level = logical, which on RDS and
# Aurora is set only through the `rds.logical_replication` parameter. So Round 6's
# databases run on dedicated parameter groups that set it, while every other round
# stays on the engine defaults (locals.tf:v7_lakeflow_round_keys and the check
# below).
#
# The addresses and names are the ones fix/contest-ledger-set-integrity created for
# its Lakeflow Connect lane, kept so an installation made from that branch upgrades
# without replacing them. That branch described the groups as "for Lakeflow Connect
# ingestion"; a description change forces a replacement, which the fixed names and
# create_before_destroy would turn into a name collision, so the description is
# ignored after creation.
#
# `max_slot_wal_keep_size` is finite: DMS's slot is standing, and while its task is
# parked between bouts the slot retains WAL, so a stalled or parked consumer must
# not retain it without bound. The 1024 MB cap is the same on both sources.
#
# Reboot and apply semantics, stated because they are easy to get wrong:
#   * `rds.logical_replication` is a *static* parameter. AWS accepts a static
#     parameter only with apply_method = "pending-reboot"; "immediate" is rejected
#     at apply time, so the method is pinned below.
#   * These groups are attached to the r6 cluster and instance *at creation*
#     (aurora.tf and rds.tf), so the parameter is in force from first boot and no
#     reboot is ever needed. Verify with `SHOW wal_level;` -> `logical`.
#   * If either group were ever associated with an already-running database, that
#     database would need a reboot before the change took effect. Terraform does not
#     reboot on a parameter-group change.

resource "aws_rds_cluster_parameter_group" "lakeflow_aurora" {
  # rds.logical_replication is a cluster-level parameter on Aurora PostgreSQL and
  # exists only in a DB *cluster* parameter group, so it is set here rather than on
  # aws_rds_cluster_instance.
  for_each = local.v7_lakeflow_rounds

  name        = "${local.v7_round_resource_names[each.key]}-aurora-lakeflow"
  family      = "aurora-postgresql17"
  description = "${upper(each.key)} Aurora PostgreSQL logical replication for change capture"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    # PostgreSQL reads this unitless value as MB; 1024 caps the slot at 1 GiB.
    name         = "max_slot_wal_keep_size"
    value        = "1024"
    apply_method = "immediate"
  }

  tags = local.v7_round_tags[each.key]

  lifecycle {
    create_before_destroy = true
    ignore_changes        = [tags["expires-at"], description]
  }
}

resource "aws_db_parameter_group" "lakeflow_rds" {
  # The RDS PostgreSQL equivalent. rds.logical_replication is an instance-level
  # parameter here, so it lives in a DB parameter group attached to the instance.
  for_each = local.v7_lakeflow_rounds

  name        = "${local.v7_round_resource_names[each.key]}-rds-lakeflow"
  family      = "postgres17"
  description = "${upper(each.key)} RDS PostgreSQL logical replication for change capture"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_slot_wal_keep_size"
    value        = "1024"
    apply_method = "immediate"
  }

  tags = local.v7_round_tags[each.key]

  lifecycle {
    create_before_destroy = true
    ignore_changes        = [tags["expires-at"], description]
  }
}

locals {
  # Attachment lookups, resolved from the resources so the names cannot drift from
  # what Terraform created. aurora.tf and rds.tf look each round up here and fall
  # back to the engine default for every other round.
  v7_aurora_cluster_parameter_group_names = {
    for round_key, group in aws_rds_cluster_parameter_group.lakeflow_aurora :
    round_key => group.name
  }
  v7_rds_parameter_group_names = {
    for round_key, group in aws_db_parameter_group.lakeflow_rds :
    round_key => group.name
  }
}

# Every logical-replication round must also stand up both an Aurora cluster and an
# RDS instance for the groups above to attach to. This fails the plan loudly if the
# round-key sets in locals.tf ever disagree, rather than leaving an orphaned group.
check "lakeflow_rounds_have_databases" {
  assert {
    condition     = length(setsubtract(local.v7_lakeflow_round_keys, local.v7_round_keys)) == 0
    error_message = "Every v7_lakeflow_round_keys entry must also be in v7_round_keys (needs an Aurora cluster)."
  }
  assert {
    condition     = length(setsubtract(local.v7_lakeflow_round_keys, local.v7_rds_round_keys)) == 0
    error_message = "Every v7_lakeflow_round_keys entry must also be in v7_rds_round_keys (needs an RDS instance)."
  }
}
