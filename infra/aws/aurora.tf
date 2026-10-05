resource "aws_rds_cluster" "aurora" {
  count = local.v7_enabled ? 0 : 1

  cluster_identifier = "${local.resource_name}-aurora"

  engine         = "aurora-postgresql"
  engine_version = "17.10"
  database_name  = local.database_name
  port           = 5432

  master_username             = local.master_username
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.round1[0].name
  vpc_security_group_ids = [aws_security_group.aurora[0].id]
  network_type           = "IPV4"

  serverlessv2_scaling_configuration {
    min_capacity             = 0
    max_capacity             = 2
    seconds_until_auto_pause = 300
  }

  storage_encrypted       = true
  backup_retention_period = 1
  copy_tags_to_snapshot   = true
  apply_immediately       = true
  deletion_protection     = false
  skip_final_snapshot     = true

  tags = local.required_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_rds_cluster_instance" "aurora_writer" {
  count = local.v7_enabled ? 0 : 1

  identifier         = "${local.resource_name}-aurora-writer"
  cluster_identifier = aws_rds_cluster.aurora[0].id

  instance_class = "db.serverless"
  engine         = aws_rds_cluster.aurora[0].engine
  engine_version = aws_rds_cluster.aurora[0].engine_version

  db_subnet_group_name       = aws_db_subnet_group.round1[0].name
  publicly_accessible        = true
  promotion_tier             = 0
  auto_minor_version_upgrade = false
  apply_immediately          = true

  tags = local.required_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_rds_cluster" "aurora_by_round" {
  for_each = local.v7_rounds

  cluster_identifier = "${local.v7_round_resource_names[each.key]}-aurora"

  engine         = "aurora-postgresql"
  engine_version = "17.10"
  database_name  = local.database_name
  port           = 5432

  master_username             = local.master_username
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.by_round[each.key].name
  vpc_security_group_ids = [aws_security_group.aurora_by_round[each.key].id]
  network_type           = "IPV4"

  # Round 6 (r6) gets a cluster parameter group that turns on logical
  # replication for AWS DMS; every other round resolves to null and keeps the
  # default.aurora-postgresql17 group, so their clusters see no change. The
  # group is attached at creation, so the parameter is in force from first boot
  # (parameter_groups.tf). With it on, r6's cluster never auto-pauses: it idles
  # at its lowest capacity instead, and the cost model bills it that way.
  db_cluster_parameter_group_name = lookup(local.v7_aurora_cluster_parameter_group_names, each.key, null)

  serverlessv2_scaling_configuration {
    min_capacity             = 0
    max_capacity             = 2
    seconds_until_auto_pause = 300
  }

  storage_encrypted       = true
  backup_retention_period = 1
  copy_tags_to_snapshot   = true
  apply_immediately       = true
  deletion_protection     = false
  skip_final_snapshot     = true

  tags = local.v7_round_tags[each.key]

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_rds_cluster_instance" "aurora_writer_by_round" {
  for_each = local.v7_rounds

  identifier         = "${local.v7_round_resource_names[each.key]}-aurora-writer"
  cluster_identifier = aws_rds_cluster.aurora_by_round[each.key].id

  instance_class = "db.serverless"
  engine         = aws_rds_cluster.aurora_by_round[each.key].engine
  engine_version = aws_rds_cluster.aurora_by_round[each.key].engine_version

  db_subnet_group_name       = aws_db_subnet_group.by_round[each.key].name
  publicly_accessible        = true
  promotion_tier             = 0
  auto_minor_version_upgrade = false
  apply_immediately          = true

  tags = local.v7_round_tags[each.key]

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}
