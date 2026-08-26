resource "aws_db_instance" "rds_control_plane_only" {
  count = local.v7_enabled ? 0 : 1

  identifier = "${local.resource_name}-rds"

  # instance_class is 2 vCPU / 4 GiB. Memory is matched to the Lakebase 2 CU
  # ceiling (~4 GB) and the Aurora 2 ACU ceiling (~4 GiB) so no lane absorbs
  # Round 5's burst on a smaller box. Keep in step with
  # server/capacity.py:RDS_INSTANCE_CLASS, which capacity_parity enforces
  # against live state.
  engine               = "postgres"
  engine_version       = "17.10"
  instance_class       = "db.t4g.medium"
  db_name              = local.database_name
  port                 = 5432
  parameter_group_name = "default.postgres17"

  username                    = local.master_username
  manage_master_user_password = true

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_subnet_group_name   = aws_db_subnet_group.round1[0].name
  vpc_security_group_ids = [aws_security_group.rds_control_plane_only[0].id]
  network_type           = "IPV4"
  publicly_accessible    = true
  multi_az               = false

  backup_retention_period    = 1
  copy_tags_to_snapshot      = true
  auto_minor_version_upgrade = false
  apply_immediately          = true
  deletion_protection        = false
  skip_final_snapshot        = true
  delete_automated_backups   = true

  performance_insights_enabled = false
  monitoring_interval          = 0

  tags = local.required_tags
}

resource "aws_db_instance" "rds_by_round" {
  # v7_rds_rounds, not v7_rounds: Round 1 has an Aurora cluster and no RDS
  # instance. See the comment on local.v7_rds_round_keys.
  for_each = local.v7_rds_rounds

  identifier = "${local.v7_round_resource_names[each.key]}-rds"

  # See rds_control_plane_only above: 4 GiB matches both competitors' ceilings.
  engine               = "postgres"
  engine_version       = "17.10"
  instance_class       = "db.t4g.medium"
  db_name              = local.database_name
  port                 = 5432
  parameter_group_name = "default.postgres17"

  username                    = local.master_username
  manage_master_user_password = true

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_subnet_group_name   = aws_db_subnet_group.by_round[each.key].name
  vpc_security_group_ids = [aws_security_group.rds_by_round[each.key].id]
  network_type           = "IPV4"
  publicly_accessible    = true
  multi_az               = false

  backup_retention_period    = 1
  copy_tags_to_snapshot      = true
  auto_minor_version_upgrade = false
  apply_immediately          = true
  deletion_protection        = false
  skip_final_snapshot        = true
  delete_automated_backups   = true

  performance_insights_enabled = false
  monitoring_interval          = 0

  tags = local.v7_round_tags[each.key]
}
