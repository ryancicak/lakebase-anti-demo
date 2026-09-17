# Preserve existing v6 state addresses after the legacy singleton resources
# gained conditional count. These moves are no-ops for fresh v7 installations.
moved {
  from = aws_db_subnet_group.round1
  to   = aws_db_subnet_group.round1[0]
}

moved {
  from = aws_security_group.aurora
  to   = aws_security_group.aurora[0]
}

moved {
  from = aws_security_group.rds_control_plane_only
  to   = aws_security_group.rds_control_plane_only[0]
}

# Preserve the existing Lakebase runner's egress-rule identity while narrowing
# it from all protocols to public PostgreSQL only. The additional HTTPS rule and
# the competitor runner's isolated rules are new resources.
moved {
  from = aws_vpc_security_group_egress_rule.round5_runner_outbound
  to   = aws_vpc_security_group_egress_rule.round5_lakebase_runner_postgres
}

moved {
  from = aws_rds_cluster.aurora
  to   = aws_rds_cluster.aurora[0]
}

moved {
  from = aws_rds_cluster_instance.aurora_writer
  to   = aws_rds_cluster_instance.aurora_writer[0]
}

moved {
  from = aws_db_instance.rds_control_plane_only
  to   = aws_db_instance.rds_control_plane_only[0]
}
