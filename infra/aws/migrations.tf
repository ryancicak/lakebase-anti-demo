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
