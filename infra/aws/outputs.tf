output "aws_region" {
  description = "AWS region containing the Round 1 resources."
  value       = var.aws_region
}

output "vpc_id" {
  description = "VPC used by both AWS database lanes."
  value       = local.selected_vpc_id
}

output "subnet_ids" {
  description = "Subnets in the shared DB subnet group."
  value       = local.selected_subnet_ids
}

output "round_installation_slugs" {
  description = "Collision-resistant installation-and-round slugs for the v7 resources; empty for the legacy v6 layout."
  value       = local.v7_round_slugs
}

output "required_tags_by_round" {
  description = "Exact Terraform ownership tags keyed by round."
  value       = local.active_required_tags
}

output "db_subnet_group_names" {
  description = "Database subnet group names keyed by round."
  value       = { for round, group in local.active_db_subnet_groups : round => group.name }
}

output "aurora_security_group_ids" {
  description = "Isolated Aurora security group IDs keyed by round."
  value       = { for round, group in local.active_aurora_security_groups : round => group.id }
}

output "aurora_cluster_ids" {
  description = "Aurora cluster identifiers keyed by round."
  value       = { for round, cluster in local.active_aurora_clusters : round => cluster.cluster_identifier }
}

output "aurora_cluster_arns" {
  description = "Aurora cluster ARNs keyed by round."
  value       = { for round, cluster in local.active_aurora_clusters : round => cluster.arn }
}

output "aurora_cluster_resource_ids" {
  description = "Stable Aurora cluster resource IDs keyed by round."
  value       = { for round, cluster in local.active_aurora_clusters : round => cluster.cluster_resource_id }
}

output "aurora_writer_instance_ids" {
  description = "Aurora writer identifiers keyed by round."
  value       = { for round, writer in local.active_aurora_writers : round => writer.identifier }
}

output "aurora_writer_instance_arns" {
  description = "Aurora writer ARNs keyed by round."
  value       = { for round, writer in local.active_aurora_writers : round => writer.arn }
}

output "aurora_writer_endpoints" {
  description = "Aurora writer hostnames keyed by round."
  value       = { for round, cluster in local.active_aurora_clusters : round => cluster.endpoint }
}

output "aurora_ports" {
  description = "Aurora PostgreSQL ports keyed by round."
  value       = { for round, cluster in local.active_aurora_clusters : round => cluster.port }
}

output "aurora_secret_arns" {
  description = "AWS-managed Aurora master-user secret ARNs keyed by round."
  value       = { for round, cluster in local.active_aurora_clusters : round => try(cluster.master_user_secret[0].secret_arn, null) }
}

output "rds_security_group_ids" {
  description = "Isolated RDS security group IDs keyed by round."
  value       = { for round, group in local.active_rds_security_groups : round => group.id }
}

output "rds_instance_ids" {
  description = "RDS PostgreSQL instance identifiers keyed by round."
  value       = { for round, instance in local.active_rds_instances : round => instance.identifier }
}

output "rds_instance_arns" {
  description = "RDS PostgreSQL instance ARNs keyed by round."
  value       = { for round, instance in local.active_rds_instances : round => instance.arn }
}

output "rds_addresses" {
  description = "RDS PostgreSQL hostnames keyed by round."
  value       = { for round, instance in local.active_rds_instances : round => instance.address }
}

output "rds_endpoints" {
  description = "RDS PostgreSQL host:port endpoints keyed by round."
  value       = { for round, instance in local.active_rds_instances : round => instance.endpoint }
}

output "rds_resource_ids" {
  description = "Stable RDS resource IDs keyed by round."
  value       = { for round, instance in local.active_rds_instances : round => instance.resource_id }
}

output "rds_secret_arns" {
  description = "AWS-managed RDS master-user secret ARNs keyed by round."
  value       = { for round, instance in local.active_rds_instances : round => try(instance.master_user_secret[0].secret_arn, null) }
}

output "db_subnet_group_name" {
  description = "Shared DB subnet group name."
  value       = local.active_db_subnet_groups["r1"].name
}

output "aurora_security_group_id" {
  description = "Baseline Aurora security group for direct operator and runner control traffic. Per-bout proxy rules are lifecycle-owned."
  value       = local.active_aurora_security_groups["r1"].id
}

output "aurora_cluster_id" {
  description = "Value for AURORA_CLUSTER_ID."
  value       = local.active_aurora_clusters["r1"].cluster_identifier
}

output "aurora_cluster_arn" {
  description = "ARN of the Aurora Serverless v2 cluster."
  value       = local.active_aurora_clusters["r1"].arn
}

output "aurora_writer_instance_id" {
  description = "Identifier of the single db.serverless writer."
  value       = local.active_aurora_writers["r1"].identifier
}

output "aurora_writer_instance_arn" {
  description = "ARN of the single db.serverless writer."
  value       = local.active_aurora_writers["r1"].arn
}

output "aurora_writer_endpoint" {
  description = "Aurora cluster writer hostname."
  value       = local.active_aurora_clusters["r1"].endpoint
}

output "aurora_port" {
  description = "Aurora PostgreSQL port."
  value       = local.active_aurora_clusters["r1"].port
}

output "aurora_secret_arn" {
  description = "ARN of the AWS-managed Aurora master-user secret. No secret value is stored or output by this module."
  value       = try(local.active_aurora_clusters["r1"].master_user_secret[0].secret_arn, null)
}

output "round5_aurora_direct_host" {
  description = "Hostname-only Aurora writer endpoint sealed for the Round 5 untimed observer and cleanup control path."
  value       = local.round5_aurora_cluster.endpoint
}

output "round5_aurora_cluster_resource_id" {
  description = "Stable exact Aurora cluster resource ID sealed into the baseline for per-bout target validation."
  value       = local.round5_aurora_cluster.cluster_resource_id
}

# The five outputs below are the pre-v7 single-lane mirror, kept because the
# manifest still carries a flat `aws.resources` block alongside the per-round
# seals. Round 1 no longer stands an RDS instance up, so under v7 they resolve
# to null rather than to an identifier. They are deliberately NOT re-pointed at
# another round's instance: aliasing r2's box into a field named for r1 would
# make the seal describe a resource Round 1 does not have, and the arming path
# would then try to describe it.
#
# Consumers must treat null as "Round 1 has no RDS instance", which is the same
# thing `round_environments.wake_idle_app.rds = null` says.

output "rds_security_group_id" {
  description = "Baseline RDS security group for direct operator and runner control traffic. Null under v7: Round 1 stands no RDS instance up. Per-bout proxy rules are lifecycle-owned."
  value       = try(local.active_rds_security_groups["r1"].id, null)
}

output "rds_instance_id" {
  description = "Value for RDS_INSTANCE_ID. Null under v7: Round 1 stands no RDS instance up."
  value       = try(local.active_rds_instances["r1"].identifier, null)
}

output "rds_instance_arn" {
  description = "ARN of the Round 1 RDS PostgreSQL instance. Null under v7: no such instance exists."
  value       = try(local.active_rds_instances["r1"].arn, null)
}

output "rds_endpoint" {
  description = "Round 1 RDS PostgreSQL source endpoint. Null under v7: no such instance exists."
  value       = try(local.active_rds_instances["r1"].endpoint, null)
}

output "rds_secret_arn" {
  description = "ARN of the AWS-managed Round 1 RDS master-user secret. Null under v7: no such instance exists. No secret value is stored or output by this module."
  value       = try(local.active_rds_instances["r1"].master_user_secret[0].secret_arn, null)
}

output "round5_rds_direct_host" {
  description = "Hostname-only RDS endpoint sealed for the Round 5 untimed observer and cleanup control path."
  value       = local.round5_rds_instance.address
}

output "round5_rds_resource_id" {
  description = "Stable exact RDS resource ID sealed into the baseline for per-bout target validation."
  value       = local.round5_rds_instance.resource_id
}

output "round5_runner_instance_id" {
  description = "Instance ID of the neutral SSM-managed Round 5 runner."
  value       = aws_instance.round5_runner.id
}

output "round5_runner_instance_arn" {
  description = "ARN of the neutral SSM-managed Round 5 runner."
  value       = aws_instance.round5_runner.arn
}

output "round5_runner_public_ip" {
  description = "Public IPv4 address assigned to the outbound-only Round 5 runner."
  value       = aws_instance.round5_runner.public_ip
}

output "round5_runner_subnet_id" {
  description = "Public-routed subnet containing the Round 5 runner."
  value       = aws_instance.round5_runner.subnet_id
}

output "round5_runner_role_arn" {
  description = "ARN of the bounded EC2 role used by the neutral runner."
  value       = aws_iam_role.round5_runner.arn
}

output "round5_runner_instance_profile_arn" {
  description = "ARN of the neutral runner's EC2 instance profile."
  value       = aws_iam_instance_profile.round5_runner.arn
}

output "round5_control_role_arn" {
  description = "ARN of the exact-principal-trusted role that creates, observes, and removes journaled per-bout add-ons."
  value       = aws_iam_role.round5_execution.arn
}

output "round5_execution_role_arn" {
  description = "Compatibility alias for round5_control_role_arn."
  value       = aws_iam_role.round5_execution.arn
}

output "round5_runner_permissions_boundary_arn" {
  description = "Permissions boundary that caps the runner at SSM core plus exact baseline and Proxy credential secret access."
  value       = aws_iam_policy.round5_runner_boundary.arn
}

output "round5_proxy_service_role_arn" {
  description = "ARN of the Terraform-owned service role shared by the Round 5 Aurora and RDS Proxies."
  value       = aws_iam_role.round5_proxy_service.arn
}

output "round5_proxy_service_policy_name" {
  description = "Name of the Proxy service role's exact two-secret inline policy."
  value       = aws_iam_role_policy.round5_proxy_secrets.name
}

output "round5_aurora_proxy_secret_arn" {
  description = "ARN of the Terraform-owned Aurora Proxy credential container; Terraform stores no secret version."
  value       = aws_secretsmanager_secret.round5_aurora_proxy_credentials.arn
}

output "round5_rds_proxy_secret_arn" {
  description = "ARN of the Terraform-owned RDS Proxy credential container; Terraform stores no secret version."
  value       = aws_secretsmanager_secret.round5_rds_proxy_credentials.arn
}

output "round5_bout_name_prefix" {
  description = "Deterministic run-scoped prefix to which the lifecycle app appends each journaled bout ID."
  value       = local.round5_bout_name_prefix
}

output "round5_bout_base_tags" {
  description = "Required static tags for every per-bout add-on; the lifecycle also adds anti-demo-bout-id."
  value       = local.round5_bout_base_tags
}

output "round5_app_principal_arn" {
  description = "Exact IAM principal trusted by the baseline control role."
  value       = var.round5_app_principal_arn
}

output "round5_runner_security_group_id" {
  description = "ID of the ingress-free Round 5 runner security group."
  value       = aws_security_group.round5_runner.id
}

output "round5_runner_egress_rule_id" {
  description = "ID of the Terraform-owned outbound rule in the immutable Round 5 baseline."
  value       = aws_vpc_security_group_egress_rule.round5_runner_outbound.id
}

output "anti_demo_runtime_role_arn" {
  description = "ARN of the single sealed principal both the operator profile and the deployed app assume. Null when anti_demo_runtime_principal_arns is empty, which is what every pre-existing installation looks like."
  value       = try(aws_iam_role.anti_demo_runtime[0].arn, null)
}

output "anti_demo_runtime_trusted_principal_arns" {
  description = "Exactly the principals named in the sealed runtime role's trust policy. Sealed as a set so `antidemo doctor` can hold the live trust document against it after the fortnightly sweep."
  value       = var.anti_demo_runtime_principal_arns
}

output "required_tags" {
  description = "Exact ownership tags applied to Terraform-managed resources; IAM roles and instance profiles canonicalize owner to lowercase."
  value       = local.required_tags
}
