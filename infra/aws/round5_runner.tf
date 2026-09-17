data "aws_partition" "current" {}

data "aws_ssm_parameter" "round5_runner_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_iam_policy_document" "round5_runner_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "round5_runner_boundary" {
  statement {
    sid = "ManagedInstanceCore"
    actions = [
      "ec2messages:AcknowledgeMessage",
      "ec2messages:DeleteMessage",
      "ec2messages:FailMessage",
      "ec2messages:GetEndpoint",
      "ec2messages:GetMessages",
      "ec2messages:SendReply",
      "ssm:DescribeAssociation",
      "ssm:DescribeDocument",
      "ssm:GetDeployablePatchSnapshotForInstance",
      "ssm:GetDocument",
      "ssm:GetManifest",
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:ListAssociations",
      "ssm:ListInstanceAssociations",
      "ssm:PutComplianceItems",
      "ssm:PutConfigurePackageResult",
      "ssm:PutInventory",
      "ssm:UpdateAssociationStatus",
      "ssm:UpdateInstanceAssociationStatus",
      "ssm:UpdateInstanceInformation",
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }

  statement {
    sid = "ReadExactLakebaseResidentControlSecret"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
    ]
    resources = [aws_secretsmanager_secret.round5_runner_control.arn]
  }

  statement {
    sid = "ConsumeExactLakebaseResidentControlQueue"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.round5_lakebase_control.arn]
  }
}

data "aws_iam_policy_document" "round5_competitor_runner_boundary" {
  statement {
    sid = "ManagedInstanceCore"
    actions = [
      "ec2messages:AcknowledgeMessage",
      "ec2messages:DeleteMessage",
      "ec2messages:FailMessage",
      "ec2messages:GetEndpoint",
      "ec2messages:GetMessages",
      "ec2messages:SendReply",
      "ssm:DescribeAssociation",
      "ssm:DescribeDocument",
      "ssm:GetDeployablePatchSnapshotForInstance",
      "ssm:GetDocument",
      "ssm:GetManifest",
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:ListAssociations",
      "ssm:ListInstanceAssociations",
      "ssm:PutComplianceItems",
      "ssm:PutConfigurePackageResult",
      "ssm:PutInventory",
      "ssm:UpdateAssociationStatus",
      "ssm:UpdateInstanceAssociationStatus",
      "ssm:UpdateInstanceInformation",
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }

  statement {
    sid = "ReadExactCompetitorDatabaseAndEventSecrets"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
    ]
    resources = [
      local.round5_rds_instance.master_user_secret[0].secret_arn,
      local.round5_aurora_cluster.master_user_secret[0].secret_arn,
      aws_secretsmanager_secret.round5_competitor_runner_control.arn,
    ]
  }

  statement {
    sid = "UseExactCompetitorProxyCredentialSecrets"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
      "secretsmanager:PutSecretValue",
    ]
    resources = [
      aws_secretsmanager_secret.round5_aurora_proxy_credentials.arn,
      aws_secretsmanager_secret.round5_rds_proxy_credentials.arn,
    ]
  }

  statement {
    sid = "ConsumeExactCompetitorResidentControlQueue"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.round5_competitor_control.arn]
  }
}

resource "aws_iam_policy" "round5_runner_boundary" {
  name_prefix = "${local.round5_iam_stem}-runner-boundary-"
  description = "Maximum permissions for the Lakebase runner: SSM, its FIFO queue, and its lane-scoped event DSN"
  policy      = data.aws_iam_policy_document.round5_runner_boundary.json

  tags = local.round5_policy_tags

  # IAM rejects deletion of a customer-managed policy while it remains the
  # runner role's permissions boundary. Keep both uniquely prefixed policies
  # briefly so Terraform can repoint the exact role before deleting the old
  # boundary during any ForceNew migration (for example, a description edit).
  #
  # The policy *document* (its permissions) still updates in place; only the
  # description is a ForceNew trigger, and replacing this existing boundary while
  # the execution role, runner role and database groups all reference it forms an
  # apply graph cycle. Hold the sealed description so the boundary is never
  # replaced; the permission set below is what actually matters and is unmanaged
  # by this ignore.
  lifecycle {
    create_before_destroy = true
    ignore_changes        = [description]
  }
}

resource "aws_iam_policy" "round5_competitor_runner_boundary" {
  name_prefix = "${local.round5_iam_stem}-competitor-boundary-"
  description = "Maximum permissions for the competitor runner: SSM, its FIFO queue, and exact competitor secrets"
  policy      = data.aws_iam_policy_document.round5_competitor_runner_boundary.json

  tags = local.round5_policy_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role" "round5_runner" {
  name_prefix          = "${local.round5_iam_stem}-runner-"
  description          = "Neutral SSM runner with fixed access to the baseline and Proxy credential secrets"
  assume_role_policy   = data.aws_iam_policy_document.round5_runner_assume.json
  permissions_boundary = aws_iam_policy.round5_runner_boundary.arn

  tags = local.round5_iam_tags
}

resource "aws_iam_role" "round5_competitor_runner" {
  # The 24-char v7 IAM stem leaves 14 characters under the 38-char cap that
  # AWS enforces on aws_iam_role / aws_iam_instance_profile name_prefix values.
  # "-competitor-runner-" (19) overflows it, so the competitor identity uses the
  # shorter "-comp-" here and on its instance profile below. Ownership is carried
  # by tags and the sealed ARN, not by this human-readable stem.
  name_prefix          = "${local.round5_iam_stem}-comp-"
  description          = "Neutral competitor resident runner with its own FIFO consumer identity"
  assume_role_policy   = data.aws_iam_policy_document.round5_runner_assume.json
  permissions_boundary = aws_iam_policy.round5_competitor_runner_boundary.arn

  tags = local.round5_iam_tags
}

resource "aws_iam_role_policy_attachment" "round5_runner_ssm" {
  role       = aws_iam_role.round5_runner.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "round5_competitor_runner_ssm" {
  role       = aws_iam_role.round5_competitor_runner.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "round5_lakebase_runner_static_secrets" {
  statement {
    sid = "ReadExactResidentControlSecret"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
    ]
    resources = [aws_secretsmanager_secret.round5_runner_control.arn]
  }
}

data "aws_iam_policy_document" "round5_competitor_runner_static_secrets" {
  statement {
    sid = "ReadExactBaselineDatabaseMasterSecrets"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
    ]
    resources = [
      local.round5_rds_instance.master_user_secret[0].secret_arn,
      local.round5_aurora_cluster.master_user_secret[0].secret_arn,
      aws_secretsmanager_secret.round5_competitor_runner_control.arn,
    ]
  }

  statement {
    sid = "UseExactProxyCredentialSecrets"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
      "secretsmanager:PutSecretValue",
    ]
    resources = [
      aws_secretsmanager_secret.round5_aurora_proxy_credentials.arn,
      aws_secretsmanager_secret.round5_rds_proxy_credentials.arn,
    ]
  }

}

resource "aws_secretsmanager_secret" "round5_runner_control" {
  name_prefix             = "${local.round5_iam_stem}-runner-control-"
  description             = "Lane-scoped PostgreSQL event DSN for the resident Round 5 Lakebase agent"
  recovery_window_in_days = 0
  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "lakebase"
  })
}

resource "aws_secretsmanager_secret" "round5_competitor_runner_control" {
  name_prefix             = "${local.round5_iam_stem}-competitor-control-"
  description             = "Lane-scoped PostgreSQL event DSN for the resident Round 5 competitor agent"
  recovery_window_in_days = 0
  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
  })
}

resource "aws_sqs_queue" "round5_lakebase_control" {
  name                        = "${local.round5_iam_stem}-lakebase-control.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  receive_wait_time_seconds   = 20
  visibility_timeout_seconds  = 720
  message_retention_seconds   = 86400
  sqs_managed_sse_enabled     = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.round5_lakebase_control_dlq.arn
    maxReceiveCount     = 3
  })

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "lakebase"
  })
}

resource "aws_sqs_queue" "round5_competitor_control" {
  name                        = "${local.round5_iam_stem}-competitor-control.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  receive_wait_time_seconds   = 20
  visibility_timeout_seconds  = 720
  message_retention_seconds   = 86400
  sqs_managed_sse_enabled     = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.round5_competitor_control_dlq.arn
    maxReceiveCount     = 3
  })

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
  })
}

resource "aws_sqs_queue" "round5_lakebase_control_dlq" {
  name                      = "${local.round5_iam_stem}-lakebase-control-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns = [
      "arn:${data.aws_partition.current.partition}:sqs:${var.aws_region}:${var.aws_account_id}:${local.round5_iam_stem}-lakebase-control.fifo"
    ]
  })

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "lakebase"
    "anti-demo-queue-role"  = "dead-letter"
  })
}

resource "aws_sqs_queue" "round5_competitor_control_dlq" {
  name                      = "${local.round5_iam_stem}-competitor-control-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns = [
      "arn:${data.aws_partition.current.partition}:sqs:${var.aws_region}:${var.aws_account_id}:${local.round5_iam_stem}-competitor-control.fifo"
    ]
  })

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
    "anti-demo-queue-role"  = "dead-letter"
  })
}

resource "aws_iam_role_policy" "round5_runner_baseline_secret" {
  name_prefix = local.round5_runner_secret_policy_prefix
  role        = aws_iam_role.round5_runner.id
  policy      = data.aws_iam_policy_document.round5_lakebase_runner_static_secrets.json
}

resource "aws_iam_role_policy" "round5_competitor_runner_baseline_secret" {
  name_prefix = local.round5_runner_secret_policy_prefix
  role        = aws_iam_role.round5_competitor_runner.id
  policy      = data.aws_iam_policy_document.round5_competitor_runner_static_secrets.json
}

data "aws_iam_policy_document" "round5_lakebase_runner_control" {
  statement {
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.round5_lakebase_control.arn]
  }
}

data "aws_iam_policy_document" "round5_competitor_runner_control" {
  statement {
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
    ]
    resources = [aws_sqs_queue.round5_competitor_control.arn]
  }
}

resource "aws_iam_role_policy" "round5_lakebase_runner_control" {
  name_prefix = "${local.round5_iam_stem}-lakebase-control-"
  role        = aws_iam_role.round5_runner.id
  policy      = data.aws_iam_policy_document.round5_lakebase_runner_control.json
}

resource "aws_iam_role_policy" "round5_competitor_runner_control" {
  name_prefix = "${local.round5_iam_stem}-competitor-control-"
  role        = aws_iam_role.round5_competitor_runner.id
  policy      = data.aws_iam_policy_document.round5_competitor_runner_control.json
}

resource "aws_iam_instance_profile" "round5_runner" {
  name_prefix = "${local.round5_iam_stem}-runner-"
  role        = aws_iam_role.round5_runner.name

  tags = local.round5_iam_tags
}

resource "aws_iam_instance_profile" "round5_competitor_runner" {
  # See the competitor role above: "-comp-" keeps the name_prefix within the
  # 38-char IAM cap for the fixed 24-char v7 stem.
  name_prefix = "${local.round5_iam_stem}-comp-"
  role        = aws_iam_role.round5_competitor_runner.name

  tags = local.round5_iam_tags
}

resource "aws_instance" "round5_runner" {
  ami                         = data.aws_ssm_parameter.round5_runner_ami.value
  instance_type               = var.round5_runner_instance_type
  subnet_id                   = local.selected_runner_subnet_id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.round5_runner.id]
  iam_instance_profile        = aws_iam_instance_profile.round5_runner.name
  ebs_optimized               = true
  monitoring                  = false

  metadata_options {
    http_endpoint               = "enabled"
    http_protocol_ipv6          = "disabled"
    http_put_response_hop_limit = 1
    http_tokens                 = "required"
    instance_metadata_tags      = "enabled"
  }

  root_block_device {
    delete_on_termination = true
    encrypted             = true
    volume_size           = 20
    volume_type           = "gp3"
    tags                  = local.round5_required_tags
  }

  tags = local.round5_required_tags

  lifecycle {
    precondition {
      condition     = contains(local.selected_subnet_ids, local.selected_runner_subnet_id)
      error_message = "runner_subnet_id must be one of the selected database subnet_ids."
    }

    precondition {
      condition     = data.aws_subnet.runner.vpc_id == local.selected_vpc_id
      error_message = "The Round 5 runner subnet must belong to the selected VPC."
    }

    precondition {
      condition = anytrue([
        for route in data.aws_route_table.runner.routes :
        route.cidr_block == "0.0.0.0/0" && can(regex("^igw-", route.gateway_id))
      ])
      error_message = "The Round 5 runner subnet must have an effective 0.0.0.0/0 route directly to an Internet Gateway."
    }
  }
}

# A fan-in lane owns a physical machine, not merely a task on the same machine.
# Keeping the original address as the Lakebase runner avoids replacing a
# working sealed instance during migration; this second instance is dedicated
# to the selected Aurora/RDS lane and has its own flock, job registry, CPU, and
# cancellation boundary.
resource "aws_instance" "round5_competitor_runner" {
  ami                         = data.aws_ssm_parameter.round5_runner_ami.value
  instance_type               = var.round5_runner_instance_type
  subnet_id                   = local.selected_runner_subnet_id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.round5_competitor_runner.id]
  iam_instance_profile        = aws_iam_instance_profile.round5_competitor_runner.name
  ebs_optimized               = true
  monitoring                  = false

  metadata_options {
    http_endpoint               = "enabled"
    http_protocol_ipv6          = "disabled"
    http_put_response_hop_limit = 1
    http_tokens                 = "required"
    instance_metadata_tags      = "enabled"
  }

  root_block_device {
    delete_on_termination = true
    encrypted             = true
    volume_size           = 20
    volume_type           = "gp3"
    tags                  = local.round5_required_tags
  }

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
  })

  lifecycle {
    precondition {
      condition     = contains(local.selected_subnet_ids, local.selected_runner_subnet_id)
      error_message = "runner_subnet_id must be one of the selected database subnet_ids."
    }

    precondition {
      condition     = data.aws_subnet.runner.vpc_id == local.selected_vpc_id
      error_message = "The Round 5 runner subnet must belong to the selected VPC."
    }

    precondition {
      condition = anytrue([
        for route in data.aws_route_table.runner.routes :
        route.cidr_block == "0.0.0.0/0" && can(regex("^igw-", route.gateway_id))
      ])
      error_message = "The Round 5 runner subnet must have an effective 0.0.0.0/0 route directly to an Internet Gateway."
    }
  }
}
