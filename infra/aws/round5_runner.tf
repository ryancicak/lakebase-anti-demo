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

  statement {
    sid = "ReadExactBaselineDatabaseMasterSecrets"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
    ]
    resources = [
      local.round5_rds_instance.master_user_secret[0].secret_arn,
      local.round5_aurora_cluster.master_user_secret[0].secret_arn,
    ]
  }
}

resource "aws_iam_policy" "round5_runner_boundary" {
  name_prefix = "${local.round5_iam_stem}-runner-boundary-"
  description = "Maximum permissions for the neutral runner, including exact baseline and journaled bout secret use"
  policy      = data.aws_iam_policy_document.round5_runner_boundary.json

  tags = local.round5_policy_tags

  # IAM rejects deletion of a customer-managed policy while it remains the
  # runner role's permissions boundary. Keep both uniquely prefixed policies
  # briefly so Terraform can repoint the exact role before deleting the old
  # boundary during any ForceNew migration (for example, a description edit).
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

resource "aws_iam_role_policy_attachment" "round5_runner_ssm" {
  role       = aws_iam_role.round5_runner.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "round5_runner_static_secrets" {
  statement {
    sid = "ReadExactBaselineDatabaseMasterSecrets"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
    ]
    resources = [
      local.round5_rds_instance.master_user_secret[0].secret_arn,
      local.round5_aurora_cluster.master_user_secret[0].secret_arn,
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

resource "aws_iam_role_policy" "round5_runner_baseline_secret" {
  name_prefix = local.round5_runner_secret_policy_prefix
  role        = aws_iam_role.round5_runner.id
  policy      = data.aws_iam_policy_document.round5_runner_static_secrets.json
}

resource "aws_iam_instance_profile" "round5_runner" {
  name_prefix = "${local.round5_iam_stem}-runner-"
  role        = aws_iam_role.round5_runner.name

  tags = local.round5_iam_tags
}

resource "aws_instance" "round5_runner" {
  ami                         = data.aws_ssm_parameter.round5_runner_ami.value
  instance_type               = "m6i.large"
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
