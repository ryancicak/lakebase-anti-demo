resource "aws_secretsmanager_secret" "round5_aurora_proxy_credentials" {
  name                    = "${local.round5_resource_name}/round5/proxy/aurora"
  description             = "Credential container for the dedicated anti_demo_burst Aurora Proxy user"
  recovery_window_in_days = 0

  tags = local.round5_required_tags
}

resource "aws_secretsmanager_secret" "round5_rds_proxy_credentials" {
  name                    = "${local.round5_resource_name}/round5/proxy/rds"
  description             = "Credential container for the dedicated anti_demo_burst RDS Proxy user"
  recovery_window_in_days = 0

  tags = local.round5_required_tags
}

data "aws_iam_policy_document" "round5_proxy_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["rds.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "round5_proxy_service" {
  name_prefix        = "${local.round5_iam_stem}-proxy-"
  description        = "Terraform-owned service role for the Round 5 Aurora and RDS Proxies"
  assume_role_policy = data.aws_iam_policy_document.round5_proxy_assume.json

  tags = local.round5_iam_tags
}

data "aws_iam_policy_document" "round5_proxy_secrets" {
  statement {
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue",
    ]
    resources = [
      aws_secretsmanager_secret.round5_aurora_proxy_credentials.arn,
      aws_secretsmanager_secret.round5_rds_proxy_credentials.arn,
    ]
  }
}

resource "aws_iam_role_policy" "round5_proxy_secrets" {
  name_prefix = local.round5_proxy_secret_policy_prefix
  role        = aws_iam_role.round5_proxy_service.id
  policy      = data.aws_iam_policy_document.round5_proxy_secrets.json
}
