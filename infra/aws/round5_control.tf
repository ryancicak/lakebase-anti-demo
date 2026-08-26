data "aws_iam_policy_document" "round5_execution_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.round5_app_principal_arn]
    }
  }
}

# Keep the stable execution address for existing baselines. In the revised
# lifecycle this is the control-plane role; it never represents a bout.
resource "aws_iam_role" "round5_execution" {
  name_prefix        = "${local.round5_iam_stem}-exec-"
  description        = "App-assumed control role for journaled, API-created Round 5 bout add-ons"
  assume_role_policy = data.aws_iam_policy_document.round5_execution_assume.json

  tags = local.round5_iam_tags
}

data "aws_iam_policy_document" "round5_execution" {
  # IAM inline-role policy quotas count every non-whitespace byte. Statement
  # labels stay in this reviewed HCL; optional JSON Sid metadata is omitted so
  # exact resource and condition constraints retain safe quota headroom.
  statement {
    actions = ["ssm:SendCommand"]
    resources = [
      aws_instance.round5_runner.arn,
      "arn:${data.aws_partition.current.partition}:ssm:${var.aws_region}::document/AWS-RunShellScript",
    ]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.round5_proxy_service.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["rds.amazonaws.com"]
    }
  }

  statement {
    actions = [
      "iam:GetRole",
      "iam:GetRolePolicy",
    ]
    resources = [aws_iam_role.round5_proxy_service.arn]
  }

  statement {
    actions   = ["ec2:CreateSecurityGroup"]
    resources = ["arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${var.aws_account_id}:security-group/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  # CreateSecurityGroup authorizes both the security group being created and
  # its existing VPC. AWS omits VPC and request-tag context while evaluating
  # the new security-group resource, so the dependent VPC authorization is
  # what constrains creation to this exact network.
  statement {
    actions = ["ec2:CreateSecurityGroup"]
    resources = [
      "arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${var.aws_account_id}:vpc/${local.selected_vpc_id}",
    ]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    actions   = ["ec2:CreateTags"]
    resources = ["arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${var.aws_account_id}:security-group/*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:CreateAction"
      values   = ["CreateSecurityGroup"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/anti-demo-run-id"
      values   = [var.run_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/managed-by"
      values   = ["round5-lifecycle"]
    }

    dynamic "condition" {
      for_each = local.round5_ownership_tags
      content {
        test     = "StringEquals"
        variable = "aws:RequestTag/${condition.key}"
        values   = [condition.value]
      }
    }

    condition {
      test     = "Null"
      variable = "aws:RequestTag/anti-demo-bout-id"
      values   = ["false"]
    }
  }

  statement {
    actions = [
      "ec2:DeleteSecurityGroup",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupIngress",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${var.aws_account_id}:security-group/*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/anti-demo-run-id"
      values   = [var.run_id]
    }

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/managed-by"
      values   = ["round5-lifecycle"]
    }

    dynamic "condition" {
      for_each = local.round5_ownership_tags
      content {
        test     = "StringEquals"
        variable = "ec2:ResourceTag/${condition.key}"
        values   = [condition.value]
      }
    }

    condition {
      test     = "Null"
      variable = "ec2:ResourceTag/anti-demo-bout-id"
      values   = ["false"]
    }
  }

  statement {
    actions = [
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:AuthorizeSecurityGroupIngress",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${var.aws_account_id}:security-group/*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/anti-demo-run-id"
      values   = [var.run_id]
    }

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/managed-by"
      values   = ["round5-lifecycle"]
    }

    dynamic "condition" {
      for_each = local.round5_ownership_tags
      content {
        test     = "StringEquals"
        variable = "ec2:ResourceTag/${condition.key}"
        values   = [condition.value]
      }
    }

    condition {
      test     = "Null"
      variable = "ec2:ResourceTag/anti-demo-bout-id"
      values   = ["false"]
    }
  }

  statement {
    actions   = ["ec2:AuthorizeSecurityGroupEgress"]
    resources = [aws_security_group.round5_runner.arn]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    actions   = ["ec2:RevokeSecurityGroupEgress"]
    resources = [aws_security_group.round5_runner.arn]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    actions = ["ec2:AuthorizeSecurityGroupIngress"]
    resources = [
      local.round5_aurora_sg.arn,
      local.round5_rds_sg.arn,
    ]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    actions = ["ec2:RevokeSecurityGroupIngress"]
    resources = [
      local.round5_aurora_sg.arn,
      local.round5_rds_sg.arn,
    ]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  # AuthorizeSecurityGroup* also evaluates the not-yet-created rule resource.
  # Target security groups remain constrained by the statements above, while
  # the dependent CreateTags authorization below enforces the full bout tags.
  statement {
    actions = [
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:AuthorizeSecurityGroupIngress",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${var.aws_account_id}:security-group-rule/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    actions   = ["ec2:CreateTags"]
    resources = ["arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${var.aws_account_id}:security-group-rule/*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:CreateAction"
      values = [
        "AuthorizeSecurityGroupEgress",
        "AuthorizeSecurityGroupIngress",
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/anti-demo-run-id"
      values   = [var.run_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/managed-by"
      values   = ["round5-lifecycle"]
    }

    dynamic "condition" {
      for_each = local.round5_ownership_tags
      content {
        test     = "StringEquals"
        variable = "aws:RequestTag/${condition.key}"
        values   = [condition.value]
      }
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/Owner"
      values   = [trimspace(var.owner)]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/owner"
      values   = [trimspace(var.owner)]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/expires-at"
      values   = [var.expires_at]
    }

    condition {
      test     = "Null"
      variable = "aws:RequestTag/anti-demo-bout-id"
      values   = ["false"]
    }
  }

  statement {
    actions = [
      "rds:AddTagsToResource",
      "rds:CreateDBProxy",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/anti-demo-run-id"
      values   = [var.run_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/managed-by"
      values   = ["round5-lifecycle"]
    }

    dynamic "condition" {
      for_each = local.round5_ownership_tags
      content {
        test     = "StringEquals"
        variable = "aws:RequestTag/${condition.key}"
        values   = [condition.value]
      }
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/Owner"
      values   = [trimspace(var.owner)]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/owner"
      values   = [trimspace(var.owner)]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/expires-at"
      values   = [var.expires_at]
    }

    condition {
      test     = "Null"
      variable = "aws:RequestTag/anti-demo-bout-id"
      values   = ["false"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    actions = [
      "rds:DeleteDBProxy",
      "rds:DeregisterDBProxyTargets",
      "rds:ModifyDBProxy",
      "rds:ModifyDBProxyTargetGroup",
      "rds:RegisterDBProxyTargets",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/anti-demo-run-id"
      values   = [var.run_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/managed-by"
      values   = ["round5-lifecycle"]
    }

    dynamic "condition" {
      for_each = local.round5_ownership_tags
      content {
        test     = "StringEquals"
        variable = "aws:ResourceTag/${condition.key}"
        values   = [condition.value]
      }
    }

    condition {
      test     = "Null"
      variable = "aws:ResourceTag/anti-demo-bout-id"
      values   = ["false"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  # Registering and deregistering a target authorizes both the per-bout target
  # group and the selected Terraform-owned database. The statement above keeps
  # dynamic Proxy resources bout-tag-fenced; this one admits only the two exact
  # sealed baseline database ARNs, whose account and region are in the ARNs.
  statement {
    actions = [
      "rds:DeregisterDBProxyTargets",
      "rds:RegisterDBProxyTargets",
    ]
    resources = [
      local.round5_rds_instance.arn,
      local.round5_aurora_cluster.arn,
    ]
  }

  statement {
    actions = [
      "cloudwatch:GetMetricStatistics",
      "ec2:DescribeInstances",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSecurityGroupRules",
      "ec2:DescribeSubnets",
      "rds:DescribeDBClusters",
      "rds:DescribeDBInstances",
      "rds:DescribeDBSubnetGroups",
      "rds:DescribeDBProxies",
      "rds:DescribeDBProxyTargetGroups",
      "rds:DescribeDBProxyTargets",
      "rds:ListTagsForResource",
      "ssm:CancelCommand",
      "ssm:DescribeInstanceInformation",
      "ssm:GetCommandInvocation",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }
}

resource "aws_iam_role_policy" "round5_execution" {
  name_prefix = local.round5_execution_policy_prefix
  role        = aws_iam_role.round5_execution.id
  policy      = data.aws_iam_policy_document.round5_execution.json
}
