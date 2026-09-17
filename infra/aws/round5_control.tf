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
      aws_instance.round5_competitor_runner.arn,
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
    actions = [
      "sqs:GetQueueAttributes",
      "sqs:SendMessage",
    ]
    resources = [
      aws_sqs_queue.round5_lakebase_control.arn,
      aws_sqs_queue.round5_competitor_control.arn,
    ]
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
      "rds:DeleteDBProxy",
      "rds:ModifyDBProxy",
      # ModifyDBProxyTargetGroup / (De)RegisterDBProxyTargets authorize against
      # BOTH the target-group ARN (granted in the next statement) AND the parent
      # db-proxy ARN. Without the db-proxy grant here, configuring the pool after
      # CreateDBProxy fails at ModifyDBProxyTargetGroup with AccessDenied on the
      # db-proxy resource, which failed the competitor setup ~56s in. The same
      # bout-tag fencing below still scopes these to this bout's owned proxy.
      "rds:ModifyDBProxyTargetGroup",
      "rds:RegisterDBProxyTargets",
      "rds:DeregisterDBProxyTargets",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:rds:${var.aws_region}:${var.aws_account_id}:db-proxy:*"]

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

  statement {
    actions = [
      "rds:DeregisterDBProxyTargets",
      "rds:ModifyDBProxyTargetGroup",
      "rds:RegisterDBProxyTargets",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:rds:${var.aws_region}:${var.aws_account_id}:target-group:*"]

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

# The control role's full permission set exceeds the 10,240-character aggregate
# limit for inline role policies, so RDS Proxy provisioning and its tag-hijack
# guard are carried as an attached customer-managed policy instead (6,144-char
# limit, and it does not count against the inline aggregate). This mirrors the
# split the anti-demo-runtime role already uses. The union of permissions on the
# role is byte-for-byte the same set; only where they are stored changed.
data "aws_iam_policy_document" "round5_execution_proxy" {
  statement {
    actions   = ["rds:CreateDBProxy"]
    resources = ["arn:${data.aws_partition.current.partition}:rds:${var.aws_region}:${var.aws_account_id}:db-proxy:*"]

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

  # CreateDBProxy evaluates AddTagsToResource as a dependent action, and the
  # service creates its default target group without accepting tags. Keep that
  # unavoidable tagging permission off every other RDS resource type and admit
  # only the exact ownership-key set. The explicit deny below prevents changing
  # an already-owned foreign proxy or target group into one this run can mutate.
  statement {
    actions = ["rds:AddTagsToResource"]
    resources = [
      "arn:${data.aws_partition.current.partition}:rds:${var.aws_region}:${var.aws_account_id}:db-proxy:*",
      "arn:${data.aws_partition.current.partition}:rds:${var.aws_region}:${var.aws_account_id}:target-group:*",
    ]

    condition {
      test     = "ForAllValues:StringEquals"
      variable = "aws:TagKeys"
      values   = local.round5_bout_tag_keys
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

    condition {
      test     = "Null"
      variable = "aws:RequestTag/anti-demo:bout-token"
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

  dynamic "statement" {
    for_each = toset(local.round5_bout_tag_keys)
    content {
      effect  = "Deny"
      actions = ["rds:AddTagsToResource"]
      resources = [
        "arn:${data.aws_partition.current.partition}:rds:${var.aws_region}:${var.aws_account_id}:db-proxy:*",
        "arn:${data.aws_partition.current.partition}:rds:${var.aws_region}:${var.aws_account_id}:target-group:*",
      ]

      condition {
        test     = "Null"
        variable = "aws:ResourceTag/${statement.value}"
        values   = ["false"]
      }

      condition {
        test     = "StringNotEquals"
        variable = "aws:RequestTag/${statement.value}"
        values   = ["$${aws:ResourceTag/${statement.value}}"]
      }
    }
  }
}

resource "aws_iam_policy" "round5_execution_proxy" {
  name_prefix = "${local.round5_iam_stem}-exec-proxy-"
  description = "Round 5 control role: RDS Proxy provisioning and tag-hijack guard, split out to respect the inline policy size limit"
  policy      = data.aws_iam_policy_document.round5_execution_proxy.json

  tags = local.round5_policy_tags
}

resource "aws_iam_role_policy_attachment" "round5_execution_proxy" {
  role       = aws_iam_role.round5_execution.name
  policy_arn = aws_iam_policy.round5_execution_proxy.arn
}
