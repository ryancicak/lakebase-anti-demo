data "aws_vpc" "default" {
  count   = local.use_default_network ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = local.use_default_network ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_subnet" "explicit" {
  for_each = local.use_default_network ? toset([]) : toset(var.subnet_ids)
  id       = each.value
}

data "aws_subnet" "runner" {
  id = local.selected_runner_subnet_id
}

data "aws_route_tables" "runner_explicit" {
  vpc_id = local.selected_vpc_id

  filter {
    name   = "association.subnet-id"
    values = [local.selected_runner_subnet_id]
  }
}

data "aws_route_tables" "runner_main" {
  vpc_id = local.selected_vpc_id

  filter {
    name   = "association.main"
    values = ["true"]
  }
}

data "aws_route_table" "runner" {
  route_table_id = local.runner_route_table_id
}

resource "aws_db_subnet_group" "round1" {
  count = local.v7_enabled ? 0 : 1

  name        = "${local.resource_name}-db-subnets"
  description = "Lakebase Anti-Demo Round 1 database subnets"
  subnet_ids  = local.selected_subnet_ids
  tags        = local.required_tags

  lifecycle {
    precondition {
      condition     = length(local.selected_subnet_ids) >= 2
      error_message = "The selected network must provide at least two subnets."
    }

    precondition {
      condition = local.use_default_network || alltrue([
        for subnet in data.aws_subnet.explicit : subnet.vpc_id == var.vpc_id
      ])
      error_message = "Every explicit subnet must belong to vpc_id."
    }

    precondition {
      condition = local.use_default_network || length(distinct([
        for subnet in data.aws_subnet.explicit : subnet.availability_zone_id
      ])) >= 2
      error_message = "Explicit subnets must span at least two Availability Zones."
    }
  }
}

resource "aws_security_group" "aurora" {
  count = local.v7_enabled ? 0 : 1

  name_prefix            = "${local.resource_name}-aurora-"
  description            = "Aurora PostgreSQL ingress from operator and the neutral Round 5 runner"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  # Inline, and it must stay inline. An inline `ingress` block makes Terraform
  # authoritative over this group's entire rule set, so anything added by hand or
  # by another tool is revoked on the next apply. That property is what makes the
  # seal enforceable rather than advisory, and moving to standalone
  # aws_vpc_security_group_ingress_rule resources would quietly give it up.
  #
  # One block with several cidr_blocks, not several blocks. AWS groups
  # permissions by protocol and port range, so this renders as a single
  # IpPermission carrying several IpRanges -- which is the shape
  # server/lifecycle.py::_postgres_ingress_is_exact counts on.
  ingress {
    description = "PostgreSQL from the explicit operator IPv4 address and the published Databricks serverless egress prefixes"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = concat([var.operator_cidr], var.serverless_egress_cidrs)
  }

  ingress {
    description = "Round 5 direct observer plus static Aurora Proxy path"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    security_groups = [
      aws_security_group.round5_competitor_runner.id,
      aws_security_group.round5_proxy["aurora"].id,
    ]
  }

  egress {
    description = "Stateful response and AWS service traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.required_tags

  # AWS does not allow Terraform to replace an attached RDS security group by
  # detaching its managed ENI. Existing runs keep their legacy description;
  # new runs receive the current one. Rules and tags remain fully managed.
  lifecycle {
    ignore_changes = [description]
  }
}

resource "aws_security_group" "rds_control_plane_only" {
  count = local.v7_enabled ? 0 : 1

  name_prefix            = "${local.resource_name}-rds-"
  description            = "RDS PostgreSQL ingress from operator and the neutral Round 5 runner"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  # Inline, and it must stay inline. An inline `ingress` block makes Terraform
  # authoritative over this group's entire rule set, so anything added by hand or
  # by another tool is revoked on the next apply. That property is what makes the
  # seal enforceable rather than advisory, and moving to standalone
  # aws_vpc_security_group_ingress_rule resources would quietly give it up.
  #
  # One block with several cidr_blocks, not several blocks. AWS groups
  # permissions by protocol and port range, so this renders as a single
  # IpPermission carrying several IpRanges -- which is the shape
  # server/lifecycle.py::_postgres_ingress_is_exact counts on.
  ingress {
    description = "PostgreSQL from the explicit operator IPv4 address and the published Databricks serverless egress prefixes"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = concat([var.operator_cidr], var.serverless_egress_cidrs)
  }

  ingress {
    description = "Round 5 direct observer plus static RDS Proxy path"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    security_groups = [
      aws_security_group.round5_competitor_runner.id,
      aws_security_group.round5_proxy["rds"].id,
    ]
  }

  egress {
    description = "Stateful response and AWS service traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.required_tags

  # AWS does not allow Terraform to replace an attached RDS security group by
  # detaching its managed ENI. Existing runs keep their legacy description;
  # new runs receive the current one. Rules and tags remain fully managed.
  lifecycle {
    ignore_changes = [description]
  }
}

resource "aws_security_group" "round5_runner" {
  name_prefix            = "${local.round5_resource_name}-runner-"
  description            = "Round 5 Lakebase runner: outbound only, with no ingress rules"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "lakebase"
  })

  # This group is attached to the running runner instance's ENI, and AWS refuses
  # to replace an attached security group. A description ForceNew would try to
  # replace it anyway -- and because the execution role, the runner boundary and
  # the database groups all cross-reference it, that replacement forms an apply
  # graph cycle. Keep the sealed group in place; rules and tags stay managed.
  # Same rationale as the aurora/rds groups above.
  lifecycle {
    ignore_changes = [description]
  }
}

resource "aws_security_group" "round5_competitor_runner" {
  name_prefix            = "${local.round5_resource_name}-competitor-runner-"
  description            = "Round 5 competitor runner: outbound only, with no ingress rules"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
  })

  # Same ENI-attachment constraint as the Lakebase runner group above.
  lifecycle {
    ignore_changes = [description]
  }
}

# Public AWS APIs and the install-time package/CA fetches all use TLS. This is
# intentionally narrower than the former all-protocol rule, but it is not a
# destination allowlist: the public Lakebase path and package bootstrap still
# require an egress architecture before TCP/443 can be made private.
resource "aws_vpc_security_group_egress_rule" "round5_lakebase_runner_https" {
  security_group_id = aws_security_group.round5_runner.id
  description       = "HTTPS for SSM, SQS, Secrets Manager, package installation, and CA refresh"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "lakebase"
  })
}

resource "aws_vpc_security_group_egress_rule" "round5_competitor_runner_https" {
  security_group_id = aws_security_group.round5_competitor_runner.id
  description       = "HTTPS for SSM, SQS, Secrets Manager, package installation, and CA refresh"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
  })
}

# Lakebase is publicly reachable and does not publish a stable customer-specific
# CIDR that a security-group rule can seal. Limit this lane's exception to
# PostgreSQL; the competitor lane has its own coordination-only rule below.
resource "aws_vpc_security_group_egress_rule" "round5_lakebase_runner_postgres" {
  security_group_id = aws_security_group.round5_runner.id
  description       = "PostgreSQL to the public Lakebase direct, pooled, and coordination endpoints"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  cidr_ipv4         = "0.0.0.0/0"

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "lakebase"
  })
}

# The v4 two-runner control plane requires BOTH residents to write their
# durable control events (agent_ready, heartbeat, progress, result, settled) to
# the public Lakebase *coordination* endpoint over PostgreSQL. That endpoint is
# the same publicly reachable Lakebase host the lakebase runner reaches above,
# and Lakebase still publishes no stable customer-specific CIDR a rule can seal,
# so this mirrors the lakebase runner's exception rather than an allowlist. Its
# absence is why the competitor lane could reach SQS/Secrets over 443 but timed
# out on 5432 to the coordination DB and never reached agent_ready -- so the ring
# never became ready even though the lakebase lane was healthy. Row-level
# security on round5_runner_event_v3 still fences every write to the lane's own
# generation and current warm attempt; opening egress does not weaken it.
resource "aws_vpc_security_group_egress_rule" "round5_competitor_runner_postgres" {
  security_group_id = aws_security_group.round5_competitor_runner.id
  description       = "PostgreSQL to the public Lakebase coordination endpoint (resident control events)"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  cidr_ipv4         = "0.0.0.0/0"

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
  })
}

# Stable least-privilege Proxy network fixtures. The per-bout Proxy is still
# created after the bell; only its immutable network envelope stands warm.
resource "aws_security_group" "round5_proxy" {
  for_each = toset(["aurora", "rds"])

  name_prefix            = "${local.round5_resource_name}-${each.key}-proxy-"
  description            = "Static Round 5 ${each.key} Proxy network fixture"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  tags = merge(local.round5_required_tags, {
    "anti-demo-warm-fixture" = "proxy-network"
    "anti-demo-variant"      = each.key
  })
}

resource "aws_vpc_security_group_ingress_rule" "round5_runner_to_proxy" {
  for_each = aws_security_group.round5_proxy

  security_group_id = each.value.id
  # This description is part of the app's exact Proxy topology gate
  # (`_verify_proxy_topology` network_ingress), which matches the ingress tuple
  # byte-for-byte. It must read exactly "PostgreSQL from the sealed Round 5
  # physical runners" or the gate fails after the ~11-min Proxy build with
  # "Round 5 exact Proxy control gate failed: network_ingress". The referenced
  # group is still only the competitor runner (the sole lane that connects to
  # the per-bout Proxy).
  description                  = "PostgreSQL from the sealed Round 5 physical runners"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.round5_competitor_runner.id

  tags = merge(local.round5_required_tags, {
    "anti-demo-warm-fixture" = "proxy-network"
    "anti-demo-variant"      = each.key
  })
}

resource "aws_vpc_security_group_egress_rule" "round5_competitor_runner_to_proxy" {
  for_each = aws_security_group.round5_proxy

  security_group_id            = aws_security_group.round5_competitor_runner.id
  description                  = "PostgreSQL to the sealed Round 5 ${each.key} Proxy fixture"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = each.value.id

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
    "anti-demo-variant"     = each.key
  })
}

resource "aws_vpc_security_group_egress_rule" "round5_competitor_runner_to_database" {
  for_each = {
    aurora = local.round5_aurora_sg.id
    rds    = local.round5_rds_sg.id
  }

  security_group_id            = aws_security_group.round5_competitor_runner.id
  description                  = "PostgreSQL observer and cleanup path to the sealed Round 5 ${each.key} source"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = each.value

  tags = merge(local.round5_required_tags, {
    "anti-demo-runner-lane" = "competitor"
    "anti-demo-variant"     = each.key
  })
}

resource "aws_vpc_security_group_egress_rule" "round5_proxy_to_database" {
  for_each = aws_security_group.round5_proxy

  security_group_id = each.value.id
  description       = "PostgreSQL to the exact sealed Round 5 source"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  referenced_security_group_id = (
    each.key == "aurora"
    ? local.round5_aurora_sg.id
    : local.round5_rds_sg.id
  )

  tags = merge(local.round5_required_tags, {
    "anti-demo-warm-fixture" = "proxy-network"
    "anti-demo-variant"      = each.key
  })
}

resource "aws_db_subnet_group" "by_round" {
  for_each = local.v7_rounds

  name        = "${local.v7_round_resource_names[each.key]}-db-subnets"
  description = "Lakebase Anti-Demo ${upper(each.key)} isolated database subnets"
  subnet_ids  = local.selected_subnet_ids
  tags        = local.v7_round_tags[each.key]

  lifecycle {
    precondition {
      condition     = length(local.selected_subnet_ids) >= 2
      error_message = "The selected network must provide at least two subnets."
    }

    precondition {
      condition = local.use_default_network || alltrue([
        for subnet in data.aws_subnet.explicit : subnet.vpc_id == var.vpc_id
      ])
      error_message = "Every explicit subnet must belong to vpc_id."
    }

    precondition {
      condition = local.use_default_network || length(distinct([
        for subnet in data.aws_subnet.explicit : subnet.availability_zone_id
      ])) >= 2
      error_message = "Explicit subnets must span at least two Availability Zones."
    }
  }
}

resource "aws_security_group" "aurora_by_round" {
  for_each = local.v7_rounds

  name_prefix            = "${local.v7_round_resource_names[each.key]}-aurora-"
  description            = "${upper(each.key)} isolated Aurora PostgreSQL ingress"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  # Inline, and it must stay inline. An inline `ingress` block makes Terraform
  # authoritative over this group's entire rule set, so anything added by hand or
  # by another tool is revoked on the next apply. That property is what makes the
  # seal enforceable rather than advisory, and moving to standalone
  # aws_vpc_security_group_ingress_rule resources would quietly give it up.
  #
  # One block with several cidr_blocks, not several blocks. AWS groups
  # permissions by protocol and port range, so this renders as a single
  # IpPermission carrying several IpRanges -- which is the shape
  # server/lifecycle.py::_postgres_ingress_is_exact counts on.
  ingress {
    description = "PostgreSQL from the explicit operator IPv4 address and the published Databricks serverless egress prefixes"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = concat([var.operator_cidr], var.serverless_egress_cidrs)
  }

  dynamic "ingress" {
    for_each = each.key == "r5" ? [true] : []
    content {
      description = "Direct PostgreSQL observer and cleanup control path from the Round 5 runner"
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      security_groups = [
        aws_security_group.round5_competitor_runner.id,
        aws_security_group.round5_proxy["aurora"].id,
      ]
    }
  }

  egress {
    description = "Stateful response and AWS service traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.v7_round_tags[each.key]
}

resource "aws_security_group" "rds_by_round" {
  # A security group with no instance behind it is free but misleading, and it
  # would keep the r1 RDS lane looking provisioned in every describe. Scoped to
  # the rounds that actually stand an instance up.
  for_each = local.v7_rds_rounds

  name_prefix            = "${local.v7_round_resource_names[each.key]}-rds-"
  description            = "${upper(each.key)} isolated RDS PostgreSQL ingress"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  # Inline, and it must stay inline. An inline `ingress` block makes Terraform
  # authoritative over this group's entire rule set, so anything added by hand or
  # by another tool is revoked on the next apply. That property is what makes the
  # seal enforceable rather than advisory, and moving to standalone
  # aws_vpc_security_group_ingress_rule resources would quietly give it up.
  #
  # One block with several cidr_blocks, not several blocks. AWS groups
  # permissions by protocol and port range, so this renders as a single
  # IpPermission carrying several IpRanges -- which is the shape
  # server/lifecycle.py::_postgres_ingress_is_exact counts on.
  ingress {
    description = "PostgreSQL from the explicit operator IPv4 address and the published Databricks serverless egress prefixes"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = concat([var.operator_cidr], var.serverless_egress_cidrs)
  }

  dynamic "ingress" {
    for_each = each.key == "r5" ? [true] : []
    content {
      description = "Direct PostgreSQL observer and cleanup control path from the Round 5 runner"
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      security_groups = [
        aws_security_group.round5_competitor_runner.id,
        aws_security_group.round5_proxy["rds"].id,
      ]
    }
  }

  egress {
    description = "Stateful response and AWS service traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.v7_round_tags[each.key]
}
