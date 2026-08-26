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
    description     = "Direct PostgreSQL observer and cleanup control path from the Round 5 runner"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.round5_runner.id]
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
    description     = "Direct PostgreSQL observer and cleanup control path from the Round 5 runner"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.round5_runner.id]
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
  description            = "Round 5 neutral runner: outbound only, with no ingress rules"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  tags = local.round5_required_tags
}

resource "aws_vpc_security_group_egress_rule" "round5_runner_outbound" {
  security_group_id = aws_security_group.round5_runner.id
  description       = "Outbound access for SSM, package installation, Lakebase, and RDS Proxy"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"

  tags = local.round5_required_tags
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
      description     = "Direct PostgreSQL observer and cleanup control path from the Round 5 runner"
      from_port       = 5432
      to_port         = 5432
      protocol        = "tcp"
      security_groups = [aws_security_group.round5_runner.id]
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
      description     = "Direct PostgreSQL observer and cleanup control path from the Round 5 runner"
      from_port       = 5432
      to_port         = 5432
      protocol        = "tcp"
      security_groups = [aws_security_group.round5_runner.id]
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
