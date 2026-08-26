variable "aws_region" {
  description = "AWS region in which to create the disposable Round 1 resources."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(?:-gov)?-[a-z]+-[0-9]+$", var.aws_region))
    error_message = "aws_region must be a valid AWS region name, for example us-east-1."
  }
}

variable "aws_account_id" {
  description = "Exact disposable AWS account allowed for this run. Provider calls fail in any other account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be exactly 12 digits."
  }
}

variable "run_id" {
  description = "Unique lowercase identifier for this owned demo run. Used in names and ownership tags."
  type        = string

  validation {
    condition = (
      length(var.run_id) >= 3 &&
      length(var.run_id) <= 24 &&
      !strcontains(var.run_id, "--") &&
      can(regex("^[a-z0-9](?:[a-z0-9-]*[a-z0-9])$", var.run_id))
    )
    error_message = "run_id must be 3-24 lowercase letters, numbers, or hyphens; it cannot begin or end with a hyphen or contain consecutive hyphens."
  }
}

variable "installation_id" {
  description = "Stable unique ID for a fresh v7 installation. When null, the legacy v6 single-database-pair layout is preserved."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.installation_id == null ? true : (
      length(trimspace(var.installation_id)) >= 8 &&
      length(var.installation_id) <= 256
    )
    error_message = "installation_id must be null or a stable non-empty identifier between 8 and 256 characters."
  }
}

variable "owner" {
  description = "Human owner recorded on every Terraform-managed AWS resource."
  type        = string

  validation {
    condition     = length(trimspace(var.owner)) >= 3 && length(var.owner) <= 128
    error_message = "owner must be a non-empty value between 3 and 128 characters."
  }
}

variable "expires_at" {
  description = "RFC 3339 expiration timestamp recorded on every Terraform-managed AWS resource."
  type        = string

  validation {
    condition     = can(timecmp(var.expires_at, "1970-01-01T00:00:00Z"))
    error_message = "expires_at must be an RFC 3339 timestamp, for example 2026-08-20T18:00:00Z."
  }
}

variable "operator_cidr" {
  description = "Explicit public IPv4 /32 allowed to connect to the disposable AWS PostgreSQL resources. No broader CIDR is accepted."
  type        = string

  validation {
    condition = (
      can(cidrnetmask(var.operator_cidr)) &&
      can(regex("/32$", var.operator_cidr))
    )
    error_message = "operator_cidr must be an explicit IPv4 /32, for example 203.0.113.10/32."
  }
}

variable "serverless_egress_cidrs" {
  description = "Published Databricks serverless outbound prefixes for this region, admitted to the database security groups alongside operator_cidr so the deployed app can race the AWS lanes. Empty admits only the operator, which is what every installation sealed before this existed does. Never written here: the values are globally routable and the repository refuses routable IPv4 literals, so server/lifecycle.py fetches them at reconcile time."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition = alltrue([
      for cidr in var.serverless_egress_cidrs :
      can(cidrnetmask(cidr)) && tonumber(split("/", cidr)[1]) >= 24
    ]) && length(distinct(var.serverless_egress_cidrs)) == length(var.serverless_egress_cidrs)
    error_message = "serverless_egress_cidrs must be distinct IPv4 CIDRs of /24 or narrower. A /16 in front of a live database is refused here as it is in server/manifest.py."
  }
}

variable "name_prefix" {
  description = "Lowercase prefix for AWS resource identifiers."
  type        = string
  default     = "lakebase-anti-demo"

  validation {
    condition = (
      length(var.name_prefix) >= 3 &&
      length(var.name_prefix) <= 24 &&
      !strcontains(var.name_prefix, "--") &&
      can(regex("^[a-z](?:[a-z0-9-]*[a-z0-9])$", var.name_prefix))
    )
    error_message = "name_prefix must be 3-24 lowercase letters, numbers, or hyphens, start with a letter, and cannot end with a hyphen or contain consecutive hyphens."
  }
}

variable "vpc_id" {
  description = "Explicit VPC ID. Set together with subnet_ids, or leave both null to use the default VPC."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.vpc_id == null || can(regex("^vpc-[0-9a-f]+$", var.vpc_id))
    error_message = "vpc_id must be null or a valid VPC ID."
  }
}

variable "subnet_ids" {
  description = "At least two subnet IDs in distinct Availability Zones. Set together with vpc_id."
  type        = list(string)
  default     = null
  nullable    = true

  validation {
    condition = var.subnet_ids == null || (
      length(var.subnet_ids) >= 2 &&
      length(distinct(var.subnet_ids)) == length(var.subnet_ids) &&
      alltrue([for subnet_id in var.subnet_ids : can(regex("^subnet-[0-9a-f]+$", subnet_id))])
    )
    error_message = "subnet_ids must be null or contain at least two distinct valid subnet IDs."
  }
}

variable "runner_subnet_id" {
  description = "Explicit public-routed subnet for the neutral Round 5 runner. Required with explicit vpc_id/subnet_ids; leave null in default-VPC mode."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.runner_subnet_id == null || can(regex("^subnet-[0-9a-f]+$", var.runner_subnet_id))
    error_message = "runner_subnet_id must be null or a valid subnet ID."
  }
}

variable "round5_app_principal_arn" {
  description = "Exact IAM role or user ARN allowed to assume the least-privilege Round 5 execution role."
  type        = string

  validation {
    condition = can(regex(
      "^arn:(?:aws|aws-us-gov|aws-cn):iam::[0-9]{12}:(?:role|user)/[A-Za-z0-9+=,.@_/-]+$",
      var.round5_app_principal_arn,
    ))
    error_message = "round5_app_principal_arn must be an exact IAM role or user ARN."
  }
}

variable "anti_demo_runtime_principal_arns" {
  description = "Every IAM principal allowed to assume the sealed anti-demo-runtime role, typically the operator's Identity Center role and the deployed app's IAM user. Empty leaves the role uncreated, which is what every installation sealed before this existed does."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition = alltrue([
      for arn in var.anti_demo_runtime_principal_arns : can(regex(
        "^arn:(?:aws|aws-us-gov|aws-cn):iam::[0-9]{12}:(?:role|user)/[A-Za-z0-9+=,.@_/-]+$",
        arn,
      ))
    ]) && length(distinct(var.anti_demo_runtime_principal_arns)) == length(var.anti_demo_runtime_principal_arns)
    error_message = "anti_demo_runtime_principal_arns must contain distinct exact IAM role or user ARNs."
  }
}

variable "anti_demo_runtime_role_name" {
  description = "Fixed name of the sealed runtime role. Fixed rather than prefixed on purpose: the operator's ~/.aws/config names it, and the fortnightly sweep is expected to delete and the installer to recreate it."
  type        = string
  default     = "anti-demo-runtime"

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,64}$", var.anti_demo_runtime_role_name))
    error_message = "anti_demo_runtime_role_name must be a valid IAM role name of at most 64 characters."
  }
}

variable "anti_demo_runtime_max_session_seconds" {
  description = "MaxSessionDuration for the sealed runtime role. Twelve hours is the AWS ceiling and costs nothing; it bounds how often a long-running server re-mints credentials, not how long the install survives."
  type        = number
  default     = 43200

  validation {
    condition     = var.anti_demo_runtime_max_session_seconds >= 3600 && var.anti_demo_runtime_max_session_seconds <= 43200
    error_message = "anti_demo_runtime_max_session_seconds must be between 3600 and 43200."
  }
}
