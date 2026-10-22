variable "location" {
  description = "Azure region in which to create resources. Must match the Lakebase workspace region."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]+$", var.location))
    error_message = "location must be a valid Azure region name in lowercase form, for example eastus or westus2."
  }
}

variable "resource_group_name" {
  description = "Existing resource group name. Terraform never creates or deletes resource groups."
  type        = string

  validation {
    condition     = length(var.resource_group_name) >= 1 && length(var.resource_group_name) <= 90
    error_message = "resource_group_name must be between 1 and 90 characters."
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

variable "owner" {
  description = "Human owner email recorded on every Terraform-managed Azure resource."
  type        = string

  validation {
    condition     = length(trimspace(var.owner)) >= 3 && length(var.owner) <= 128
    error_message = "owner must be a non-empty value between 3 and 128 characters."
  }
}

variable "expires_at" {
  description = "RFC 3339 expiration timestamp recorded on every Terraform-managed Azure resource, as on AWS. The app moves it while the installation is in use (server/lease.py)."
  type        = string

  validation {
    condition     = can(timecmp(var.expires_at, "1970-01-01T00:00:00Z")) && can(regex("Z$", var.expires_at))
    error_message = "expires_at must be an RFC 3339 UTC timestamp, for example 2026-10-22T18:00:00Z."
  }
}

variable "policy_tags" {
  description = "Tags the subscription's own policy requires on every resource, written as given. Empty for a subscription that requires none."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for name in keys(var.policy_tags) :
      !contains(["anti-demo-run-id", "owner", "managed-by", "expires-at"], lower(name))
    ])
    error_message = "policy_tags cannot redefine the installation's own tags (anti-demo-run-id, owner, managed-by, expires-at)."
  }

  # Azure reads tag names in any case, and refuses a request that names one twice.
  validation {
    condition     = length(distinct([for name in keys(var.policy_tags) : lower(name)])) == length(var.policy_tags)
    error_message = "policy_tags names must differ in more than case."
  }
}

variable "lease_date_tag" {
  description = "Name of a tag that carries the lease's date (YYYY-MM-DD) on every resource, for a subscription whose policy expects one. The app moves it with expires-at. Null writes no such tag."
  type        = string
  default     = null

  validation {
    condition     = var.lease_date_tag == null || can(regex("^[A-Za-z][A-Za-z0-9_.-]{0,63}$", var.lease_date_tag))
    error_message = "lease_date_tag must be a plain tag name: letters, digits, '_', '.' or '-', starting with a letter."
  }

  validation {
    condition     = var.lease_date_tag == null || !contains(["anti-demo-run-id", "owner", "managed-by", "expires-at"], lower(coalesce(var.lease_date_tag, "-")))
    error_message = "lease_date_tag would write a date over one of the installation's own tags."
  }

  validation {
    condition     = var.lease_date_tag == null || !contains([for name in keys(var.policy_tags) : lower(name)], lower(coalesce(var.lease_date_tag, "-")))
    error_message = "lease_date_tag is the lease's; it can't also be a fixed policy tag."
  }
}

variable "operator_cidr" {
  description = "Explicit public IPv4 /32 allowed to connect to the PostgreSQL servers. No broader CIDR is accepted."
  type        = string

  validation {
    condition = (
      can(cidrnetmask(var.operator_cidr)) &&
      can(regex("/32$", var.operator_cidr))
    )
    error_message = "operator_cidr must be an explicit IPv4 /32, for example 203.0.113.10/32."
  }

  # On Flexible Server a 0.0.0.0 rule means "allow every Azure service", which admits other
  # customers' resources. No operator connects from the others (loopback, link-local, multicast).
  validation {
    condition     = !can(regex("^(0\\.|127\\.|169\\.254\\.|22[4-9]\\.|23[0-9]\\.)", var.operator_cidr))
    error_message = "operator_cidr must be an address an operator connects from."
  }
}

variable "databricks_egress_cidrs" {
  description = "Published Databricks serverless outbound prefixes for this region, admitted to PostgreSQL firewall rules so the deployed app can race the Azure lanes. Azure CIDRs may be /23 wide; this list comes from the published vendor feed."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition = alltrue([
      for cidr in var.databricks_egress_cidrs :
      can(cidrnetmask(cidr)) && tonumber(split("/", cidr)[1]) >= 23
    ]) && length(distinct(var.databricks_egress_cidrs)) == length(var.databricks_egress_cidrs)
    error_message = "databricks_egress_cidrs must be distinct IPv4 CIDRs of /23 or narrower."
  }
}


variable "name_prefix" {
  description = "Lowercase prefix for Azure resource identifiers."
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

variable "installation_id" {
  description = "Stable unique ID for the installation, used in storage and other resource names. When null, a digest is derived from run_id."
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

variable "zone" {
  description = "Availability zone for every server and runner. Pinned, because a size can be refused in one zone of a region and offered in the others, and an unpinned create lands where Azure picks (Phase 0: B2s in westus2 failed when unpinned and in zone 3, and worked in zones 1 and 2). One zone also keeps Round 5's runners beside its server. Null leaves placement to Azure, for a region without availability zones."
  type        = string
  default     = "1"
  nullable    = true

  validation {
    condition     = var.zone == null || contains(["1", "2", "3"], coalesce(var.zone, "1"))
    error_message = "zone must be \"1\", \"2\" or \"3\", or null in a region without availability zones."
  }
}

variable "key_vault_authorization" {
  description = "How the Round 5 vault authorizes: \"rbac\" (role assignments, which need role-assignment rights) or \"access_policy\" (access policies, which a Contributor can set, so a group with Contributor alone needs no vault grants)."
  type        = string
  default     = "rbac"

  validation {
    condition     = contains(["rbac", "access_policy"], var.key_vault_authorization)
    error_message = "key_vault_authorization must be \"rbac\" or \"access_policy\"."
  }
}

variable "flexible_server_sku" {
  description = "SKU for r2, r3, r4, r6 Flexible Servers. B2s by design; General Purpose where a subscription can't create Burstable in its region, disclosed on the card."
  type        = string
  default     = "B_Standard_B2s"

  validation {
    condition     = can(regex("^(B_Standard_B|GP_Standard_D)[0-9]", var.flexible_server_sku))
    error_message = "flexible_server_sku must be a Burstable or General Purpose SKU, for example B_Standard_B2s."
  }
}

variable "round5_server_sku" {
  description = "SKU for r5 Flexible Server (must be GeneralPurpose for PgBouncer support)."
  type        = string
  default     = "GP_Standard_D2ds_v5"

  validation {
    condition     = can(regex("^GP_Standard_D", var.round5_server_sku))
    error_message = "round5_server_sku must be a GeneralPurpose tier SKU, for example GP_Standard_D2ds_v5. Burstable tiers cannot enable PgBouncer."
  }
}

# On: Round 5 races on Azure, and the installer writes only what isn't a default, so this is what a
# virgin install builds. Off, the installation offers no Round 5, and the runners don't bill; their
# network, Key Vault and Service Bus stand either way. The runners reach out through a NAT gateway
# and carry no public IP, which a subscription's policy may deny on a NIC without an NSG of its own.
variable "round5_runners" {
  description = "Whether Round 5's two runner VMs, their NICs, NAT gateway and roles stand."
  type        = bool
  default     = true
}

variable "runner_vm_sku" {
  description = "SKU for Round 5 runner VMs: 8 vCPU and 16 GiB, the shape the harness sizes (server/capacity.py's AZURE_ROUND5_RUNNER_SIZE names the same). westus2 zone 1 refused D8ls_v6 with ZonalAllocationFailed twice on 2026-10-20 and created the v7 at once; a v6 is the one to try when a zone has no v7 left."
  type        = string
  default     = "Standard_D8ls_v7"

  validation {
    condition     = can(regex("^Standard_D[0-9]+ls_v[67]$", var.runner_vm_sku))
    error_message = "runner_vm_sku must be a Standard_D*ls_v6 or _v7 SKU, for example Standard_D8ls_v7."
  }
}

variable "runner_ssh_public_key" {
  description = "SSH public key for runner VM authentication. Required to connect to runners."
  type        = string

  validation {
    # An ed25519 key line is under 100 characters, so check its shape, not its length.
    condition     = can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp(256|384|521)) [A-Za-z0-9+/=]{40,}", trimspace(var.runner_ssh_public_key)))
    error_message = "runner_ssh_public_key must be an OpenSSH public key line (ssh-ed25519, ssh-rsa or ecdsa)."
  }
}

variable "admin_password" {
  description = "Admin password for every Flexible Server. Ephemeral: it reaches Azure through the write-only administrator_password_wo and never enters plan or state."
  type        = string
  sensitive   = true
  ephemeral   = true

  validation {
    condition     = length(var.admin_password) >= 16 && length(var.admin_password) <= 128
    error_message = "admin_password must be 16-128 characters."
  }
}

variable "admin_password_version" {
  description = "Bump to send a new admin_password to every server; the write-only value itself is never compared."
  type        = number
  default     = 1
}

variable "manage_role_assignments" {
  description = "If true, Terraform creates role assignments. If false, outputs the exact role assignments an administrator must make by hand."
  type        = bool
  default     = true
}
