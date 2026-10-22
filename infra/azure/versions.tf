terraform {
  required_version = ">= 1.12"

  # The default and only unconditional mode: state lives beside the manifest, one
  # generation per directory, addressed by `-backend-config=path=...` at init.
  # This mirrors the AWS edition: the manifest and state are a single
  # self-describing unit.
  backend "local" {}

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.10"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      # Recover purged key vaults; not available when purge_protection is enabled.
      recover_soft_deleted_key_vaults = true
    }
  }

  # Authenticates from the environment (ARM_CLIENT_ID, ARM_CLIENT_SECRET, ARM_TENANT_ID,
  # ARM_SUBSCRIPTION_ID), or from an `az login` when no client secret is set.
  # The installation's storage account has shared-key access off, so the provider must
  # reach its data plane with Entra ID tokens rather than account keys.
  storage_use_azuread = true

  # resource_provider_registrations = "none" prevents Terraform from attempting
  # to register providers at subscription scope, which a Contributor (without
  # User Access Administrator) cannot do. Preflight checks confirm they are
  # already Registered.
  resource_provider_registrations = "none"
}
