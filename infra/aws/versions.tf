terraform {
  required_version = ">= 1.9.0"

  # The default and only unconditional mode: state lives beside the manifest, one
  # generation per directory, addressed by `-backend-config=path=...` at init.
  # `antidemo cleanup` compares manifest tags against live AWS tags, so keeping the
  # state and the manifest in one gitignored directory means a generation is a
  # single self-describing unit.
  #
  # A backend block cannot take variables and a configuration cannot declare two
  # backends, so remote state is opt-in through a generated override file:
  # `backend_override.tf` replaces this block when it is present. See
  # backend_override.tf.example for the exact shape, and docs/DEPLOY.md for how
  # bootstrap.sh selects it. That file is gitignored and generated per
  # generation, never hand-edited, so a local-state generation and an S3-state
  # generation can share this working directory without either adopting the
  # other's backend.
  #
  # required_version stays at 1.9.0 for this mode. The S3 mode additionally
  # needs 1.11 for S3-native state locking, and bootstrap.sh enforces that floor
  # only when it is selected, so nothing here changes for existing installs.
  backend "local" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [var.aws_account_id]
}
