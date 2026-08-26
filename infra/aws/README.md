# AWS baseline infrastructure

This Terraform root owns the durable, secret-free AWS baseline only:

- the Aurora PostgreSQL and RDS PostgreSQL databases and their AWS-managed master secrets;
- the shared DB subnet group and baseline database security groups;
- one neutral, ingress-free SSM runner, its runner security group and outbound rule;
- the runner role/profile, SSM attachment, and Terraform-owned runner permissions boundary;
- the exact-principal-trusted Round 5 control role and its bounded inline policy;
- two durable Proxy credential secret containers, with no Terraform-managed secret values;
- one RDS Proxy service role with one exact two-secret inline policy;
- a state-owned destroy guard.

Terraform does **not** own any credential secret version or value, proxy security group or rule,
RDS Proxy, target-group setting, or target. The application lifecycle updates the two durable
secret values and creates the remaining add-ons with AWS APIs after first journaling intent in
`anti_demo_coordination.round5_creation_journal`; it removes the dynamic add-ons at the end of the
bout. No per-bout IAM identity appears in Terraform state or outputs.

## Baseline safety contract

Every Terraform-managed taggable resource receives `anti-demo-run-id`, `Owner`, `owner`,
`expires-at`, and `managed-by=terraform`. Per-bout resources instead carry those same ownership
values, `managed-by=round5-lifecycle`, and a nonempty `anti-demo-bout-id`. Their names begin with
the deterministic `round5_bout_name_prefix` output, which is a readable prefix plus a digest of
the complete run-scoped baseline name; the lifecycle app appends the journaled bout ID.

The control role trusts exactly `round5_app_principal_arn`. It can operate only in the configured
account and region where the API supports those condition keys. Its dynamic create permissions
require the run ownership tags. `iam:PassRole`, `iam:GetRole`, and `iam:GetRolePolicy` name only
the Terraform-owned Proxy role; the control role cannot create, modify, or delete IAM roles or
policies. The Proxy role trusts only `rds.amazonaws.com`, and its only inline policy can describe
and read exactly the two Proxy credential containers. The runner identity policy and permissions
boundary allow describing, reading, and writing those same exact containers while retaining
read-only access to the two exact AWS-managed master secrets.

Every database baseline security group admits PostgreSQL over one
`concat([var.operator_cidr], var.serverless_egress_cidrs)` ingress block, so a reader auditing
blast radius must count both halves: the operator's exact `/32`, plus — on an installation that
seals them — the Databricks-published serverless egress prefixes for the workspace's region. The
variable defaults to `[]`, which admits the operator alone; the installation these docs were
written against seals four further ranges, which is what lets the deployed app race an opponent
over TCP 5432 at all. Those are the vendor's published prefixes rather than anything this project
chooses, and the validation refuses anything broader than a `/24`. The two baseline groups and
the Round 5 per-round groups additionally admit the neutral runner's security group. The
lifecycle adds and journals narrowly scoped proxy rules for each bout and removes them with that
bout. The runner security group has no ingress and one Terraform-owned outbound rule.

Both databases use `manage_master_user_password = true`; Terraform exposes only their
AWS-managed secret ARNs. Treat Terraform state as sensitive even though this module never stores
a database password.

## Network selection

Leave `vpc_id`, `subnet_ids`, and `runner_subnet_id` unset to discover the default VPC, or set all
three. Explicit database subnets must belong to the VPC and span at least two Availability
Zones. The runner subnet must be selected from that list. Terraform resolves an explicit subnet
route-table association first and otherwise falls back to the VPC main route table; the runner
precondition requires an effective direct `0.0.0.0/0` Internet Gateway route.

## Validate and review

From this directory:

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform fmt -check -recursive
terraform validate
```

For the reviewed sandbox, the stable app principal has this shape. `111122223333` is
AWS's reserved documentation account ID, used here and in `docs/iam/` in place of any real
account; substitute your own:

```text
arn:aws:iam::111122223333:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_YourPermissionSetName_aaaabbbbccccdddd
```

Bind the stable IAM role ARN, never an STS assumed-role session ARN. Provider account allowlisting
still makes a mismatched `aws_account_id` fail. No plan or apply is part of repository validation.

## One-time legacy partial-state reconciliation

Older Round 5 Terraform may have partially created per-bout-shaped resources. Do not let the new
configuration plan their deletion first. The lifecycle must perform its ownership checks and
physical cleanup before obsolete addresses leave state.

From the repository root, run the revised reconciliation:

```bash
./antidemo setup
```

That command checks the exact run/account/region/tags, reconciles the failed legacy partial
setup, verifies the RDS `default.postgres17` baseline is available and in sync, and removes the
obsolete add-ons. It must succeed before continuing. Then, from `infra/aws`, make a protected
state backup and remove only obsolete addresses that are actually present:

```bash
terraform state pull > ../../.anti-demo/terraform-pre-round5-baseline-migration.tfstate

for address in \
  data.aws_iam_policy_document.round5_proxy_secret \
  data.aws_iam_policy_document.round5_runner_secrets \
  aws_db_parameter_group.rds_round5 \
  aws_db_proxy_target.round5 \
  aws_db_proxy_default_target_group.round5 \
  aws_db_proxy.round5 \
  aws_vpc_security_group_ingress_rule.round5_runner_to_proxy \
  aws_vpc_security_group_egress_rule.round5_proxy_to_rds \
  aws_security_group.round5_proxy \
  aws_iam_role_policy.round5_proxy_secret \
  aws_iam_role.round5_proxy \
  aws_iam_role_policy.round5_runner_secrets \
  aws_secretsmanager_secret.round5_lakebase_credentials \
  aws_secretsmanager_secret.round5_rds_credentials
do
  terraform state list | rg -x "$address" >/dev/null && terraform state rm "$address"
done
```

This sequence intentionally removes bindings only after lifecycle deletion; `state rm` does not
delete AWS objects. Do not remove database, runner, baseline IAM, baseline security-group, runner
egress, or destroy-guard addresses. Afterward, review the ordinary plan: it should retain the
database and runner baseline, update the stable execution role into the control role, create the
static Proxy role and secret containers, update the runner boundary, and retain
`terraform_data.round5_destroy_guard`; it must contain no per-bout IAM resource.

## Destroy gate

Terraform cannot query the PostgreSQL creation journal, so it must not pretend that an input or
local file proves cleanup. `terraform_data.round5_destroy_guard` has `prevent_destroy = true`;
therefore raw `terraform destroy` and `terraform plan -destroy` refuse while the sentinel remains
in state.

Run cleanup from the repository root first:

```bash
./antidemo cleanup
```

The lifecycle queries `anti_demo_coordination.round5_creation_journal`, requires every entry to
be `DELETED`, performs run-tag discovery for absence of add-ons, and writes the local evidence
receipt `.anti-demo/round5-clean-receipt.json`. Only after that command and receipt succeed may
the operator release exactly the Terraform sentinel and create a destroy plan:

```bash
cd infra/aws
terraform state rm terraform_data.round5_destroy_guard
terraform plan -destroy -out=round1-destroy.tfplan
terraform show round1-destroy.tfplan
```

If cleanup or evidence fails, do not remove the guard. Removing it is an explicit state mutation
and authorization to destroy the durable baseline; it is not part of setup, validation, or bout
cleanup. Database deletion is permanent because final snapshots are disabled.
