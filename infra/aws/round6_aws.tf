# Round 6's AWS lane: AWS DMS captures each change to Round 6's source table from one competitor's
# r6 database over logical replication and writes it to S3, and an AWS Glue job per competitor
# streams those files into a Delta table that the lakehouse reads as a Unity Catalog external
# table. AWS moves the data; see docs/design/v1.1-rounds-4-6-aws.md, section 4, and
# glue/round6_writer.py for the job itself. Round 4's Glue writer in reverse.
#
# Built by a second, additive apply. Unity Catalog reads the lane's Delta tables through an IAM role
# that trusts Databricks' Unity Catalog role only with the storage credential's external ID, and the
# installer learns that ID by creating the credential first. Until `round6_uc_external_id` is set,
# none of this exists, and every other resource plans exactly as it did before.
#
# Nothing here is created or deleted per bout. The app starts the DMS task and a Glue run at the
# bell and stops both after the bout; everything below stands for the life of the installation.
# While parked, only the replication instance bills (a dms.t3.small); the tasks, endpoints and jobs
# cost nothing, and the bucket holds a few kilobytes of change files, tables and checkpoints.

locals {
  round6_aws_enabled = local.v7_enabled && var.round6_uc_external_id != null

  round6_aws_competitors = local.round6_aws_enabled ? toset(["aurora", "rds"]) : toset([])
  round6_aws_name        = local.v7_enabled ? local.v7_round_resource_names["r6"] : ""
  round6_aws_tags        = local.v7_enabled ? local.v7_round_tags["r6"] : {}
  round6_aws_iam_tags    = local.v7_enabled ? local.v7_round_iam_tags["r6"] : {}

  # One /24 of the default VPC's 172.31.0.0/16 above the default subnets, like Round 4's Glue
  # subnet, and never the same one: it steps from Round 4's by one to 190 places, chosen by the
  # installation ID, around the 191 /24s Round 4 chooses among. DMS requires subnets in two
  # Availability Zones, so the /24 is split into two /25s. An explicit VPC names its own.
  round6_dms_subnet_netnum = local.v7_enabled ? 64 + (
    local.round4_glue_subnet_netnum - 64 + 1
    + parseint(substr(sha256("${trimspace(var.installation_id)}:r6-dms"), 0, 2), 16) % 190
  ) % 191 : 0
  round6_dms_subnet_cidr = (
    !local.round6_aws_enabled ? null :
    var.round6_dms_subnet_cidr != null ? var.round6_dms_subnet_cidr :
    local.use_default_network ? cidrsubnet(data.aws_vpc.default[0].cidr_block, 8, local.round6_dms_subnet_netnum) :
    null
  )
  # The runner's zone, which every installation already uses, and the first other zone.
  round6_dms_zones = local.round6_aws_enabled ? [
    data.aws_subnet.runner.availability_zone,
    [
      for zone in sort(data.aws_availability_zones.round6_dms[0].names) :
      zone if zone != data.aws_subnet.runner.availability_zone
    ][0],
  ] : []

  round6_capture_database_role = "round6_capture"
  # What a source endpoint holds until the installer sets the capture role's real credential on
  # it. Not a credential: nothing can log in with it, and the endpoint ignores later changes.
  round6_endpoint_placeholder = "set-by-the-installer"
  round6_source_schema        = "round6"
  round6_source_table         = "live_orders"
  round6_history_table        = "live_orders_history"
  round6_script_path          = "${path.module}/../../glue/round6_writer.py"
  round6_script_key           = "scripts/round6_writer.py"

  round6_database_hosts = local.round6_aws_enabled ? {
    aurora = aws_rds_cluster.aurora_by_round["r6"].endpoint
    rds    = aws_db_instance.rds_by_round["r6"].address
  } : {}
  round6_dms_engines = {
    aurora = "aurora-postgresql"
    rds    = "postgres"
  }

  # A fixed name, because the installer names this role in the storage credential it creates
  # before the role exists (Databricks accepts that), to learn the external ID the trust below
  # requires.
  round6_uc_role_name = local.v7_enabled ? "${local.v7_round_slugs["r6"]}-uc" : ""
  round6_uc_role_arn  = "arn:${data.aws_partition.current.partition}:iam::${var.aws_account_id}:role/${local.round6_uc_role_name}"
}

data "aws_availability_zones" "round6_dms" {
  count = local.round6_aws_enabled ? 1 : 0

  state = "available"
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

# DMS's account-wide service role, whose name AWS fixes for the whole account. DMS cannot create a
# replication subnet group without it. It is read here, never managed: another installation in the
# account may already rely on it, so the installer adopts it if it exists, creates it only if it
# does not, and nothing ever deletes it (docs/design/v1.1-rounds-4-6-aws.md, section 5).
data "aws_iam_role" "dms_vpc" {
  count = local.round6_aws_enabled ? 1 : 0

  name = "dms-vpc-role"
}

# The replication instance lives here, and nothing else does. Its own route table carries only the
# VPC's local route and the S3 gateway endpoint, so no other subnet's routing changes and DMS can
# reach the r6 databases and S3 and nothing on the internet.
resource "aws_subnet" "round6_dms" {
  count = local.round6_aws_enabled ? 2 : 0

  vpc_id                  = local.selected_vpc_id
  cidr_block              = local.round6_dms_subnet_cidr == null ? null : cidrsubnet(local.round6_dms_subnet_cidr, 1, count.index)
  availability_zone       = local.round6_dms_zones[count.index]
  map_public_ip_on_launch = false

  tags = merge(local.round6_aws_tags, {
    "Name" = "${local.round6_aws_name}-dms-${count.index}"
  })

  lifecycle {
    ignore_changes = [tags["expires-at"]]

    precondition {
      condition     = local.round6_dms_subnet_cidr != null
      error_message = "An explicit VPC must set round6_dms_subnet_cidr to a free /24 for Round 6's DMS lane."
    }
  }
}

resource "aws_route_table" "round6_dms" {
  count = local.round6_aws_enabled ? 1 : 0

  vpc_id = local.selected_vpc_id

  tags = merge(local.round6_aws_tags, {
    "Name" = "${local.round6_aws_name}-dms"
  })

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_route_table_association" "round6_dms" {
  count = local.round6_aws_enabled ? 2 : 0

  subnet_id      = aws_subnet.round6_dms[count.index].id
  route_table_id = aws_route_table.round6_dms[0].id
}

# DMS 3.4.7 and later reach S3 from a private subnet only through a VPC endpoint, and a gateway
# endpoint is enough for S3.
resource "aws_vpc_endpoint" "round6_dms_s3" {
  count = local.round6_aws_enabled ? 1 : 0

  vpc_id            = local.selected_vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.round6_dms[0].id]

  tags = merge(local.round6_aws_tags, {
    "Name" = "${local.round6_aws_name}-dms-s3"
  })

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_security_group" "round6_dms" {
  count = local.round6_aws_enabled ? 1 : 0

  name_prefix            = "${local.round6_aws_name}-dms-"
  description            = "Round 6 AWS DMS change capture: the r6 databases and S3"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  # Nothing connects to a replication instance; it only connects out.
  # The subnets' route table is what bounds this: it routes only inside the VPC and to S3.
  egress {
    description = "The r6 databases and S3, which are all the subnets route to"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.round6_aws_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_s3_bucket" "round6_aws" {
  count = local.round6_aws_enabled ? 1 : 0

  bucket = "${local.round6_aws_name}-cdc"
  # Holds DMS's change files, the lane's two Delta tables, their checkpoints and the script: all of
  # it this installation's, and all of it rebuilt by a reinstall. Uninstall must be able to take it
  # with whatever is in it.
  force_destroy = true

  tags = local.round6_aws_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_s3_bucket_public_access_block" "round6_aws" {
  count = local.round6_aws_enabled ? 1 : 0

  bucket                  = aws_s3_bucket.round6_aws[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "round6_aws" {
  count = local.round6_aws_enabled ? 1 : 0

  bucket = aws_s3_bucket.round6_aws[0].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "round6_aws" {
  count = local.round6_aws_enabled ? 1 : 0

  bucket = aws_s3_bucket.round6_aws[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# A change file is read once, by the next run after DMS writes it, and every bout runs the job until
# its row arrives, so a week-old file has always been read. The Delta tables and the standing
# checkpoints never expire. The bucket is not versioned: DMS's own guidance, because versions slow
# the listing its endpoint test depends on.
resource "aws_s3_bucket_lifecycle_configuration" "round6_aws" {
  count = local.round6_aws_enabled ? 1 : 0

  bucket = aws_s3_bucket.round6_aws[0].id

  rule {
    id     = "expire-change-files"
    status = "Enabled"
    filter {
      prefix = "dms/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_object" "round6_glue_script" {
  count = local.round6_aws_enabled ? 1 : 0

  bucket       = aws_s3_bucket.round6_aws[0].id
  key          = local.round6_script_key
  source       = local.round6_script_path
  etag         = filemd5(local.round6_script_path)
  content_type = "text/x-python"
  # What `doctor` compares against the sealed digest, so a script changed in S3 is found before a
  # bout runs it.
  metadata = {
    "sha256" = filesha256(local.round6_script_path)
  }
}

data "aws_iam_policy_document" "round6_dms_s3_assume" {
  count = local.round6_aws_enabled ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["dms.amazonaws.com"]
    }
  }
}

# What DMS writes change files as, and nothing more: its own prefix in the lane's bucket.
resource "aws_iam_role" "round6_dms_s3" {
  count = local.round6_aws_enabled ? 1 : 0

  name_prefix        = "${local.v7_round_slugs["r6"]}-dms-s3-"
  description        = "Round 6 AWS DMS: writes change files to the lane's bucket"
  assume_role_policy = data.aws_iam_policy_document.round6_dms_s3_assume[0].json

  tags = local.round6_aws_iam_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

data "aws_iam_policy_document" "round6_dms_s3_access" {
  count = local.round6_aws_enabled ? 1 : 0

  statement {
    sid       = "WriteChangeFiles"
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:PutObjectTagging"]
    resources = ["${aws_s3_bucket.round6_aws[0].arn}/dms/*"]
  }

  statement {
    sid       = "ListOwnBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.round6_aws[0].arn]
  }
}

resource "aws_iam_role_policy" "round6_dms_s3_access" {
  count = local.round6_aws_enabled ? 1 : 0

  name   = "round6-dms-s3-access"
  role   = aws_iam_role.round6_dms_s3[0].id
  policy = data.aws_iam_policy_document.round6_dms_s3_access[0].json
}

data "aws_iam_policy_document" "round6_glue_assume" {
  count = local.round6_aws_enabled ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "round6_glue" {
  count = local.round6_aws_enabled ? 1 : 0

  name_prefix        = "${local.v7_round_slugs["r6"]}-glue-"
  description        = "Round 6 AWS Glue writer: DMS change files into the lane's Delta tables"
  assume_role_policy = data.aws_iam_policy_document.round6_glue_assume[0].json

  tags = local.round6_aws_iam_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

# The AWS-managed policy every Glue job runs with: the job's logs and Glue's own reads. The measured
# spike ran on it.
resource "aws_iam_role_policy_attachment" "round6_glue_service" {
  count = local.round6_aws_enabled ? 1 : 0

  role       = aws_iam_role.round6_glue[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "round6_glue_access" {
  count = local.round6_aws_enabled ? 1 : 0

  statement {
    sid     = "ReadChangeFilesAndTheScript"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.round6_aws[0].arn}/dms/*",
      "${aws_s3_bucket.round6_aws[0].arn}/scripts/*",
    ]
  }

  # `delta*` and `checkpoints*` rather than `delta/*` and `checkpoints/*`: Glue's S3 filesystem
  # writes a folder marker beside each directory it creates (`checkpoints_$folder$`), and a run whose
  # marker write is refused fails before its stream starts, as Round 4's first proof found.
  statement {
    sid     = "KeepTablesAndCheckpoints"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.round6_aws[0].arn}/delta*",
      "${aws_s3_bucket.round6_aws[0].arn}/checkpoints*",
    ]
  }

  statement {
    sid       = "ListOwnBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.round6_aws[0].arn]
  }
}

resource "aws_iam_role_policy" "round6_glue_access" {
  count = local.round6_aws_enabled ? 1 : 0

  name   = "round6-glue-access"
  role   = aws_iam_role.round6_glue[0].id
  policy = data.aws_iam_policy_document.round6_glue_access[0].json
}

# Unity Catalog reads the lane's Delta tables as this role, and only reads them. Databricks' Unity
# Catalog role may assume it only with this installation's storage credential's external ID, and the
# role may assume itself, which Databricks requires. The self-trust names the account and pins the
# caller to this role's own ARN, rather than naming the role as a principal, because IAM refuses a
# trust policy that names a role before that role exists.
data "aws_iam_policy_document" "round6_uc_assume" {
  count = local.round6_aws_enabled ? 1 : 0

  statement {
    sid     = "UnityCatalogWithTheExternalId"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.round6_uc_master_role_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.round6_uc_external_id]
    }
  }

  statement {
    sid     = "ItselfOnly"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${var.aws_account_id}:root"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:PrincipalArn"
      values   = [local.round6_uc_role_arn]
    }
  }
}

resource "aws_iam_role" "round6_uc" {
  count = local.round6_aws_enabled ? 1 : 0

  name               = local.round6_uc_role_name
  description        = "Round 6 AWS lane: Unity Catalog reads its Delta tables, read-only"
  assume_role_policy = data.aws_iam_policy_document.round6_uc_assume[0].json

  tags = local.round6_aws_iam_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

data "aws_iam_policy_document" "round6_uc_access" {
  count = local.round6_aws_enabled ? 1 : 0

  statement {
    sid       = "ReadTheLaneTables"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.round6_aws[0].arn}/delta/*"]
  }

  statement {
    sid       = "ListTheLaneTables"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.round6_aws[0].arn]
  }

  statement {
    sid       = "AssumeItself"
    actions   = ["sts:AssumeRole"]
    resources = [local.round6_uc_role_arn]
  }
}

resource "aws_iam_role_policy" "round6_uc_access" {
  count = local.round6_aws_enabled ? 1 : 0

  name   = "round6-uc-read"
  role   = aws_iam_role.round6_uc[0].id
  policy = data.aws_iam_policy_document.round6_uc_access[0].json
}

resource "aws_dms_replication_subnet_group" "round6" {
  count = local.round6_aws_enabled ? 1 : 0

  replication_subnet_group_id          = "${local.round6_aws_name}-dms"
  replication_subnet_group_description = "Round 6 AWS DMS change capture: two isolated subnets"
  subnet_ids                           = aws_subnet.round6_dms[*].id

  tags = local.round6_aws_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }

  # DMS refuses a subnet group until its account-wide VPC role exists; the installer makes sure of
  # it before this apply, and the data source fails the plan with a clear error if it has not.
  depends_on = [data.aws_iam_role.dms_vpc]
}

resource "aws_dms_replication_instance" "round6" {
  count = local.round6_aws_enabled ? 1 : 0

  replication_instance_id     = "${local.round6_aws_name}-dms"
  replication_instance_class  = "dms.t3.small"
  allocated_storage           = 20
  engine_version              = "3.6.1"
  multi_az                    = false
  publicly_accessible         = false
  auto_minor_version_upgrade  = false
  apply_immediately           = true
  availability_zone           = local.round6_dms_zones[0]
  replication_subnet_group_id = aws_dms_replication_subnet_group.round6[0].id
  vpc_security_group_ids      = [aws_security_group.round6_dms[0].id]

  tags = local.round6_aws_tags

  lifecycle {
    # An engine AWS upgrades during maintenance is not drift to undo: DMS cannot be downgraded, and
    # a plan that tried would refuse every later reconcile (the class of Round 5's AMI drift).
    ignore_changes = [tags["expires-at"], engine_version]
  }

  depends_on = [
    aws_route_table_association.round6_dms,
    aws_vpc_endpoint.round6_dms_s3,
  ]
}

# One source endpoint per competitor: the r6 database's address and its capture role. The password
# is the installer's, set on every re-run, and never in Terraform state; this placeholder is what an
# endpoint holds before the installer has run. DDL capture is off, so DMS installs no event trigger
# or audit table in the source database and the capture role needs no privilege beyond replication
# and reading the one table.
resource "aws_dms_endpoint" "round6_source" {
  for_each = local.round6_aws_competitors

  endpoint_id   = "${local.round6_aws_name}-src-${each.key}"
  endpoint_type = "source"
  engine_name   = local.round6_dms_engines[each.key]
  server_name   = local.round6_database_hosts[each.key]
  port          = 5432
  database_name = local.database_name
  username      = local.round6_capture_database_role
  password      = local.round6_endpoint_placeholder
  ssl_mode      = "require"

  postgres_settings {
    capture_ddls = false
  }

  tags = local.round6_aws_tags

  # See the target endpoint below: DMS can hold an endpoint in `deleting` past the provider's
  # five minutes just after its task goes.
  timeouts {
    delete = "20m"
  }

  lifecycle {
    ignore_changes = [password, tags["expires-at"]]
  }
}

# One S3 target per competitor: Parquet change files under its own prefix, each carrying DMS's
# operation and commit timestamp, flushed within a second of the change rather than DMS's default
# minute or 32 MB.
resource "aws_dms_s3_endpoint" "round6_target" {
  for_each = local.round6_aws_competitors

  endpoint_id             = "${local.round6_aws_name}-dst-${each.key}"
  endpoint_type           = "target"
  bucket_name             = aws_s3_bucket.round6_aws[0].bucket
  bucket_folder           = "dms/${each.key}"
  service_access_role_arn = aws_iam_role.round6_dms_s3[0].arn
  data_format             = "parquet"
  parquet_version         = "parquet-2-0"
  encryption_mode         = "SSE_S3"
  timestamp_column_name   = "dms_commit_ts"
  cdc_max_batch_interval  = 1
  cdc_min_file_size       = 1

  tags = local.round6_aws_tags

  # The first full v1.1 uninstall (2026-09-30) failed here: DMS held this endpoint in
  # `deleting` past the provider's five-minute default just after its task was deleted, then
  # finished on its own. Waiting longer is the whole fix; it changes nothing in AWS.
  timeouts {
    delete = "20m"
  }

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }

  depends_on = [aws_iam_role_policy.round6_dms_s3_access]
}

# One change-capture task per competitor, parked. The installer's first start creates the task's
# replication slot, which stands from then on: a stopped task keeps it, and each bell resumes from
# it (docs/design/v1.1-rounds-4-6-aws.md, section 4).
resource "aws_dms_replication_task" "round6" {
  for_each = local.round6_aws_competitors

  replication_task_id      = "${local.round6_aws_name}-cdc-${each.key}"
  migration_type           = "cdc"
  replication_instance_arn = aws_dms_replication_instance.round6[0].replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.round6_source[each.key].endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.round6_target[each.key].endpoint_arn
  start_replication_task   = false

  table_mappings = jsonencode({
    rules = [
      {
        "rule-type" = "selection"
        "rule-id"   = "1"
        "rule-name" = "round6-live-orders"
        "object-locator" = {
          "schema-name" = local.round6_source_schema
          "table-name"  = local.round6_source_table
        }
        "rule-action" = "include"
      },
    ]
  })

  tags = local.round6_aws_tags

  lifecycle {
    # DMS answers with its whole settings document, defaults included, for a task created with
    # none; that is not a change to make.
    ignore_changes = [tags["expires-at"], replication_task_settings]
  }
}

resource "aws_glue_job" "round6_writer" {
  for_each = local.round6_aws_competitors

  name              = "${local.round6_aws_name}-writer-${each.key}"
  description       = "Round 6 AWS lane: the r6 ${each.key} database's changes into a Delta table"
  role_arn          = aws_iam_role.round6_glue[0].arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  # A run nothing stops, because the app died, stops itself within half an hour.
  timeout = 30

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.round6_aws[0].bucket}/${aws_s3_object.round6_glue_script[0].key}"
    python_version  = "3"
  }

  # Two runs would share a checkpoint. See the design's section 2 for why the app waits for a
  # stopped run's slot rather than raising this.
  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--datalake-formats"    = "delta"
    "--conf"                = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--source"              = "s3://${aws_s3_bucket.round6_aws[0].bucket}/dms/${each.key}/${local.round6_source_schema}/${local.round6_source_table}/"
    "--target"              = "s3://${aws_s3_bucket.round6_aws[0].bucket}/delta/${each.key}/${local.round6_history_table}"
    "--checkpoint"          = "s3://${aws_s3_bucket.round6_aws[0].bucket}/checkpoints/${each.key}"
    "--competitor"          = each.key
    "--trigger_seconds"     = "1"
  }

  tags = local.round6_aws_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }

  depends_on = [
    aws_iam_role_policy_attachment.round6_glue_service,
    aws_iam_role_policy.round6_glue_access,
  ]
}
