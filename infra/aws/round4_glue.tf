# Round 4's AWS lane: one AWS Glue job per competitor that reads Round 4's Delta table straight from
# S3 and writes it into that competitor's r4 database. AWS moves the data; see
# docs/design/v1.1-rounds-4-6-aws.md, section 4, and glue/round4_writer.py for the job itself.
#
# Built by a second, additive apply. The Glue role is read-only on Round 4's table and nothing
# else, and that table's S3 location exists only after the installer has created the table, which
# happens after the first apply. Until `round4_source_location` is set, none of this exists, and
# every other resource plans exactly as it did before.
#
# Nothing here is created or deleted per bout. The app starts a job run at the bell and stops it
# after the bout; everything below stands for the life of the installation and costs nothing while
# the jobs are parked except the few kilobytes in the bucket.

locals {
  round4_glue_enabled = local.v7_enabled && var.round4_source_location != null

  round4_glue_competitors = local.round4_glue_enabled ? toset(["aurora", "rds"]) : toset([])
  round4_glue_name        = local.v7_enabled ? local.v7_round_resource_names["r4"] : ""
  round4_glue_tags        = local.v7_enabled ? local.v7_round_tags["r4"] : {}
  round4_glue_iam_tags    = local.v7_enabled ? local.v7_round_iam_tags["r4"] : {}

  # s3://bucket/prefix/to/the/table -> "bucket" and "prefix/to/the/table".
  round4_source_parts  = local.round4_glue_enabled ? split("/", trimsuffix(trimprefix(var.round4_source_location, "s3://"), "/")) : []
  round4_source_bucket = local.round4_glue_enabled ? local.round4_source_parts[0] : ""
  round4_source_prefix = local.round4_glue_enabled ? join("/", slice(local.round4_source_parts, 1, length(local.round4_source_parts))) : ""

  # A /24 of the default VPC's 172.31.0.0/16 above the default subnets, which fill the first /18
  # (at most four /20s), chosen by the installation ID so that two installations in one VPC do
  # not collide. An explicit VPC has no such layout to rely on and names its own.
  round4_glue_subnet_netnum = local.v7_enabled ? 64 + parseint(substr(sha256("${trimspace(var.installation_id)}:r4-glue"), 0, 2), 16) % 191 : 0
  round4_glue_subnet_cidr = (
    !local.round4_glue_enabled ? null :
    var.round4_glue_subnet_cidr != null ? var.round4_glue_subnet_cidr :
    local.use_default_network ? cidrsubnet(data.aws_vpc.default[0].cidr_block, 8, local.round4_glue_subnet_netnum) :
    null
  )
  round4_glue_zone = data.aws_subnet.runner.availability_zone

  round4_writer_database_role = "round4_writer"
  round4_target_schema        = "round4"
  round4_target_table         = "model_score_ledger"
  round4_script_path          = "${path.module}/../../glue/round4_writer.py"
  round4_script_key           = "scripts/round4_writer.py"

  round4_database_hosts = local.round4_glue_enabled ? {
    aurora = aws_rds_cluster.aurora_by_round["r4"].endpoint
    rds    = aws_db_instance.rds_by_round["r4"].address
  } : {}
}

# The Glue ENIs live here, and nothing else does. Its own route table carries only the VPC's local
# route and the S3 gateway endpoint, so no other subnet's routing changes and a job can reach the r4
# databases and S3 and nothing on the internet.
resource "aws_subnet" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  vpc_id                  = local.selected_vpc_id
  cidr_block              = local.round4_glue_subnet_cidr
  availability_zone       = local.round4_glue_zone
  map_public_ip_on_launch = false

  tags = merge(local.round4_glue_tags, {
    "Name" = "${local.round4_glue_name}-glue"
  })

  lifecycle {
    ignore_changes = [tags["expires-at"]]

    precondition {
      condition     = local.round4_glue_subnet_cidr != null
      error_message = "An explicit VPC must set round4_glue_subnet_cidr to a free /24 for Round 4's Glue lane."
    }
  }
}

resource "aws_route_table" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  vpc_id = local.selected_vpc_id

  tags = merge(local.round4_glue_tags, {
    "Name" = "${local.round4_glue_name}-glue"
  })

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_route_table_association" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  subnet_id      = aws_subnet.round4_glue[0].id
  route_table_id = aws_route_table.round4_glue[0].id
}

resource "aws_vpc_endpoint" "round4_glue_s3" {
  count = local.round4_glue_enabled ? 1 : 0

  vpc_id            = local.selected_vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.round4_glue[0].id]

  tags = merge(local.round4_glue_tags, {
    "Name" = "${local.round4_glue_name}-glue-s3"
  })

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_security_group" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  name_prefix            = "${local.round4_glue_name}-glue-"
  description            = "Round 4 AWS Glue writer: its own nodes, the r4 databases and S3"
  vpc_id                 = local.selected_vpc_id
  revoke_rules_on_delete = true

  # Glue requires a self-referencing rule so a job's nodes can reach each other.
  ingress {
    description = "Between the nodes of one Glue job"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # The subnet's route table is what bounds this: it routes only inside the VPC and to S3.
  egress {
    description = "The r4 databases and S3, which are all the subnet routes to"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.round4_glue_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_s3_bucket" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  bucket = "${local.round4_glue_name}-glue"
  # Holds only the script, per-run checkpoints and run markers, all of them disposable; uninstall
  # must be able to take it with whatever is in it.
  force_destroy = true

  tags = local.round4_glue_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

resource "aws_s3_bucket_public_access_block" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  bucket                  = aws_s3_bucket.round4_glue[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  bucket = aws_s3_bucket.round4_glue[0].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  bucket = aws_s3_bucket.round4_glue[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Every run writes a checkpoint nobody reads again and a marker the app reads once, after its bout.
resource "aws_s3_bucket_lifecycle_configuration" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  bucket = aws_s3_bucket.round4_glue[0].id

  rule {
    id     = "expire-checkpoints"
    status = "Enabled"
    filter {
      prefix = "checkpoints/"
    }
    expiration {
      days = 2
    }
  }

  rule {
    id     = "expire-markers"
    status = "Enabled"
    filter {
      prefix = "markers/"
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

resource "aws_s3_object" "round4_glue_script" {
  count = local.round4_glue_enabled ? 1 : 0

  bucket       = aws_s3_bucket.round4_glue[0].id
  key          = local.round4_script_key
  source       = local.round4_script_path
  etag         = filemd5(local.round4_script_path)
  content_type = "text/x-python"
  # What `doctor` compares against the sealed digest, so a script changed in S3 is found before a
  # bout runs it.
  metadata = {
    "sha256" = filesha256(local.round4_script_path)
  }
}

data "aws_iam_policy_document" "round4_glue_assume" {
  count = local.round4_glue_enabled ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "round4_glue" {
  count = local.round4_glue_enabled ? 1 : 0

  name_prefix        = "${local.v7_round_slugs["r4"]}-glue-"
  description        = "Round 4 AWS Glue writer: reads Round 4's Delta table from S3"
  assume_role_policy = data.aws_iam_policy_document.round4_glue_assume[0].json

  tags = local.round4_glue_iam_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }
}

# The AWS-managed policy every Glue job runs with: the network interfaces a VPC connection needs,
# the job's logs and Glue's own reads. The measured spike ran on it.
resource "aws_iam_role_policy_attachment" "round4_glue_service" {
  count = local.round4_glue_enabled ? 1 : 0

  role       = aws_iam_role.round4_glue[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "round4_glue_access" {
  count = local.round4_glue_enabled ? 1 : 0

  # Round 4's table and nothing else in the workspace's storage. This reads the lakehouse *around*
  # Unity Catalog, straight off S3, which is what an outside engine does without credential
  # vending, Iceberg REST or Delta Sharing.
  statement {
    sid       = "ReadRoundFourTableOnly"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${local.round4_source_bucket}/${local.round4_source_prefix}/*"]
  }

  statement {
    sid       = "ListRoundFourTableOnly"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${local.round4_source_bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [local.round4_source_prefix, "${local.round4_source_prefix}/*"]
    }
  }

  statement {
    sid       = "ReadTheScript"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.round4_glue[0].arn}/scripts/*"]
  }

  # `checkpoints*` rather than `checkpoints/*`: Glue's S3 filesystem writes a folder marker
  # beside each directory it creates (`checkpoints_$folder$`), and a run whose marker write is
  # refused fails before its stream starts (the test installation's first proof, 2026-09-29).
  statement {
    sid     = "KeepCheckpointsAndMarkers"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.round4_glue[0].arn}/checkpoints*",
      "${aws_s3_bucket.round4_glue[0].arn}/markers*",
    ]
  }

  statement {
    sid       = "ListOwnBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.round4_glue[0].arn]
  }
}

resource "aws_iam_role_policy" "round4_glue_access" {
  count = local.round4_glue_enabled ? 1 : 0

  name   = "round4-glue-access"
  role   = aws_iam_role.round4_glue[0].id
  policy = data.aws_iam_policy_document.round4_glue_access[0].json
}

# One connection per competitor: the r4 database's address, its writer role and the network a run
# is placed in. The password is the installer's, set on every re-run, and never in Terraform
# state; this placeholder is what a connection holds before the installer has run.
resource "aws_glue_connection" "round4" {
  for_each = local.round4_glue_competitors

  name            = "${local.round4_glue_name}-${each.key}"
  connection_type = "JDBC"

  connection_properties = {
    "JDBC_CONNECTION_URL" = "jdbc:postgresql://${local.round4_database_hosts[each.key]}:5432/${local.database_name}"
    "USERNAME"            = local.round4_writer_database_role
    "PASSWORD"            = "set-by-the-installer"
  }

  physical_connection_requirements {
    availability_zone      = local.round4_glue_zone
    security_group_id_list = [aws_security_group.round4_glue[0].id]
    subnet_id              = aws_subnet.round4_glue[0].id
  }

  tags = local.round4_glue_tags

  lifecycle {
    ignore_changes = [connection_properties["PASSWORD"], tags["expires-at"]]
  }
}

resource "aws_glue_job" "round4_writer" {
  for_each = local.round4_glue_competitors

  name              = "${local.round4_glue_name}-writer-${each.key}"
  description       = "Round 4 AWS lane: Delta table in S3 to the r4 ${each.key} database"
  role_arn          = aws_iam_role.round4_glue[0].arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  # A run nothing stops, because the app died, stops itself within half an hour.
  timeout     = 30
  connections = [aws_glue_connection.round4[each.key].name]

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.round4_glue[0].bucket}/${aws_s3_object.round4_glue_script[0].key}"
    python_version  = "3"
  }

  # Two runs would share a table and race each other's writes. See the design's section 2 for why
  # the app waits for a stopped run's slot rather than raising this.
  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--datalake-formats"    = "delta"
    "--conf"                = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--source_path"         = trimsuffix(var.round4_source_location, "/")
    "--source_table_id"     = "read-at-start"
    "--starting_version"    = "snapshot"
    "--bucket"              = aws_s3_bucket.round4_glue[0].bucket
    "--competitor"          = each.key
    "--connection_name"     = aws_glue_connection.round4[each.key].name
    "--database"            = local.database_name
    "--target_schema"       = local.round4_target_schema
    "--target_table"        = local.round4_target_table
    "--trigger_seconds"     = "1"
    "--run_tag"             = "manual"
  }

  tags = local.round4_glue_tags

  lifecycle {
    ignore_changes = [tags["expires-at"]]
  }

  depends_on = [
    aws_iam_role_policy_attachment.round4_glue_service,
    aws_iam_role_policy.round4_glue_access,
    aws_route_table_association.round4_glue,
    aws_vpc_endpoint.round4_glue_s3,
  ]
}
