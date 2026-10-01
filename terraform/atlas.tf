# ---------------------------------------------------------------------------
# Atlas cluster
# ---------------------------------------------------------------------------

locals {
  # Every shard (or the single replica set, if not sharded) gets the
  # same cross-region distribution: 2 electable nodes in London
  # (EU_WEST_2, priority 7 - the Atlas primary always prefers this
  # region), 2 in Dublin/Ireland (EU_WEST_1, priority 6), 1 in
  # Frankfurt (EU_CENTRAL_1, priority 5). Previously the sharded branch
  # instead put each *whole shard* as a single 3-node replica set in
  # one round-robin region - i.e. a shard landing on Frankfurt had NO
  # presence in London/Dublin at all, which isn't the intended
  # architecture.
  region_configs_2_2_1 = [
    {
      electable_specs = {
        instance_size = var.atlas_instance_size
        node_count    = 2
      }
      provider_name = "AWS"
      priority      = 7
      region_name   = "EU_WEST_2"
    },
    {
      electable_specs = {
        instance_size = var.atlas_instance_size
        node_count    = 2
      }
      provider_name = "AWS"
      priority      = 6
      region_name   = "EU_WEST_1"
    },
    {
      electable_specs = {
        instance_size = var.atlas_instance_size
        node_count    = 1
      }
      provider_name = "AWS"
      priority      = 5
      region_name   = "EU_CENTRAL_1"
    },
  ]

  # Build replication_specs as a list of objects (nested-type attribute, not block)
  cluster_replication_specs = local.is_sharded ? [
    for shard_idx in range(local.effective_shard_count) : {
      region_configs = local.region_configs_2_2_1
    }
    ] : [
    {
      region_configs = local.region_configs_2_2_1
    }
  ]
}

resource "mongodbatlas_advanced_cluster" "this" {
  project_id             = var.atlas_project_id
  name                   = var.atlas_cluster_name
  cluster_type           = local.is_sharded ? "SHARDED" : "REPLICASET"
  backup_enabled         = var.atlas_backup_enabled
  mongo_db_major_version = "8.0"
  version_release_system = "LTS"

  replication_specs = local.cluster_replication_specs

  advanced_configuration = {
    oplog_size_mb = 16384
  }

  config_server_management_mode = (
    local.is_sharded && var.config_server_type == "dedicated"
    ? "FIXED_TO_DEDICATED"
    : null
  )

  tags = {
    Project   = "DistKafka"
    ManagedBy = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Database user
# ---------------------------------------------------------------------------

resource "mongodbatlas_database_user" "app" {
  project_id         = var.atlas_project_id
  username           = var.db_username
  password           = var.db_password
  auth_database_name = "admin"

  roles {
    role_name     = "readWrite"
    database_name = var.db_name
  }

  roles {
    role_name     = "dbAdmin"
    database_name = var.db_name
  }

  dynamic "roles" {
    for_each = local.is_sharded ? [1] : []
    content {
      role_name     = "enableSharding"
      database_name = "admin"
    }
  }

  scopes {
    name = mongodbatlas_advanced_cluster.this.name
    type = "CLUSTER"
  }
}

# ---------------------------------------------------------------------------
# IP access list — both EC2 Elastic IPs (public-IP fallback path; not
# used when PrivateLink is enabled, since all traffic then routes
# privately instead of over the public internet)
# ---------------------------------------------------------------------------

resource "mongodbatlas_project_ip_access_list" "jpclient1" {
  count      = var.privatelink_enabled ? 0 : 1
  project_id = var.atlas_project_id
  ip_address = aws_eip.jpclient1.public_ip
  comment    = "jpclient1 (London) - ${aws_instance.jpclient1.id}"
}

resource "mongodbatlas_project_ip_access_list" "jpclient2" {
  count      = var.privatelink_enabled ? 0 : 1
  project_id = var.atlas_project_id
  ip_address = aws_eip.jpclient2.public_ip
  comment    = "jpclient2 (Ireland) - ${aws_instance.jpclient2.id}"
}

# A pre-existing, NOT-Terraform-managed "0.0.0.0/0" entry was found in
# this project's IP access list - i.e. literally anyone on the internet
# with valid DB credentials could connect, completely undermining the
# point of restricting access to just the two EC2 hosts (let alone
# PrivateLink). Brought under Terraform control here specifically so it
# can be declaratively and permanently removed (count = 0, always) -
# see README/ARCHITECTURE.md. NOT touching the other unrelated entries
# found in the same project's access list ("Memex EC2 app instance",
# "Claims demo EC2 app instance") - those belong to other apps sharing
# this Atlas project, out of scope for DistKafka.
# A pre-existing, NOT-Terraform-managed "0.0.0.0/0" entry was found in
# this project's IP access list - i.e. literally anyone on the internet
# with valid DB credentials could connect, completely undermining the
# point of restricting access to just the two EC2 hosts (let alone
# PrivateLink). Replaced here with just the deployer's own static IP
# (same local.my_cidr already used for SSH/Kafka security group access
# elsewhere in this repo) - kept separate from, and independent of, the
# EC2 hosts' own access (mongodbatlas_project_ip_access_list.jpclient1/2
# above), so this doesn't get removed just because privatelink_enabled
# is toggled - it's for YOUR admin/debugging access, not the hosts'.
# NOT touching the other unrelated entries found in the same project's
# access list ("Memex EC2 app instance", "Claims demo EC2 app
# instance") - those belong to other apps sharing this Atlas project,
# out of scope for DistKafka.
resource "mongodbatlas_project_ip_access_list" "deployer" {
  project_id = var.atlas_project_id
  cidr_block = local.my_cidr
  comment    = "Deployer's own static IP (admin/debugging access, independent of the EC2 hosts)"
}

# ---------------------------------------------------------------------------
# PrivateLink (enabled by default - set var.privatelink_enabled = false to opt out)
# ---------------------------------------------------------------------------
#
# Atlas may only populate connection_strings.private_endpoint once EVERY
# region the cluster spans has a completed private endpoint connection -
# it's all-or-nothing across the whole (multi-region) cluster, not per-region.
# This cluster spans London/EU_WEST_2, Ireland/EU_WEST_1, and
# Frankfurt/EU_CENTRAL_1. Frankfurt's endpoint service is consumed by an
# interface endpoint hosted in Dublin, so no Frankfurt AWS VPC is needed.
#
# Each connected region needs three resources, in this order:
#   1. mongodbatlas_privatelink_endpoint  - asks Atlas to create the
#      AWS-side PrivateLink service; returns endpoint_service_name
#   2. aws_vpc_endpoint (in ec2.tf)       - the actual AWS interface
#      endpoint in your VPC, pointed at that service_name
#   3. mongodbatlas_privatelink_endpoint_service - links the AWS
#      endpoint's ID back to Atlas to complete the connection
#
# Frankfurt uses all three logical PrivateLink resources, but its AWS
# interface endpoint is hosted in Dublin via AWS cross-region PrivateLink.
# No Frankfurt AWS VPC, interface endpoint, or endpoint-service attachment
# is managed by this configuration.
#
# (The previous version of this file had resources (1) and (3) above
# swapped/inverted relative to the actual provider schema, and had no
# aws_vpc_endpoint at all - neither would have worked as written.)

resource "mongodbatlas_privatelink_endpoint" "london" {
  count         = var.privatelink_enabled ? 1 : 0
  project_id    = var.atlas_project_id
  provider_name = "AWS"
  region        = "EU_WEST_2"
}

resource "mongodbatlas_privatelink_endpoint" "ireland" {
  count         = var.privatelink_enabled ? 1 : 0
  project_id    = var.atlas_project_id
  provider_name = "AWS"
  region        = "EU_WEST_1"
}

resource "mongodbatlas_privatelink_endpoint" "frankfurt" {
  count         = var.privatelink_enabled ? 1 : 0
  project_id    = var.atlas_project_id
  provider_name = "AWS"
  region        = "EU_CENTRAL_1"

  # The Frankfurt Atlas service is consumed by an interface endpoint hosted
  # in Dublin, so permit that AWS region as a remote endpoint region.
  supported_remote_regions = ["EU_WEST_1"]
}

resource "mongodbatlas_privatelink_endpoint_service" "london" {
  count               = var.privatelink_enabled ? 1 : 0
  project_id          = var.atlas_project_id
  private_link_id     = mongodbatlas_privatelink_endpoint.london[0].private_link_id
  endpoint_service_id = aws_vpc_endpoint.london[0].id
  provider_name       = "AWS"
}

# Atlas appears to only support one in-flight "add private endpoint
# connection" operation per project at a time - creating london/
# ireland's linking resources concurrently (Terraform's
# default behaviour for independent resources) intermittently fails
# with "Projects with private endpoints in multiple regions cannot
# support more than one endpoint in each region", even though each
# region only ever has exactly one consumer. Forcing them to be
# created strictly sequentially avoids this.
resource "mongodbatlas_privatelink_endpoint_service" "ireland" {
  count               = var.privatelink_enabled ? 1 : 0
  project_id          = var.atlas_project_id
  private_link_id     = mongodbatlas_privatelink_endpoint.ireland[0].private_link_id
  endpoint_service_id = aws_vpc_endpoint.ireland[0].id
  provider_name       = "AWS"

  depends_on = [mongodbatlas_privatelink_endpoint_service.london]
}

resource "mongodbatlas_privatelink_endpoint_service" "frankfurt" {
  count               = var.privatelink_enabled ? 1 : 0
  project_id          = var.atlas_project_id
  private_link_id     = mongodbatlas_privatelink_endpoint.frankfurt[0].private_link_id
  endpoint_service_id = aws_vpc_endpoint.frankfurt_from_ireland[0].id
  provider_name       = "AWS"

  depends_on = [mongodbatlas_privatelink_endpoint_service.ireland]
}
