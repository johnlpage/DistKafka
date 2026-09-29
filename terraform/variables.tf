# ---------------------------------------------------------------------------
# MongoDB Atlas
# ---------------------------------------------------------------------------

variable "atlas_project_id" {
  description = "Existing MongoDB Atlas Project ID (groupId) to create resources in."
  type        = string
}

variable "atlas_cluster_name" {
  description = "Name of the Atlas cluster as it will appear in the Atlas UI."
  type        = string
  default     = "distkafka-cluster"
}

variable "atlas_instance_size" {
  description = "Atlas tier for electable nodes (e.g. M30, M40, M50)."
  type        = string
  default     = "M30"
}

variable "nshards" {
  description = "Number of shards. 0 = single replica set (5 nodes: 2 London/2 Dublin/1 Frankfurt). >=1 = sharded cluster with that many shards, each shard ALSO distributed 2 London/2 Dublin/1 Frankfurt (same layout, repeated per shard). Forced to at least 1 (sharded) when var.privatelink_enabled = true, regardless of this value, since PrivateLink's cross-region failover here relies on mongos routers (see provisions.tf)."
  type        = number
  default     = 0
  validation {
    condition     = var.nshards >= 0
    error_message = "nshards must be 0 (replica set) or greater (sharded)."
  }
}

variable "config_server_type" {
  description = "Config server type for sharded clusters. 'embedded' (collocated with shard nodes) or 'dedicated' (separate M30 config servers). Only applies when sharded (nshards > 0, or privatelink_enabled = true which forces sharding - see var.nshards)."
  type        = string
  default     = "embedded"
  validation {
    condition     = contains(["embedded", "dedicated"], var.config_server_type)
    error_message = "config_server_type must be 'embedded' or 'dedicated'."
  }
}

variable "atlas_disk_size_gb" {
  description = "Disk size in GB per electable node. 0 = use Atlas default."
  type        = number
  default     = 0
}

variable "atlas_backup_enabled" {
  description = "Enable Atlas continuous cloud backups."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Database user
# ---------------------------------------------------------------------------

variable "db_username" {
  description = "Username for the Atlas database user."
  type        = string
  default     = "distkafkaApp"
}

variable "db_password" {
  description = "Password for the Atlas database user. Supply via TF_VAR_db_password."
  type        = string
  sensitive   = true
}

variable "db_name" {
  description = "Application database name. The connector watches the 'payments' collection within this database."
  type        = string
  default     = "bank"
}

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

variable "kafka_version" {
  description = "Apache Kafka release version. Must be a currently supported release (see https://kafka.apache.org/downloads) so downloads resolve via Apache's fast mirror network instead of the throttled archive.apache.org tier. Kafka 4.x runs KRaft-only (no ZooKeeper)."
  type        = string
  default     = "4.3.1"
}

variable "kafka_topic" {
  description = "Kafka topic the MongoDB Source Connector publishes change events to."
  type        = string
  default     = "bank.payments"
}

variable "kafka_read_preference" {
  description = "MongoDB read preference used by both hosts' Kafka Source Connectors for reading the change stream (appended to connection.uri as ?readPreference=...). Default \"primary\" reads from the Atlas primary (London/EU_WEST_2) regardless of which region the connector runs in - meaning Ireland's connector always pays a cross-region network hop to read the oplog. Set to \"nearest\" or \"secondaryPreferred\" to let Ireland's connector read from a local Ireland secondary instead, trading that network hop for that secondary's own replication lag - useful for A/B testing which is faster. See scripts/run_multiregion_load_test.sh."
  type        = string
  default     = "primary"
}

# ---------------------------------------------------------------------------
# EC2
# ---------------------------------------------------------------------------

variable "ec2_instance_type" {
  description = "EC2 instance type for jpclient1 and jpclient2. Must have enough RAM to run both the Kafka broker JVM and the Kafka Connect (MongoDB source connector) JVM concurrently - t3.small (2GB) OOM-kills Connect under this load; t3.medium (4GB) is the minimum comfortable size."
  type        = string
  default     = "t3.medium"
}

variable "ec2_volume_size" {
  description = "Root EBS volume size in GB for each EC2 host."
  type        = number
  default     = 20
}

variable "key_pair_name" {
  description = "Name for the generated AWS key pair."
  type        = string
  default     = "distkafka-ec2-key"
}

variable "java_version" {
  description = "Amazon Corretto major version to install."
  type        = string
  default     = "21"
}

# ---------------------------------------------------------------------------
# PrivateLink (placeholder, initially disabled)
# ---------------------------------------------------------------------------

variable "privatelink_enabled" {
  description = "Enable PrivateLink between EC2 hosts and Atlas, replacing the public IP access list. Forces the cluster to sharded (see var.nshards) and creates one PrivateLink endpoint per region the cluster spans (London, Dublin, Frankfurt - Atlas requires an endpoint in every region before it populates private connection strings at all). London and Dublin are additionally cross-connected via a Transit Gateway peering link (see transit_gateway.tf) so each host can reach the OTHER region's endpoint too - Atlas's API only allows one consumer per regional PrivateLink service, so this cross-region reachability has to come from network routing (TGW), not a second Atlas registration. Both hosts get one combined mongos seed-list connection string covering every reachable region, giving automatic driver-level failover if either region's endpoint becomes unavailable. See ARCHITECTURE.md for the full design and the real-world gotchas (CIDR overlap, route table quirks) this required working around."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# Resource tags (provider-level default_tags)
# ---------------------------------------------------------------------------

variable "tag_owner" {
  type        = string
  default     = "john.page"
  description = "Value for the 'owner' default tag."
}

variable "tag_purpose" {
  type        = string
  default     = "other"
  description = "Value for the 'purpose' default tag."
}

variable "tag_expire_on" {
  type        = string
  default     = null
  description = "Value for the 'expire_on' default tag. Auto-computes as 48h from plan time if unset."
}