data "http" "my_public_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  my_cidr          = "${trimspace(data.http.my_public_ip.response_body)}/32"
  auto_expire_time = timeadd(plantimestamp(), "48h")

  common_tags = {
    owner     = var.tag_owner
    expire_on = coalesce(var.tag_expire_on, local.auto_expire_time)
    purpose   = var.tag_purpose
  }

  # PrivateLink's cross-region mongos-seed-list failover approach (see
  # provisions.tf) only works for sharded clusters - mongos routers are
  # stateless query routers, so combining routers from two regions into
  # one seed list is safe (no per-endpoint topology-rediscovery risk
  # the way there would be with raw replica-set members). So enabling
  # PrivateLink defaults the cluster to sharded (1 shard, embedded
  # config servers - var.config_server_type's own default) unless you
  # explicitly ask for more shards via var.nshards.
  effective_shard_count = var.privatelink_enabled ? max(var.nshards, 1) : (var.nshards > 0 ? var.nshards : 1)
  is_sharded            = var.nshards > 0 || var.privatelink_enabled
}

provider "aws" {
  region = "eu-west-1"

  default_tags {
    tags = local.common_tags
  }

  ignore_tags {
    keys = [
      "mongodb:infosec:creationTime",
      "mongodb:infosec:lastModifiedTime",
      "mongodb:infosec:creatorIAMRole",
      "mongodb:infosec:creatorIAMUser",
      "mongodb:infosec:creator",
      "mongodb:infosec:WhatIsThis",
    ]
  }
}

provider "aws" {
  alias  = "ireland"
  region = "eu-west-1"

  default_tags {
    tags = local.common_tags
  }

  ignore_tags {
    keys = [
      "mongodb:infosec:creationTime",
      "mongodb:infosec:lastModifiedTime",
      "mongodb:infosec:creatorIAMRole",
      "mongodb:infosec:creatorIAMUser",
      "mongodb:infosec:creator",
      "mongodb:infosec:WhatIsThis",
    ]
  }
}

provider "aws" {
  alias  = "london"
  region = "eu-west-2"

  default_tags {
    tags = local.common_tags
  }

  ignore_tags {
    keys = [
      "mongodb:infosec:creationTime",
      "mongodb:infosec:lastModifiedTime",
      "mongodb:infosec:creatorIAMRole",
      "mongodb:infosec:creatorIAMUser",
      "mongodb:infosec:creator",
      "mongodb:infosec:WhatIsThis",
    ]
  }
}

provider "mongodbatlas" {}
