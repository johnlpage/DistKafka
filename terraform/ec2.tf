# ---------------------------------------------------------------------------
# AMI lookup (per region)
# ---------------------------------------------------------------------------

data "aws_ami" "al2023_ireland" {
  provider    = aws.ireland
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_ami" "al2023_london" {
  provider    = aws.london
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ---------------------------------------------------------------------------
# Default VPC + subnet per region
# ---------------------------------------------------------------------------

data "aws_vpc" "default_ireland" {
  provider = aws.ireland
  default  = true
}

data "aws_vpc" "default_london" {
  provider = aws.london
  default  = true
}

# Frankfurt has no EC2 client, but needs a default VPC lookup too - see
# providers.tf's aws.frankfurt alias for why (PrivateLink requires an
# endpoint in every region the Atlas cluster spans, not just the two
# regions with actual EC2 hosts).
data "aws_vpc" "default_frankfurt" {
  count    = var.privatelink_enabled ? 1 : 0
  provider = aws.frankfurt
  default  = true
}

data "aws_subnets" "default_ireland" {
  provider = aws.ireland
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default_ireland.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_subnets" "default_london" {
  provider = aws.london
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default_london.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_subnets" "default_frankfurt" {
  count    = var.privatelink_enabled ? 1 : 0
  provider = aws.frankfurt
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default_frankfurt[0].id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# ---------------------------------------------------------------------------
# SSH key pair
# ---------------------------------------------------------------------------

resource "tls_private_key" "ssh" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "ireland" {
  provider   = aws.ireland
  key_name   = var.key_pair_name
  public_key = tls_private_key.ssh.public_key_openssh
}

resource "aws_key_pair" "london" {
  provider   = aws.london
  key_name   = var.key_pair_name
  public_key = tls_private_key.ssh.public_key_openssh
}

resource "local_sensitive_file" "private_key" {
  content         = tls_private_key.ssh.private_key_pem
  filename        = "${path.module}/${var.key_pair_name}.pem"
  file_permission = "0600"
}

# ---------------------------------------------------------------------------
# Security groups (one per region)
# ---------------------------------------------------------------------------

resource "aws_security_group" "jpclient2" {
  provider    = aws.ireland
  name        = "distkafka-jpclient2-sg"
  description = "jpclient2 (Ireland) - SSH + Kafka from deployer IP, all outbound"
  vpc_id      = data.aws_vpc.default_ireland.id

  ingress {
    description = "SSH from deployer IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
  }

  ingress {
    description = "Kafka plaintext from deployer IP"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "jpclient1" {
  provider    = aws.london
  name        = "distkafka-jpclient1-sg"
  description = "jpclient1 (London) - SSH + Kafka from deployer IP, all outbound"
  vpc_id      = data.aws_vpc.default_london.id

  ingress {
    description = "SSH from deployer IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
  }

  ingress {
    description = "Kafka plaintext from deployer IP"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------------------------------------------------------------------
# EC2 instances
# ---------------------------------------------------------------------------

resource "aws_instance" "jpclient2" {
  provider      = aws.ireland
  ami           = data.aws_ami.al2023_ireland.id
  instance_type = var.ec2_instance_type
  # When PrivateLink is enabled, this instance lives in the SAME
  # dedicated non-overlapping-CIDR subnet as its own region's
  # PrivateLink endpoint (see transit_gateway.tf) - bidirectional TGW
  # routing to/from London requires BOTH the instance and the endpoint
  # to be outside the overlapping 172.31.0.0/16 range both regions'
  # default VPCs share; moving only the endpoint left the return path
  # unroutable (Ireland's VPC always treats 172.31.0.0/16 as local,
  # so it can never forward return traffic for it out via TGW).
  subnet_id                   = var.privatelink_enabled ? aws_subnet.privatelink_ireland[0].id : data.aws_subnets.default_ireland.ids[0]
  vpc_security_group_ids      = [aws_security_group.jpclient2.id]
  key_name                    = aws_key_pair.ireland.key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size           = var.ec2_volume_size
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail
    dnf install -y java-${var.java_version}-amazon-corretto-devel nc
    java -version
    cat >> /etc/ssh/sshd_config <<'SSHD'
    ClientAliveInterval 60
    ClientAliveCountMax 1440
    SSHD
    systemctl reload sshd

  EOF

  tags = { Name = "jpclient2-ireland" }
}

resource "aws_instance" "jpclient1" {
  provider                    = aws.london
  ami                         = data.aws_ami.al2023_london.id
  instance_type               = var.ec2_instance_type
  subnet_id                   = var.privatelink_enabled ? aws_subnet.privatelink_london[0].id : data.aws_subnets.default_london.ids[0]
  vpc_security_group_ids      = [aws_security_group.jpclient1.id]
  key_name                    = aws_key_pair.london.key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size           = var.ec2_volume_size
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail
    dnf install -y java-${var.java_version}-amazon-corretto-devel nc
    java -version
    cat >> /etc/ssh/sshd_config <<'SSHD'
    ClientAliveInterval 60
    ClientAliveCountMax 1440
    SSHD
    systemctl reload sshd

  EOF

  tags = { Name = "jpclient1-london" }
}

# ---------------------------------------------------------------------------
# Elastic IPs
# ---------------------------------------------------------------------------

resource "aws_eip" "jpclient2" {
  provider = aws.ireland
  instance = aws_instance.jpclient2.id
  domain   = "vpc"
  tags     = { Name = "jpclient2-eip" }
}

resource "aws_eip" "jpclient1" {
  provider = aws.london
  instance = aws_instance.jpclient1.id
  domain   = "vpc"
  tags     = { Name = "jpclient1-eip" }
}

# ---------------------------------------------------------------------------
# Route53 DNS records
# ---------------------------------------------------------------------------

data "aws_route53_zone" "mongosa" {
  name         = "mongosa.net."
  private_zone = false
}

resource "aws_route53_record" "jpclient1" {
  zone_id = data.aws_route53_zone.mongosa.zone_id
  name    = "jpclient-london.mongosa.net"
  type    = "A"
  ttl     = 30
  records = [aws_eip.jpclient1.public_ip]
}

resource "aws_route53_record" "jpclient2" {
  zone_id = data.aws_route53_zone.mongosa.zone_id
  name    = "jpclient-ireland.mongosa.net"
  type    = "A"
  ttl     = 30
  records = [aws_eip.jpclient2.public_ip]
}

# ---------------------------------------------------------------------------
# PrivateLink: AWS-side interface endpoints (one per region the Atlas
# cluster spans - see providers.tf/atlas.tf for why Frankfurt is
# required even with no EC2 client there)
# ---------------------------------------------------------------------------
#
# Security groups: per AWS's own PrivateLink troubleshooting guidance,
# the interface endpoint must accept inbound traffic on ALL ports in
# 1024-65535, not just 27017 - Atlas's load balancer multiplexes each
# replica-set member onto a different port on the same private IP (e.g.
# node 1 on :1024, node 2 on :1025, etc., as shown by `nslookup -type=SRV`
# against a PrivateLink-aware connection string).

resource "aws_security_group" "privatelink_london" {
  count       = var.privatelink_enabled ? 1 : 0
  provider    = aws.london
  name        = "distkafka-privatelink-london-sg"
  description = "Inbound from the VPC to the Atlas PrivateLink interface endpoint (London)"
  vpc_id      = data.aws_vpc.default_london.id

  # jpclient1 and this endpoint now share the same dedicated subnet
  # (see aws_subnet.privatelink_london / aws_instance.jpclient1) - and
  # Ireland's EC2 instance now lives in its own dedicated,
  # non-overlapping subnet too, reached via the cross-region Transit
  # Gateway link (transit_gateway.tf). Two distinct rules since the
  # CIDRs genuinely differ now (no more accidental duplicate-rule
  # rejection from both regions sharing 172.31.0.0/16).
  ingress {
    description = "Atlas mongod/mongos ports, from London own dedicated subnet"
    from_port   = 1024
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [aws_subnet.privatelink_london[0].cidr_block]
  }

  ingress {
    description = "Same, from Ireland dedicated subnet via cross-region Transit Gateway"
    from_port   = 1024
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [aws_subnet.privatelink_ireland[0].cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "privatelink_ireland" {
  count       = var.privatelink_enabled ? 1 : 0
  provider    = aws.ireland
  name        = "distkafka-privatelink-ireland-sg"
  description = "Inbound from the VPC to the Atlas PrivateLink interface endpoint (Ireland)"
  vpc_id      = data.aws_vpc.default_ireland.id

  # See privatelink_london's identical comment.
  ingress {
    description = "Atlas mongod/mongos ports, from Ireland own dedicated subnet"
    from_port   = 1024
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [aws_subnet.privatelink_ireland[0].cidr_block]
  }

  ingress {
    description = "Same, from London dedicated subnet via cross-region Transit Gateway"
    from_port   = 1024
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [aws_subnet.privatelink_london[0].cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "privatelink_frankfurt" {
  count       = var.privatelink_enabled ? 1 : 0
  provider    = aws.frankfurt
  name        = "distkafka-privatelink-frankfurt-sg"
  description = "Inbound from the VPC to the Atlas PrivateLink interface endpoint (Frankfurt - no EC2 client here, endpoint only exists to satisfy the all-regions-or-none PrivateLink requirement)"
  vpc_id      = data.aws_vpc.default_frankfurt[0].id

  ingress {
    description = "Atlas mongod/mongos ports via PrivateLink load balancer"
    from_port   = 1024
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default_frankfurt[0].cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc_endpoint" "london" {
  count             = var.privatelink_enabled ? 1 : 0
  provider          = aws.london
  vpc_id            = data.aws_vpc.default_london.id
  service_name      = mongodbatlas_privatelink_endpoint.london[0].endpoint_service_name
  vpc_endpoint_type = "Interface"
  # Dedicated non-overlapping-CIDR subnet (see transit_gateway.tf) so
  # Ireland's EC2 host can reach this endpoint's ENI via TGW - AWS
  # rejects cross-VPC routes into the shared/overlapping default
  # 172.31.0.0/16 range both regions' default VPCs use.
  subnet_ids         = [aws_subnet.privatelink_london[0].id]
  security_group_ids = [aws_security_group.privatelink_london[0].id]
}

resource "aws_vpc_endpoint" "ireland" {
  count              = var.privatelink_enabled ? 1 : 0
  provider           = aws.ireland
  vpc_id             = data.aws_vpc.default_ireland.id
  service_name       = mongodbatlas_privatelink_endpoint.ireland[0].endpoint_service_name
  vpc_endpoint_type  = "Interface"
  subnet_ids         = [aws_subnet.privatelink_ireland[0].id]
  security_group_ids = [aws_security_group.privatelink_ireland[0].id]
}

resource "aws_vpc_endpoint" "frankfurt" {
  count              = var.privatelink_enabled ? 1 : 0
  provider           = aws.frankfurt
  vpc_id             = data.aws_vpc.default_frankfurt[0].id
  service_name       = mongodbatlas_privatelink_endpoint.frankfurt[0].endpoint_service_name
  vpc_endpoint_type  = "Interface"
  subnet_ids         = data.aws_subnets.default_frankfurt[0].ids
  security_group_ids = [aws_security_group.privatelink_frankfurt[0].id]
}
