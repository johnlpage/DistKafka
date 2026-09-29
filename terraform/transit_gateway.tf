# ---------------------------------------------------------------------------
# Cross-region Transit Gateway: lets London's EC2 host reach Ireland's
# regional PrivateLink endpoint (and vice versa) purely via network
# routing - NOT by registering a second consumer with Atlas. Atlas only
# allows one consumer attachment per regional PrivateLink service (see
# atlas.tf's comments for how we found this out the hard way), so
# cross-region fallback has to work by routing traffic to the ONE
# already-registered endpoint in each region, rather than creating a
# second endpoint there.
#
# TGW is a regional resource, so cross-region connectivity needs one
# TGW per region plus a TGW peering attachment between them (the
# standard AWS pattern for cross-region TGW).
#
# IMPORTANT CIDR gotcha: both regions' *default* VPCs use the identical
# 172.31.0.0/16 range (standard for AWS default VPCs). VPC-to-VPC
# routing (TGW OR peering) fundamentally requires non-overlapping
# CIDRs - AWS rejects any route whose destination is "equal to or more
# specific than" the VPC's own local CIDR, which the shared
# 172.31.0.0/16 range always is on both sides. Rather than migrating
# both regions to brand new custom VPCs (a much bigger, riskier
# change), each region's PrivateLink endpoint ENI is placed in a small
# dedicated subnet carved from a secondary, non-overlapping CIDR block
# instead - the EC2 instances themselves are untouched, still living in
# their original default subnets. London's default VPC happened to
# already have an unused secondary CIDR (172.32.0.0/16, confirmed via
# `aws ec2 describe-subnets` - no existing subnets there); Ireland's
# didn't, so we add one (172.33.0.0/16).
# ---------------------------------------------------------------------------

resource "aws_vpc_ipv4_cidr_block_association" "ireland_privatelink" {
  count      = var.privatelink_enabled ? 1 : 0
  provider   = aws.ireland
  vpc_id     = data.aws_vpc.default_ireland.id
  cidr_block = "172.33.0.0/16"
}

resource "aws_subnet" "privatelink_london" {
  count             = var.privatelink_enabled ? 1 : 0
  provider          = aws.london
  vpc_id            = data.aws_vpc.default_london.id
  cidr_block        = "172.32.1.0/24"
  availability_zone = "eu-west-2a"
  tags              = { Name = "distkafka-privatelink-london-subnet" }
}

resource "aws_subnet" "privatelink_ireland" {
  count             = var.privatelink_enabled ? 1 : 0
  provider          = aws.ireland
  vpc_id            = data.aws_vpc.default_ireland.id
  cidr_block        = "172.33.1.0/24"
  availability_zone = "eu-west-1a"
  tags              = { Name = "distkafka-privatelink-ireland-subnet" }

  depends_on = [aws_vpc_ipv4_cidr_block_association.ireland_privatelink]
}

resource "aws_ec2_transit_gateway" "london" {
  count       = var.privatelink_enabled ? 1 : 0
  provider    = aws.london
  description = "distkafka - London side of cross-region link to Ireland's PrivateLink endpoint"
  tags        = { Name = "distkafka-tgw-london" }
}

resource "aws_ec2_transit_gateway" "ireland" {
  count       = var.privatelink_enabled ? 1 : 0
  provider    = aws.ireland
  description = "distkafka - Ireland side of cross-region link to London's PrivateLink endpoint"
  tags        = { Name = "distkafka-tgw-ireland" }
}

resource "aws_ec2_transit_gateway_vpc_attachment" "london" {
  count              = var.privatelink_enabled ? 1 : 0
  provider           = aws.london
  transit_gateway_id = aws_ec2_transit_gateway.london[0].id
  vpc_id             = data.aws_vpc.default_london.id
  subnet_ids         = [aws_subnet.privatelink_london[0].id]
  tags               = { Name = "distkafka-tgw-attach-london" }
}

resource "aws_ec2_transit_gateway_vpc_attachment" "ireland" {
  count              = var.privatelink_enabled ? 1 : 0
  provider           = aws.ireland
  transit_gateway_id = aws_ec2_transit_gateway.ireland[0].id
  vpc_id             = data.aws_vpc.default_ireland.id
  subnet_ids         = [aws_subnet.privatelink_ireland[0].id]
  tags               = { Name = "distkafka-tgw-attach-ireland" }
}

# ---------------------------------------------------------------------------
# Cross-region TGW peering (London <-> Ireland)
# ---------------------------------------------------------------------------

resource "aws_ec2_transit_gateway_peering_attachment" "london_to_ireland" {
  count                   = var.privatelink_enabled ? 1 : 0
  provider                = aws.london
  transit_gateway_id      = aws_ec2_transit_gateway.london[0].id
  peer_transit_gateway_id = aws_ec2_transit_gateway.ireland[0].id
  peer_region             = "eu-west-1"
  tags                    = { Name = "distkafka-tgw-peer-london-to-ireland" }
}

resource "aws_ec2_transit_gateway_peering_attachment_accepter" "ireland_accepts_london" {
  count                         = var.privatelink_enabled ? 1 : 0
  provider                      = aws.ireland
  transit_gateway_attachment_id = aws_ec2_transit_gateway_peering_attachment.london_to_ireland[0].id
  tags                          = { Name = "distkafka-tgw-peer-ireland-accepts-london" }
}

# ---------------------------------------------------------------------------
# TGW route table entries: peering attachments do NOT auto-propagate
# routes the way plain VPC attachments can, so these need to be added
# explicitly on both sides. Routes target the dedicated PrivateLink
# subnet CIDRs (non-overlapping), not the whole (overlapping) VPC CIDR.
# ---------------------------------------------------------------------------

resource "aws_ec2_transit_gateway_route" "london_to_ireland" {
  count                          = var.privatelink_enabled ? 1 : 0
  provider                       = aws.london
  destination_cidr_block         = aws_subnet.privatelink_ireland[0].cidr_block
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_peering_attachment.london_to_ireland[0].id
  transit_gateway_route_table_id = aws_ec2_transit_gateway.london[0].association_default_route_table_id
}

resource "aws_ec2_transit_gateway_route" "ireland_to_london" {
  count                          = var.privatelink_enabled ? 1 : 0
  provider                       = aws.ireland
  destination_cidr_block         = aws_subnet.privatelink_london[0].cidr_block
  transit_gateway_attachment_id  = aws_ec2_transit_gateway_peering_attachment_accepter.ireland_accepts_london[0].id
  transit_gateway_route_table_id = aws_ec2_transit_gateway.ireland[0].association_default_route_table_id
}

# ---------------------------------------------------------------------------
# VPC route table entries: send traffic for the OTHER region's dedicated
# PrivateLink subnet out via this region's own TGW attachment. These
# target the small dedicated /24s, not the whole (overlapping) VPC
# CIDR - AWS rejects routes whose destination overlaps your own local
# VPC CIDR ("equal to or more specific than"), which is exactly why the
# dedicated non-overlapping subnets exist in the first place.
#
# Both the EC2 instance AND its region's PrivateLink endpoint now live
# in this same dedicated subnet (see aws_instance.jpclient1/jpclient2's
# subnet_id and aws_subnet.privatelink_london/ireland) - a freshly
# created subnet with no explicit aws_route_table_association falls
# back to the VPC's implicit main route table, so that's the correct
# target here. (Earlier iteration of this file had a data-source-based
# lookup for "whichever table the instance's ORIGINAL subnet actually
# used" - no longer needed now that the instance has moved into this
# dedicated subnet instead of staying in the original default one.)
# ---------------------------------------------------------------------------

resource "aws_route" "london_to_ireland" {
  count                  = var.privatelink_enabled ? 1 : 0
  provider               = aws.london
  route_table_id         = data.aws_vpc.default_london.main_route_table_id
  destination_cidr_block = aws_subnet.privatelink_ireland[0].cidr_block
  transit_gateway_id     = aws_ec2_transit_gateway.london[0].id

  depends_on = [aws_ec2_transit_gateway_vpc_attachment.london]
}

resource "aws_route" "ireland_to_london" {
  count                  = var.privatelink_enabled ? 1 : 0
  provider               = aws.ireland
  route_table_id         = data.aws_vpc.default_ireland.main_route_table_id
  destination_cidr_block = aws_subnet.privatelink_london[0].cidr_block
  transit_gateway_id     = aws_ec2_transit_gateway.ireland[0].id

  depends_on = [aws_ec2_transit_gateway_vpc_attachment.ireland]
}
