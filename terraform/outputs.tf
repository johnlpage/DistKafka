output "atlas_cluster_name" {
  description = "Name of the Atlas cluster."
  value       = mongodbatlas_advanced_cluster.this.name
}

output "atlas_connection_string_srv" {
  description = "Standard SRV connection string for the Atlas cluster (public endpoint)."
  value       = mongodbatlas_advanced_cluster.this.connection_strings.standard_srv
}

output "atlas_private_connection_strings_srv" {
  description = "PrivateLink SRV connection strings for the Atlas MONGOS endpoints used by the EC2 clients; empty until PrivateLink is enabled and its connection strings are available."
  value       = [for endpoint in local.accessible_private_endpoints : endpoint.srv_connection_string]
}

output "atlas_num_shards" {
  description = "Current number of shards deployed."
  value       = var.nshards
}

output "jpclient_london_public_ip" {
  description = "Static public (Elastic) IP of the London EC2 client (jpclient1) - same region as the Atlas cluster's primary (EU_WEST_2, priority 7)."
  value       = aws_eip.jpclient1.public_ip
}

output "jpclient_ireland_public_ip" {
  description = "Static public (Elastic) IP of the Ireland EC2 client (jpclient2) - a secondary Atlas region (EU_WEST_1, priority 6)."
  value       = aws_eip.jpclient2.public_ip
}

output "jpclient_london_hostname" {
  description = "DNS hostname for the London EC2 client."
  value       = aws_route53_record.jpclient1.name
}

output "jpclient_ireland_hostname" {
  description = "DNS hostname for the Ireland EC2 client."
  value       = aws_route53_record.jpclient2.name
}

output "ssh_private_key_path" {
  description = "Local path to the generated SSH private key file."
  value       = local_sensitive_file.private_key.filename
}

output "ssh_command_london" {
  description = "Command to SSH into the London client (same region as the Atlas primary) with local port forwarding."
  value       = "ssh -i ${local_sensitive_file.private_key.filename} -L 9092:localhost:9092 -L 8083:localhost:8083 ec2-user@${aws_eip.jpclient1.public_ip}"
}

output "ssh_command_ireland" {
  description = "Command to SSH into the Ireland client with local port forwarding."
  value       = "ssh -i ${local_sensitive_file.private_key.filename} -L 9092:localhost:9092 -L 8083:localhost:8083 ec2-user@${aws_eip.jpclient2.public_ip}"
}
