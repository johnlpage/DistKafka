locals {
  # readPreference is appended as a URI query param (the mongo-kafka
  # connector has no separate read-preference setting of its own - it
  # builds its MongoClient directly from this connection string and
  # calls .watch() on it with no override, so read preference is fully
  # inherited from here). Harmless for the load-test producer/consumer
  # scripts too, which also use this URI: read preference only affects
  # reads, and writes always route to the primary regardless.
  #
  # When PrivateLink is enabled, BOTH hosts get the SAME combined
  # connection string listing every mongos router from BOTH regions.
  # London and Dublin each have a local Atlas PrivateLink endpoint and
  # consumer. Frankfurt's Atlas service is consumed by a cross-region
  # interface endpoint hosted in Dublin; no AWS resources are created in
  # Frankfurt.
  #
  # This combined seed list gives "prefer local, fall back to remote"
  # behaviour for free: the MongoDB driver's own server-selection logic
  # picks the lowest-latency reachable host, which will naturally be
  # the local region's mongos under normal conditions, and the other
  # region's if the local one becomes unreachable - no custom retry
  # logic needed anywhere (producer/consumer scripts or the Kafka
  # connector, which just builds its MongoClient from connection.uri).
  #
  # AWS cross-region PrivateLink is used for Frankfurt: the Dublin VPC
  # endpoint sets service_region to eu-central-1 and Atlas accepts
  # EU_WEST_1 as a supported remote endpoint region.
  private_endpoints_list = var.privatelink_enabled ? coalesce(
    mongodbatlas_advanced_cluster.this.connection_strings.private_endpoint, []
  ) : []

  accessible_private_endpoints = [
    for pe in local.private_endpoints_list : pe
    if pe.type == "MONGOS" && length([
      for e in pe.endpoints : e.endpoint_id
      if contains([
        aws_vpc_endpoint.london[0].id,
        aws_vpc_endpoint.ireland[0].id,
        aws_vpc_endpoint.frankfurt_from_ireland[0].id,
      ], e.endpoint_id)
    ]) > 0
  ]

  mongo_uri_public = mongodbatlas_advanced_cluster.this.connection_strings.standard_srv

  # Use Atlas's own PrivateLink SRV record (e.g.
  # "mongodb+srv://<cluster>-pl-0.<id>.mongodb.net") rather than
  # manually expanding the non-SRV connection_string into an explicit
  # host:port seed list. Functionally equivalent (the SRV record
  # resolves to the exact same mongos set, and the driver's own
  # topology monitoring marks unreachable members as such regardless
  # of URI form - see ARCHITECTURE.md), but the SRV form auto-updates
  # if Atlas ever changes the underlying mongos topology, and doesn't
  # require this file to keep reconstructing/parsing connection
  # strings by hand.
  mongo_uri_private_srv = length(local.accessible_private_endpoints) > 0 ? local.accessible_private_endpoints[0].srv_connection_string : null

  # Atlas doesn't populate connection_strings.private_endpoint in the
  # SAME apply that creates the endpoint-linking resources (there's no
  # explicit attribute reference tying mongodbatlas_advanced_cluster to
  # mongodbatlas_privatelink_endpoint_service, so Terraform has no way
  # to know it needs to defer evaluation here) - so gracefully fall
  # back to the public URI whenever the private one isn't ready yet,
  # rather than hard-failing the plan. Expect a second `terraform
  # apply` to be needed after the endpoints go AVAILABLE before the
  # private connection strings actually show up.
  mongo_uri_base = coalesce(local.mongo_uri_private_srv, local.mongo_uri_public)

  mongo_uri = "${replace(
    local.mongo_uri_base,
    "mongodb+srv://",
    "mongodb+srv://${urlencode(var.db_username)}:${urlencode(var.db_password)}@"
  )}/?readPreference=${var.kafka_read_preference}"

  # jpclient1 = London (EU_WEST_2) = same region as the Atlas cluster's
  # primary (priority 7, the highest of the three regions - see
  # atlas.tf). jpclient2 = Ireland (EU_WEST_1, priority 6). Embedding a
  # human-readable label in each host's .env lets the load-test scripts
  # print which client is running and its proximity to the primary,
  # without needing external context to interpret latency results.
  rendered_env_jpclient1 = templatefile("${path.module}/templates/env.tpl", {
    mongo_uri       = local.mongo_uri
    db_name         = var.db_name
    collection_name = "payments"
    kafka_topic     = var.kafka_topic
    client_label    = "London (EU_WEST_2) - same region as Atlas primary"
  })

  rendered_env_jpclient2 = templatefile("${path.module}/templates/env.tpl", {
    mongo_uri       = local.mongo_uri
    db_name         = var.db_name
    collection_name = "payments"
    kafka_topic     = var.kafka_topic
    client_label    = "Ireland (EU_WEST_1) - secondary Atlas region"
  })
}

# ---------------------------------------------------------------------------
# Stage 1: Bootstrap Kafka (runs concurrently with Atlas creation)
# ---------------------------------------------------------------------------

resource "terraform_data" "bootstrap_jpclient1" {
  triggers_replace = [
    filemd5("${path.module}/scripts/setup-kafka.sh"),
    var.kafka_version,
  ]

  connection {
    type        = "ssh"
    host        = aws_eip.jpclient1.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "15m"
  }

  provisioner "remote-exec" {
    inline = ["cloud-init status --wait"]
  }

  provisioner "file" {
    source      = "${path.module}/scripts/setup-kafka.sh"
    destination = "/home/ec2-user/setup-kafka.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/setup-kafka.sh",
      "sudo env KAFKA_VERSION=${var.kafka_version} KAFKA_TOPIC=${var.kafka_topic} /home/ec2-user/setup-kafka.sh",
    ]
  }

  depends_on = [
    aws_instance.jpclient1,
    aws_eip.jpclient1,
  ]
}

resource "terraform_data" "bootstrap_jpclient2" {
  triggers_replace = [
    filemd5("${path.module}/scripts/setup-kafka.sh"),
    var.kafka_version,
  ]

  connection {
    type        = "ssh"
    host        = aws_eip.jpclient2.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "15m"
  }

  provisioner "remote-exec" {
    inline = ["cloud-init status --wait"]
  }

  provisioner "file" {
    source      = "${path.module}/scripts/setup-kafka.sh"
    destination = "/home/ec2-user/setup-kafka.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/setup-kafka.sh",
      "sudo env KAFKA_VERSION=${var.kafka_version} KAFKA_TOPIC=${var.kafka_topic} /home/ec2-user/setup-kafka.sh",
    ]
  }

  depends_on = [
    aws_instance.jpclient2,
    aws_eip.jpclient2,
  ]
}

# ---------------------------------------------------------------------------
# Stage 2: Install MongoDB Kafka Connector + Python deps
# ---------------------------------------------------------------------------

resource "terraform_data" "connector_jpclient1" {
  triggers_replace = [
    filemd5("${path.module}/scripts/setup-connector.sh"),
    sha256(local.rendered_env_jpclient1),
  ]

  connection {
    type        = "ssh"
    host        = aws_eip.jpclient1.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "15m"
  }

  provisioner "remote-exec" {
    inline = ["cloud-init status --wait"]
  }

  provisioner "file" {
    content     = local.rendered_env_jpclient1
    destination = "/home/ec2-user/.env"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/setup-connector.sh"
    destination = "/home/ec2-user/setup-connector.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod 600 /home/ec2-user/.env",
      "chmod +x /home/ec2-user/setup-connector.sh",
      "sudo /home/ec2-user/setup-connector.sh",
    ]
  }

  depends_on = [
    terraform_data.bootstrap_jpclient1,
    mongodbatlas_advanced_cluster.this,
    mongodbatlas_database_user.app,
    mongodbatlas_project_ip_access_list.jpclient1,
  ]
}

resource "terraform_data" "connector_jpclient2" {
  triggers_replace = [
    filemd5("${path.module}/scripts/setup-connector.sh"),
    sha256(local.rendered_env_jpclient2),
  ]

  connection {
    type        = "ssh"
    host        = aws_eip.jpclient2.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "15m"
  }

  provisioner "remote-exec" {
    inline = ["cloud-init status --wait"]
  }

  provisioner "file" {
    content     = local.rendered_env_jpclient2
    destination = "/home/ec2-user/.env"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/setup-connector.sh"
    destination = "/home/ec2-user/setup-connector.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod 600 /home/ec2-user/.env",
      "chmod +x /home/ec2-user/setup-connector.sh",
      "sudo /home/ec2-user/setup-connector.sh",
    ]
  }

  depends_on = [
    terraform_data.bootstrap_jpclient2,
    mongodbatlas_advanced_cluster.this,
    mongodbatlas_database_user.app,
    mongodbatlas_project_ip_access_list.jpclient2,
  ]
}

# ---------------------------------------------------------------------------
# Stage 3: Upload Python test scripts + run end-to-end smoke test
# ---------------------------------------------------------------------------

resource "terraform_data" "test_jpclient1" {
  triggers_replace = [
    filemd5("${path.module}/scripts/producer.py"),
    filemd5("${path.module}/scripts/consumer.py"),
  ]

  connection {
    type        = "ssh"
    host        = aws_eip.jpclient1.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "5m"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/producer.py"
    destination = "/home/ec2-user/producer.py"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/consumer.py"
    destination = "/home/ec2-user/consumer.py"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/producer.py /home/ec2-user/consumer.py",
      # Insert a document from this host
      "echo '--- Inserting document from jpclient1 ---'",
      "/home/ec2-user/producer.py",
      # Consume from Kafka topic and write CSV
      "echo '--- Consuming from Kafka ---'",
      "/home/ec2-user/consumer.py",
      "echo '--- Results ---'",
      "cat /home/ec2-user/kafka_results.csv 2>/dev/null || echo 'No results yet'",
    ]
  }

  depends_on = [
    terraform_data.connector_jpclient1,
  ]
}

resource "terraform_data" "test_jpclient2" {
  triggers_replace = [
    filemd5("${path.module}/scripts/producer.py"),
    filemd5("${path.module}/scripts/consumer.py"),
  ]

  connection {
    type        = "ssh"
    host        = aws_eip.jpclient2.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "5m"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/producer.py"
    destination = "/home/ec2-user/producer.py"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/consumer.py"
    destination = "/home/ec2-user/consumer.py"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/producer.py /home/ec2-user/consumer.py",
      # Insert a document from this host
      "echo '--- Inserting document from jpclient2 ---'",
      "/home/ec2-user/producer.py",
      # Consume from Kafka topic and write CSV
      "echo '--- Consuming from Kafka ---'",
      "/home/ec2-user/consumer.py",
      "echo '--- Results ---'",
      "cat /home/ec2-user/kafka_results.csv 2>/dev/null || echo 'No results yet'",
    ]
  }

  depends_on = [
    terraform_data.connector_jpclient2,
  ]
}

# ---------------------------------------------------------------------------
# Stage 4: Upload load-test scripts (not run automatically - these insert
# thousands of documents and can take several minutes, so they're meant to
# be invoked manually over SSH: ./run_load_test.sh [count])
# ---------------------------------------------------------------------------

resource "terraform_data" "loadtest_jpclient1" {
  triggers_replace = [
    filemd5("${path.module}/scripts/load_producer.py"),
    filemd5("${path.module}/scripts/load_consumer.py"),
    filemd5("${path.module}/scripts/compute_load_stats.py"),
    filemd5("${path.module}/scripts/run_load_test.sh"),
  ]

  connection {
    type        = "ssh"
    host        = aws_eip.jpclient1.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "5m"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/load_producer.py"
    destination = "/home/ec2-user/load_producer.py"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/load_consumer.py"
    destination = "/home/ec2-user/load_consumer.py"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/compute_load_stats.py"
    destination = "/home/ec2-user/compute_load_stats.py"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/run_load_test.sh"
    destination = "/home/ec2-user/run_load_test.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/load_producer.py /home/ec2-user/load_consumer.py /home/ec2-user/compute_load_stats.py /home/ec2-user/run_load_test.sh",
    ]
  }

  depends_on = [
    terraform_data.connector_jpclient1,
  ]
}

resource "terraform_data" "loadtest_jpclient2" {
  triggers_replace = [
    filemd5("${path.module}/scripts/load_producer.py"),
    filemd5("${path.module}/scripts/load_consumer.py"),
    filemd5("${path.module}/scripts/compute_load_stats.py"),
    filemd5("${path.module}/scripts/run_load_test.sh"),
  ]

  connection {
    type        = "ssh"
    host        = aws_eip.jpclient2.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "5m"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/load_producer.py"
    destination = "/home/ec2-user/load_producer.py"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/load_consumer.py"
    destination = "/home/ec2-user/load_consumer.py"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/compute_load_stats.py"
    destination = "/home/ec2-user/compute_load_stats.py"
  }

  provisioner "file" {
    source      = "${path.module}/scripts/run_load_test.sh"
    destination = "/home/ec2-user/run_load_test.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/load_producer.py /home/ec2-user/load_consumer.py /home/ec2-user/compute_load_stats.py /home/ec2-user/run_load_test.sh",
    ]
  }

  depends_on = [
    terraform_data.connector_jpclient2,
  ]
}
