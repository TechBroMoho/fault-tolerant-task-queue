# The benchmark stack (SPEC §7 Phase 7): ECS on the EC2 launch type, in one AZ of the
# default VPC, no NAT. Everything here is disposable: `make aws-down` destroys all of
# it after every session. The budget and the ECR repo live in ../base (ADR-045).
#
# Layout:
#   redis host (aws_instance, fixed private IP known at plan time) ── Redis task
#   worker ASG  (worker_hosts × c7i-flex.large)                  ── worker service
#   loadgen ASG (loadgen_hosts, may be 0)                        ── one-off loadgen tasks
# Tasks use host networking: no ENI per task (a .large has only 3), no NAT, no port
# mapping on the hot path. Placement is pinned by an instance attribute, ftq.role.

provider "aws" {
  region = "us-west-2"
  default_tags {
    tags = { project = "ftq" }
  }
}

data "aws_caller_identity" "current" {}

locals {
  name          = "ftq"
  image         = "${data.aws_caller_identity.current.account_id}.dkr.ecr.us-west-2.amazonaws.com/ftq:${var.image_tag}"
  loadgen_role  = var.loadgen_hosts > 0 ? "loadgen" : "redis"
  vcpus_per_box = 2 # every Free-plan-eligible type is 2 vCPU (PROGRESS, Phase 7 pre-flight)
  total_vcpus   = local.vcpus_per_box * (1 + var.worker_hosts + var.loadgen_hosts)
  redis_url     = "redis://${aws_instance.redis.private_ip}:6379/0"
}

# ---------------------------------------------------------------- guard rails ($0)

# The account's running on-demand vCPU quota, read live. A fleet over it would half-launch
# (the ASG keeps retrying and some tasks never place), so refuse at plan time instead.
data "aws_servicequotas_service_quota" "vcpus" {
  service_code = "ec2"
  quota_code   = "L-1216C47A"
}

# Free plan: stay on free-tier-eligible types, which are the ones the plan is known to
# allow. Checked here so a typo'd type fails the plan, not the launch.
data "aws_ec2_instance_type" "redis" {
  instance_type = var.redis_instance_type
}

data "aws_ec2_instance_type" "host" {
  instance_type = var.host_instance_type
}

resource "terraform_data" "guards" {
  lifecycle {
    # The quota is not a spending limit (AWS granted 64 when asked for 16), so the
    # approved size is enforced separately.
    precondition {
      condition     = local.total_vcpus <= var.max_vcpus
      error_message = "The fleet needs ${local.total_vcpus} vCPUs; the approved cap (max_vcpus) is ${var.max_vcpus}."
    }
    precondition {
      condition     = local.total_vcpus <= data.aws_servicequotas_service_quota.vcpus.value
      error_message = "The fleet needs ${local.total_vcpus} vCPUs; the account's quota is ${data.aws_servicequotas_service_quota.vcpus.value}."
    }
    precondition {
      condition     = data.aws_ec2_instance_type.redis.free_tier_eligible && data.aws_ec2_instance_type.host.free_tier_eligible
      error_message = "Only free-tier-eligible instance types (Free plan)."
    }
    # SPEC §7: no t-family. Their default "unlimited" credit mode bills extra under
    # sustained load, and a throttled burstable host would distort the numbers.
    precondition {
      condition     = !data.aws_ec2_instance_type.redis.burstable_performance_supported && !data.aws_ec2_instance_type.host.burstable_performance_supported
      error_message = "Burstable (t-family) types are not allowed for the benchmark fleet."
    }
    precondition {
      condition     = var.workers <= 2 * var.worker_hosts
      error_message = "Each worker reserves 1 vCPU: at most 2 per worker host."
    }
  }
}

# ---------------------------------------------------------------- network

data "aws_vpc" "default" {
  default = true
}

data "aws_subnet" "one" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = var.az
  default_for_az    = true
}

# One SG for every host. Redis is reachable only from members of this SG, never from the
# internet. No SSH: a shell, if ever needed, goes through SSM Session Manager.
resource "aws_security_group" "cluster" {
  name        = "${local.name}-cluster"
  description = "ftq ECS hosts: Redis from members only; no inbound from outside"
  vpc_id      = data.aws_vpc.default.id
}

resource "aws_vpc_security_group_ingress_rule" "redis_from_cluster" {
  security_group_id            = aws_security_group.cluster.id
  referenced_security_group_id = aws_security_group.cluster.id
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
  description                  = "Redis, from ftq hosts only"
}

# Outbound to the internet: the ECS agent, ECR, and CloudWatch Logs are reached over
# public endpoints (no NAT gateway, no VPC endpoints: both bill by the hour).
resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.cluster.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "ECS agent, ECR, CloudWatch Logs"
}

# ---------------------------------------------------------------- IAM

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "host" {
  name               = "${local.name}-ecs-host"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "host_ecs" {
  role       = aws_iam_role.host.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "host_ssm" {
  role       = aws_iam_role.host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "host" {
  name = "${local.name}-ecs-host"
  role = aws_iam_role.host.name
}

# Used by the ECS agent to pull the image and ship logs on the tasks' behalf.
resource "aws_iam_role" "execution" {
  name               = "${local.name}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ---------------------------------------------------------------- cluster + hosts

resource "aws_ecs_cluster" "ftq" {
  name = local.name
  # Container Insights bills per metric; the benchmark measures CPU itself (ADR-041).
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

# ECS-optimized Amazon Linux 2023, x86_64 (the image is built for linux/amd64).
data "aws_ssm_parameter" "ecs_ami" {
  name = "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"
}

locals {
  # Joins the cluster with a role attribute that task placement keys on.
  user_data = { for role in ["redis", "worker", "loadgen"] : role => <<-EOT
    #!/bin/bash
    set -euo pipefail
    cat >> /etc/ecs/ecs.config <<CFG
    ECS_CLUSTER=${aws_ecs_cluster.ftq.name}
    ECS_INSTANCE_ATTRIBUTES={"ftq.role":"${role}"}
    ECS_ENABLE_CONTAINER_METADATA=true
    CFG
    %{if role == "redis"~}
    # Redis's own startup warnings: fork needs overcommit, and THP adds latency spikes.
    sysctl -w vm.overcommit_memory=1
    sysctl -w net.core.somaxconn=4096
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
    %{endif~}
    EOT
  }
}

resource "aws_instance" "redis" {
  ami                    = data.aws_ssm_parameter.ecs_ami.value
  instance_type          = var.redis_instance_type
  subnet_id              = data.aws_subnet.one.id
  vpc_security_group_ids = [aws_security_group.cluster.id]
  iam_instance_profile   = aws_iam_instance_profile.host.name
  user_data              = local.user_data["redis"]
  # Reach ECR/ECS/Logs without a NAT gateway (the default subnet would do this anyway).
  associate_public_ip_address = true
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }
  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }
  tags = { Name = "${local.name}-redis" }

  depends_on = [terraform_data.guards]
}

resource "aws_launch_template" "host" {
  for_each      = toset(["worker", "loadgen"])
  name          = "${local.name}-${each.key}"
  image_id      = data.aws_ssm_parameter.ecs_ami.value
  instance_type = var.host_instance_type
  user_data     = base64encode(local.user_data[each.key])
  iam_instance_profile {
    name = aws_iam_instance_profile.host.name
  }
  network_interfaces {
    associate_public_ip_address = true # reach ECR/ECS/Logs without a NAT gateway
    security_groups             = [aws_security_group.cluster.id]
    delete_on_termination       = true
  }
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_type           = "gp3"
      volume_size           = 30
      delete_on_termination = true
    }
  }
  tag_specifications {
    resource_type = "instance"
    tags          = { Name = "${local.name}-${each.key}", project = "ftq" }
  }
  tag_specifications {
    resource_type = "volume"
    tags          = { project = "ftq" }
  }
}

# Fixed-size groups, not ECS capacity providers with managed scaling: a benchmark fleet
# must not grow or shrink mid-run, and a fixed group tears down cleanly (ADR-045).
resource "aws_autoscaling_group" "host" {
  for_each = { for role, n in { worker = var.worker_hosts, loadgen = var.loadgen_hosts } : role => n if n > 0 }

  name                = "${local.name}-${each.key}"
  min_size            = each.value
  max_size            = each.value
  desired_capacity    = each.value
  vpc_zone_identifier = [data.aws_subnet.one.id]
  launch_template {
    id      = aws_launch_template.host[each.key].id
    version = aws_launch_template.host[each.key].latest_version
  }
  tag {
    key                 = "project"
    value               = "ftq"
    propagate_at_launch = true
  }

  depends_on = [terraform_data.guards]
}

# ---------------------------------------------------------------- logs

resource "aws_cloudwatch_log_group" "task" {
  for_each          = toset(["redis", "worker", "loadgen"])
  name              = "/ftq/${each.key}"
  retention_in_days = 1
}

locals {
  # non-blocking: a slow log pipe must never stall a worker's event loop (heartbeats).
  log_config = { for name in ["redis", "worker", "loadgen"] : name => {
    logDriver = "awslogs"
    options = {
      "awslogs-group"         = aws_cloudwatch_log_group.task[name].name
      "awslogs-region"        = "us-west-2"
      "awslogs-stream-prefix" = name
      "mode"                  = "non-blocking"
      "max-buffer-size"       = "4m"
    }
  } }
  ulimits = [{ name = "nofile", softLimit = 65536, hardLimit = 65536 }]
}

# ---------------------------------------------------------------- tasks

resource "aws_ecs_task_definition" "redis" {
  family                   = "${local.name}-redis"
  network_mode             = "host"
  requires_compatibilities = ["EC2"]
  execution_role_arn       = aws_iam_role.execution.arn
  container_definitions = jsonencode([{
    name              = "redis"
    image             = "public.ecr.aws/docker/library/redis:8.8.3" # same pin as docker-compose.yml
    essential         = true
    memoryReservation = 5632
    ulimits           = local.ulimits
    command = [
      "redis-server",
      "--bind", "0.0.0.0",
      # No password: the SG admits only ftq hosts, and nothing else is in the VPC.
      "--protected-mode", "no",
      # Throughput runs: no AOF, no RDB snapshots (SPEC §7: say so). Redis durability is
      # outside the zero-loss claim anyway (SPEC §4, ADR-013).
      "--appendonly", "no",
      "--save", "",
      "--maxmemory", var.redis_maxmemory,
      "--maxmemory-policy", "noeviction",
      "--io-threads", tostring(var.redis_io_threads),
    ]
    healthCheck = {
      command  = ["CMD", "redis-cli", "ping"]
      interval = 5
      timeout  = 2
      retries  = 3
    }
    logConfiguration = local.log_config["redis"]
  }])
}

resource "aws_ecs_service" "redis" {
  name            = "redis"
  cluster         = aws_ecs_cluster.ftq.id
  task_definition = aws_ecs_task_definition.redis.arn
  desired_count   = 1
  launch_type     = "EC2"
  # A replacement Redis must start only after the old one is gone (same host port).
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  placement_constraints {
    type       = "memberOf"
    expression = "attribute:ftq.role == redis"
  }
}

locals {
  ftq_env = [
    { name = "FTQ_REDIS_URL", value = local.redis_url },
    { name = "FTQ_LOG_LEVEL", value = var.log_level },
    { name = "FTQ_LOG_FORMAT", value = "json" },
  ]
}

resource "aws_ecs_task_definition" "worker" {
  family                   = "${local.name}-worker"
  network_mode             = "host"
  requires_compatibilities = ["EC2"]
  execution_role_arn       = aws_iam_role.execution.arn
  container_definitions = jsonencode([{
    name              = "worker"
    image             = local.image
    essential         = true
    cpu               = 1024 # 1 vCPU reserved: 2 workers per 2-vCPU host, never more
    memoryReservation = 512
    ulimits           = local.ulimits
    command           = ["worker"]
    environment       = concat(local.ftq_env, [{ name = "FTQ_CONCURRENCY", value = tostring(var.worker_concurrency) }])
    # SIGTERM drains within FTQ_SHUTDOWN_GRACE (30 s); SIGKILL only after that.
    stopTimeout      = 40
    logConfiguration = local.log_config["worker"]
  }])
}

resource "aws_ecs_service" "worker" {
  name            = "worker"
  cluster         = aws_ecs_cluster.ftq.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.workers
  launch_type     = "EC2"
  # No spare capacity to roll into: replace in place.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  placement_constraints {
    type       = "memberOf"
    expression = "attribute:ftq.role == worker"
  }
  # Even spread: with 2 per host this is just "fill every host".
  ordered_placement_strategy {
    type  = "spread"
    field = "instanceId"
  }
  depends_on = [aws_ecs_service.redis]
}

# One-off tasks (`make aws-smoke`, Phase 8's loadgen). The command is supplied at run-task
# time; `timeout` is the hard wall-clock guard, so a forgotten run stops itself.
resource "aws_ecs_task_definition" "loadgen" {
  family                   = "${local.name}-loadgen"
  network_mode             = "host"
  requires_compatibilities = ["EC2"]
  execution_role_arn       = aws_iam_role.execution.arn
  container_definitions = jsonencode([{
    name              = "loadgen"
    image             = local.image
    essential         = true
    memoryReservation = 1024
    ulimits           = local.ulimits
    workingDirectory  = "/opt/ftq" # bench/ lives here (docker/Dockerfile)
    entryPoint        = ["timeout", "-k", "30", tostring(var.task_max_seconds)]
    command           = ["ftq", "stats"]
    environment       = local.ftq_env
    logConfiguration  = local.log_config["loadgen"]
  }])
}

# ---------------------------------------------------------------- outputs

output "cluster" {
  value = aws_ecs_cluster.ftq.name
}

output "loadgen_placement" {
  description = "Constraint for run-task: where one-off tasks go."
  value       = "attribute:ftq.role == ${local.loadgen_role}"
}

output "loadgen_task_definition" {
  value = aws_ecs_task_definition.loadgen.arn
}

output "fleet" {
  value = {
    vcpus        = local.total_vcpus
    quota        = data.aws_servicequotas_service_quota.vcpus.value
    worker_hosts = var.worker_hosts
    workers      = var.workers
    loadgen_on   = local.loadgen_role
  }
}
