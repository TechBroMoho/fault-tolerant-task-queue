# Sizing knobs. The defaults are the small footprint that fits the Free plan's default
# 5-vCPU quota (Phase 7: 1 Redis host that also runs the loadgen + 1 worker host = 4
# vCPUs). Phase 8 raises them, e.g. -var worker_hosts=6 -var workers=12 -var loadgen_hosts=1.

variable "az" {
  description = "The one AZ everything runs in: cross-AZ traffic is billed (SPEC §7)."
  type        = string
  default     = "us-west-2a"
}

variable "image_tag" {
  description = "Tag of the ftq image in the ECR repo (make aws-image pushes the git short SHA)."
  type        = string
}

variable "redis_instance_type" {
  description = "Redis host: 8 GiB for maxmemory plus AOF/fork headroom (SPEC §7)."
  type        = string
  default     = "m7i-flex.large"
}

variable "host_instance_type" {
  description = "Worker and loadgen hosts."
  type        = string
  default     = "c7i-flex.large"
}

variable "worker_hosts" {
  description = "Instances in the worker ASG."
  type        = number
  default     = 1
}

variable "workers" {
  description = "Worker tasks (the ECS service's desired count). Each reserves 1 vCPU, so at most 2 per host."
  type        = number
  default     = 2
  validation {
    condition     = var.workers >= 0
    error_message = "workers must be >= 0."
  }
}

variable "loadgen_hosts" {
  description = "Dedicated loadgen instances. 0 = the loadgen runs on the Redis host (small footprint only)."
  type        = number
  default     = 0
}

variable "worker_concurrency" {
  description = "FTQ_CONCURRENCY for the workers (ADR-042: 50 for benchmarks)."
  type        = number
  default     = 50
}

variable "redis_maxmemory" {
  description = "Redis maxmemory; the policy is always noeviction (eviction would break idempotency)."
  type        = string
  default     = "5gb"
}

variable "redis_io_threads" {
  description = "Redis io-threads (ADR-042: Phase 8 repeats the 1 vs 4 comparison)."
  type        = number
  default     = 1
}

variable "log_level" {
  description = "Worker log level. WARNING while benchmarking: per-job logs at 10K/s would cost real money in CloudWatch."
  type        = string
  default     = "WARNING"
}

variable "task_max_seconds" {
  description = "Hard wall-clock limit of a one-off (loadgen/bench) task: it stops itself (SPEC §7 Phase 8)."
  type        = number
  default     = 1800
}

variable "max_vcpus" {
  description = "Our own cap on the fleet, below the account quota (which AWS set to 64). 16 = what Mohammed approved for Phase 8. Raise only with his yes."
  type        = number
  default     = 16
}
