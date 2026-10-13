terraform {
  required_version = ">= 1.6.0"
}

variable "environment" {
  type        = string
  description = "Environment name used in topic configuration."
  default     = "dev"
}

variable "orders_partitions" {
  type        = number
  description = "Partition count for the order stream."
  default     = 6

  validation {
    condition     = var.orders_partitions >= 3
    error_message = "Production-like streams should use at least three partitions."
  }
}

locals {
  topics = {
    "orders.v1" = {
      partitions       = var.orders_partitions
      retention_ms     = 604800000
      cleanup_policy   = "delete"
      min_insync_replicas = 2
    }
    "orders.dlq.v1" = {
      partitions       = 3
      retention_ms     = 2592000000
      cleanup_policy   = "delete"
      min_insync_replicas = 2
    }
  }
}

# Provider-specific Kafka resources are intentionally omitted from the public
# case so the same topic contract can map to Confluent Cloud, AWS MSK or another
# managed Kafka service. These outputs are the IaC contract consumed by an
# environment-specific module.
output "topic_contract" {
  value = local.topics
}
