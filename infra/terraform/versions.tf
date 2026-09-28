terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # Kept on 5.x to match the LocalStack 4.6 image that CI applies this configuration to.
      version = "~> 5.100"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "video-processing-pipeline"
      ManagedBy = "terraform"
    }
  }
}
