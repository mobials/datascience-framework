terraform {
  backend "s3" {
    bucket = "terraform.datascience.infrastructure"
    key    = "production/analytics/route53/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region  = "${ var.region }"
  version = "~> 1.9"
}

resource "aws_route53_zone" "internal-zone" {
  name   = "${ var.route53_internal_zone_name }"
  vpc_id = "${ var.vpc_id }"

  tags   = {
    terraform = true
    env       = "${ var.env }"
    app       = "${ var.app_name }"
  }
}
