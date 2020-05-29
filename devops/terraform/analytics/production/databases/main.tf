terraform {
  backend "s3" {
    bucket = "terraform.datascience.infrastructure"
    key    = "production/analytics/rds/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region  = "us-east-1"
  version = "~> 1.9"
}

provider "random" {
  version = "~> 1.3"
}

resource "random_string" "db_password" {
  length  = 32
  special = false
}

data "aws_route53_zone" "private_zone" {
  name         = "${ var.route53_internal_zone_name }"
  private_zone = true
}

module "db" {

  rds_engine                    = "postgres"
  rds_engine_version            = "11.7"
  rds_parameter_group_name      = "default.postgres11"
  rds_port                      = "5432"

  rds_database_name             = "${ var.app_name }"
  rds_password                  = "${ random_string.db_password.result }"
  rds_vpc_id                    = "${ var.vpc_id }"
  rds_env                       = "${ var.env }"
  rds_identifier                = "${ var.app_name }"
  rds_username                  = "${ var.rds_username }"
  rds_route53_zone_id           = "${ data.aws_route53_zone.private_zone.id }"
  rds_instance_class            = "${ var.rds_instance_type }"
  rds_subnet_group_name         = "${ var.rds_subnet_group_name }"
  rds_route53_cname             = "db"
  rds_application_name          = "${ var.app_name }"
  rds_skip_final_snapshot       = false
  rds_final_snapshot_identifier = "${ var.env }-${ var.app_name }-final-snapshot"
  rds_multi_az                  = "${ var.rds_multi_az }"
  rds_storage_type              = "${ var.rds_storage_type }"
  rds_allocated_storage         = "${ var.rds_allocated_storage }"
  rds_create_storage_alarm      = true


  source = "git::git@github.com:mobials/devops.git//terraform/modules/database/mysql/module"
}
