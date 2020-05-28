env               = "staging"
app_name          = "analytics"
region            = "us-east-1"
vpc_id            = "vpc-0c176c69"
private_subnet_id = "subnet-9b8439c2"
public_subnet_id  = "subnet-9a8439c3"

route53_internal_zone_name = "analytics-staging.internal"

rds_username          = "analytics"
rds_db_name           = "analytics"
rds_instance_type     = "db.t3.small"
rds_subnet_group_name = "staging_rds_subnet_group_private"
rds_multi_az          = "false"
rds_storage_type      = "gp2"
rds_allocated_storage = 100
