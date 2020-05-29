env               = "production"
app_name          = "analytics"
region            = "us-east-1"
vpc_id            = "vpc-f3d28696"
private_subnet_id = "subnet-65098f3c"
public_subnet_id  = "subnet-64098f3d"

route53_internal_zone_name = "analytics.internal"

rds_username          = "analytics"
rds_db_name           = "analytics"
rds_instance_type     = "db.t3.small"
rds_subnet_group_name = "production_rds_subnet_group_private"
rds_multi_az          = "false"
rds_storage_type      = "gp2"
rds_allocated_storage = 300
