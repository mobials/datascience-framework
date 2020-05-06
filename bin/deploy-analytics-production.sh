#!/usr/bin/env bash

echo "Beginning deployment..."

sh bin/devops-setup.sh

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

cd devops/ansible

ansible-playbook -i inventories/analytics/production playbooks/deploy.yml

cd ../..

echo "Deployment Complete"
