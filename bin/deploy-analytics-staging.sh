#!/usr/bin/env bash

echo "Beginning deployment..."

sh bin/devops-setup.sh

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

GIT_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)

cd devops/ansible

ansible-playbook -i inventories/analytics/staging playbooks/deploy.yml --extra-vars "app_branch_name=$GIT_BRANCH_NAME" -vvv

cd ../..

echo "Deployment Complete"
