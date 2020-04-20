#!/usr/bin/env bash

echo "Beginning deployment..."

sh bin/devops-setup.sh

GIT_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)

cd devops/ansible

ansible-playbook -i inventories/staging playbooks/deploy.yml --extra-vars "app_branch_name=$GIT_BRANCH_NAME" -vvv

cd ../..

echo "Deployment Complete"
