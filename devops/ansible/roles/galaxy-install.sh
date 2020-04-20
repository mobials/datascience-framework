#!/usr/bin/env bash

# this script is a simple helper to install vendor Ansible Galaxy roles

ansible-galaxy install -f -v -p vendor -r requirements.yml
