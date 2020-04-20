### Configuration

In this folder you will find the (mostly) Ansible scripts required to configure 
(provision) our instances as well as some additional helpers to configure 
RabbitMQ and such.


## Getting Started

1. Install Ansible inside your Vagrant box. There's a helper script to do this for you in the Vagrant repo called start.sh. 
2. Setup your AWS credentials in the box (start.sh script explains how)
3. Start the ssh agent via `eval "$(ssh-agent -s)"`
4. Copy your reviewsii-dev.pem file from host to box's ~/.ssh folder
5. Add the pem file to your ssh agent via `ssh-add ~/.ssh/reviewsii-dev.pem`
6. Install the Elastic Beanstalk CLI tools https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install.html

Note: it's helpful to put the ssh-agent parts above into your bash profile script, so that this is done for you upon
log in / reboot. E.g., in your `~/.bashrc` file add these lines to the bottom:

````$bash
# start ssh agent
eval "$(ssh-agent -s)"

# add our key to agent
ssh-add ~/.ssh/reviewsii-dev.pem

````



## The Playbooks

There are three playbooks, only one of which you'll generally need to worry about. 

`bastion.yml` 

This playbook is for provisioning a new bastion server. However, this has already been done for you so you shouldn't 
need to re-run this unless you're adding additional IPs to the white list. 

`provision.yml`

This playbook provisions a new base AMI for the credsii consumers. Essentially, it installs all of the standard software
required to run the app (e.g., Supervisor, git, etc.), creates an image of it, and then shuts down the server. The deploy 
playbook will always use the most recent AMI as its base. You should only need to run this playbook if you've change some
of the software in the server. 

`deploy.yml`

This playbook deploys the consumers & Elastic Beanstalk application servers. Note that when deploying the consumers, the existing consumers (if any) are stopped
briefly while git is pulling in the latest code and supervisor is restarting. Generally this shouldn't be a problem, but
it's something that should be kept in mind. 


## Running a Playbook

To run a playbook, cd to the Credsii root directory in your box and enter:

`ansible-playbook -i inventories/{environment name} {playbook name}`

So if we wanted to deploy the app to staging, we would use:

`ansible-playbook -i inventories/staging deploy.yml`

If we wanted to deploy to production, it would be:

`ansible-playbook -i inventories/production deploy.yml`

## Troubleshooting

Generally, Ansible playbooks are considered idempotent. If you run into an error, you can usually run the script again 
without worrying about something breaking. 

###### Ansible can't connect to the remote machines. 

Make sure your ssh agent is running and you added your pem file. Also check that the IP you're on is included in the 
white listed IPs.

Sometimes when an instance boots on AWS, it takes longer than normal to reach the 'ready' state, which causes Ansible to 
bug out. If this happens, you can safely re-run the same playbook and it should work as normal. 

###### How do I deploy a branch other than master?

To deploy a branch other than master, modify the `app_branch_name` variable in the appropriate inventory file. E.g., 
to change the staging branch from `master` to `patrick-edited-vendor-dir`, we would replace this line:

`app_branch_name: master`

with:

`app_branch_name: patrick-edited-vendor-dir`

You don't actually need the `app_branch_name` value to be set. The deploy script defaults to master if it isn't set. 

NOTE: for the deploy script to work, you must have your current branch pushed to origin!
