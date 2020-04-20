These packer scripts help build the base image used for our consumers. To run them, cd into the parent directory and run:

For staging

`packer build -var-file=packer/staging-vars.json packer/golden.json`

for production

`packer build -var-file=packer/production-vars.json packer/golden.json`

You should only need to run these if you are changing what applications / software
are installed on the base images.
