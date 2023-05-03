#!/bin/bash

set -eu

# use YOUR OWN file name
# use YOUR OWN name of the stack
# use YOUR OWN profile name

# aws cloudformation deploy --template-file marks-sample-stack.yml \
# 		--stack-name marks-bucket-stack --profile generation-de \
#          --region eu-west-1

# aws cloudformation deploy --template-file cool-beans-aws-lambda-template.yml \
# 		--stack-name cool-beans-final-project-stack --profile lili_me --region eu-west-1

aws cloudformation create-stack --stack-name cool-beans-final-project \
	--template-body file://cool-beans-aws-lambda-template.yml --region eu-west-1 \
	--capabilities CAPABILITY_IAM --profile lili_me