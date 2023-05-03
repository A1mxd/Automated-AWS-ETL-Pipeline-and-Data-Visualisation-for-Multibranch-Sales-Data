#!/bin/bash

set -eu

profile_name=$1

aws cloudformation deploy --stack-name cool-beans-final-project \
	--template-file cool-beans-aws-lambda-template.yml --region eu-west-1 \
	--capabilities CAPABILITY_IAM --profile ${profile_name}