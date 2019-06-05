"""
Utils to create lambda functions.
"""
import uuid

import boto3

from khan.aws.config import AWS_REGION, AWS_ROLE_ARN, LAMBDA_UPDATE, VPC_CONFIG
from khan.aws.packaging import package_with_dependencies

lambdacli = boto3.client('lambda', region_name=AWS_REGION)
show_responses = False


def uuid_str():
    """ Generate an unique id."""
    return str(uuid.uuid4())


def new_lambda(name, handler, extra_modules=None):
    """
    Packages all files that the function *handler* depends on into a zip.
    Creates a new lambda with name *name* on AWS using that zip.
    This one does not have VPC config. So it has access to external services
    such as SNS and SQS. (But can't connect directly to Redis)
    """
    if extra_modules is None:
        extra_modules = []
    # zip function-module and dependencies
    zipfile, lamhand = package_with_dependencies(handler,
                                                 extra_modules=extra_modules)

    # create the new lambda by uploading the zip.
    try:
        response = lambdacli.create_function(
            FunctionName=name,
            Runtime='python3.7',
            Role=AWS_ROLE_ARN,
            Handler=lamhand,
            Code={'ZipFile': zipfile.getvalue()},
            Publish=True,
            Description='Lambda with cloud object.',
            Timeout=300,
            MemorySize=3008,
            VpcConfig=VPC_CONFIG,
            Layers=[
                "arn:aws:lambda:us-east-1:668099181075:layer:AWSLambda-Python37-SciPy1x:2"
            ]
            # DeadLetterConfig={
            #     'TargetArn': 'string'
            # },
            # KMSKeyArn='string',
            # TracingConfig={
            #     'Mode': 'Active'|'PassThrough'
            # },
            # Tags={
            #     'string': 'string'
            # }
        )
        if show_responses:
            print(response)
        print(f"New lambda {name} created successfully.")
    except lambdacli.exceptions.ResourceConflictException:
        print("Lambda already exists...")
        if LAMBDA_UPDATE:
            print("Updating Lambda...")
            delete_lambda(name)
            new_lambda(name, handler, extra_modules)
        else:
            print("*NO* Lambda update. Proceeding...")


def delete_lambda(name):
    """ Deletes a lambda function from AWS with name *name*."""
    response = lambdacli.delete_function(FunctionName=name)
    if show_responses:
        print(response)
    print(f"Lambda {name} deleted successfully.")


def set_lambda_concurrency(name, concurrency_value):
    """ Sets the maximum amount of concurrent executions to a lambda
     function with name *name*."""
    response = lambdacli.put_function_concurrency(
        FunctionName=name,
        ReservedConcurrentExecutions=concurrency_value
    )
    if show_responses:
        print(response)
    print(f"Lambda {name} concurrency limit set"
          f" to {concurrency_value} successfully.")


def map_lambda_to_queue(lambda_name, queue_arn):
    try:
        lambdacli.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=lambda_name,
        )
    except lambdacli.exceptions.ResourceConflictException:
        print("Trigger mapping already exists. Skipping...")
