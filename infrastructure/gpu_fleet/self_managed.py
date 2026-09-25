# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""The per-region side of the raw-EC2 path: a log group and an age terminator.

Batch cannot race regions: a job goes to one queue in one region. Spot capacity for 8-GPU
nodes is scarce and moves between regions by the hour, so the fastest way to a node is to
ask every region at once and cancel the losers. That path launches raw EC2 spot nodes from
the SAME launch templates the compute environments use and drives docker over SSM.

A raw node has no Batch timeout, so the terminator supplies one: every 15 minutes it
terminates instances tagged `rt-gpu:managed-by=hunt` older than their `rt-gpu:ttl-hours`
tag (default max_node_hours). The operator policy lives in the global stack.
"""
from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack, aws_events as events, \
    aws_events_targets as targets, aws_iam as iam, aws_lambda as lam, aws_logs as logs
from constructs import Construct

from . import catalogue as cat

TERMINATOR_CODE = r'''
import os, datetime, boto3
TAG_KEY, TAG_VALUE = os.environ["TAG_KEY"], os.environ["TAG_VALUE"]
TTL_KEY = os.environ["TTL_TAG_KEY"]
MAX_H = float(os.environ["MAX_NODE_HOURS"])

def handler(event, ctx):
    ec2 = boto3.client("ec2")
    now = datetime.datetime.now(datetime.timezone.utc)
    kill, seen = [], 0
    pager = ec2.get_paginator("describe_instances")
    for page in pager.paginate(Filters=[{"Name": f"tag:{TAG_KEY}", "Values": [TAG_VALUE]},
                                        {"Name": "instance-state-name", "Values": ["pending", "running"]}]):
        for r in page["Reservations"]:
            for i in r["Instances"]:
                seen += 1
                tags = {t["Key"]: t["Value"] for t in i.get("Tags", [])}
                ttl = float(tags.get(TTL_KEY, MAX_H))
                age_h = (now - i["LaunchTime"]).total_seconds() / 3600
                if age_h > ttl:
                    kill.append((i["InstanceId"], round(age_h, 1), ttl))
    if kill:
        ec2.terminate_instances(InstanceIds=[k[0] for k in kill])
    print({"seen": seen, "terminated": kill})
    return {"seen": seen, "terminated": kill}
'''


class SelfManagedNodes(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, log_group_name: str,
                 max_node_hours: int) -> None:
        super().__init__(scope, construct_id)
        stack = Stack.of(self)
        self.log_group = logs.LogGroup(self, "NodeLogs", log_group_name=log_group_name,
                                       retention=logs.RetentionDays.ONE_MONTH,
                                       removal_policy=RemovalPolicy.DESTROY)
        fn = lam.Function(
            self, "AgeTerminator", runtime=lam.Runtime.PYTHON_3_12, handler="index.handler",
            code=lam.Code.from_inline(TERMINATOR_CODE), timeout=Duration.seconds(60),
            description=f"terminates {cat.HUNT_TAG_KEY}={cat.HUNT_TAG_VALUE} nodes older than "
                        f"their ttl (default {max_node_hours} h)",
            environment={"TAG_KEY": cat.HUNT_TAG_KEY, "TAG_VALUE": cat.HUNT_TAG_VALUE,
                         "TTL_TAG_KEY": cat.TTL_TAG_KEY, "MAX_NODE_HOURS": str(max_node_hours)})
        fn.add_to_role_policy(iam.PolicyStatement(actions=["ec2:DescribeInstances"], resources=["*"]))
        fn.add_to_role_policy(iam.PolicyStatement(
            actions=["ec2:TerminateInstances"],
            resources=[f"arn:{stack.partition}:ec2:{stack.region}:{stack.account}:instance/*"],
            conditions={"StringEquals": {f"ec2:ResourceTag/{cat.HUNT_TAG_KEY}": cat.HUNT_TAG_VALUE}}))
        events.Rule(self, "Every15Min", schedule=events.Schedule.rate(Duration.minutes(15)),
                    targets=[targets.LambdaFunction(fn)])
        CfnOutput(self, "NodeLogGroup", value=self.log_group.log_group_name)
        CfnOutput(self, "TerminatorFunction", value=fn.function_name)
