import boto3
from botocore.client import ClientError
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import Group, User

from databricks import sql
from databricks.sql.exc import ServerOperationError, DatabaseError
import os
import time
import pprint
pp = pprint.PrettyPrinter(indent=2,width=160)

import logging
logger = logging.getLogger()


def list_or_single(a):
  if not a:
    yield []
  elif isinstance(a, str):
    yield a
  else:
    for _ in a:
      yield _
  
def actions(statement):
  action = statement['Action']
  return list_or_single(action)

def resources(statement):
  resource = statement.get('Resource',None)
  return list_or_single(resource)

def walk_instanceProfile(host:str, instance_profile:str, counter:int):
  logger.setLevel(logging.DEBUG)
  iam = boto3.Session().client('iam')
  t = []

  arn = instance_profile.instance_profile_arn
  t.append((f'dbx:workspace/{host}', 'dbx:instanceProfile', arn))
  instance_profile_name = arn.split('/')[-1:][0]
  logger.info(f'Processing instance profile {instance_profile_name}')

  try:
    ip = iam.get_instance_profile(InstanceProfileName=instance_profile_name)
    logger.info(f'Processing instance profile {instance_profile_name}:{arn}')

    for r in ip['InstanceProfile']['Roles']:
      role_name = r['RoleName']

      for p in iam.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']:
        policy_arn = p['PolicyArn']
        policy = iam.get_policy(PolicyArn = policy_arn)
        policy_version = iam.get_policy_version(
            PolicyArn = policy_arn,
            VersionId = policy['Policy']['DefaultVersionId']
        )
        policy_document = policy_version['PolicyVersion']['Document']
        for statement in policy_document['Statement']:
          effect = statement['Effect'].lower()
          for action in actions(statement):
            for resource in resources(statement):
                acl_id = f'aws:acl-{counter}'
                t.append((arn, 'rdf:has', acl_id))
                t.append((acl_id, f'iam:{effect}', action))
                t.append((acl_id, f'iam:resource', resource))
                counter += 1             

  except ClientError as e:
    logger.warning(f'Error processing instance profile {instance_profile_name}: {e}')

  return t, counter


def walk_databricks_instance_profiles(host: str, token:str):
  
  triples=[]
  w = WorkspaceClient(host=host, token=token)
  counter = 0
  
  for i,instance_profile in enumerate(w.instance_profiles.list()):
    t, counter = walk_instanceProfile(host, instance_profile, counter)
    logger.debug("Instance Profiler Walker ", len(t),counter)
    if len(t) > 0:
      triples.append(t)
      
  return triples