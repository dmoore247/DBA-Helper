# Databricks notebook source
# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %pip install --quiet databricks-sql-connector databricks-sdk boto3 botocore

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

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

# COMMAND ----------

# from permissions graph
from roles import *

# COMMAND ----------



triples = []
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = "e2-demo-field-eng.cloud.databricks.com"
triples = walk_databricks_instance_profiles(host, token)
pp.pprint(triples)


# COMMAND ----------


