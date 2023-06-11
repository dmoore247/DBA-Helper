from databricks.sdk.service.iam import Group, User
import os
import time
import pprint
pp = pprint.PrettyPrinter(indent=2,width=160)

"""
  Walk workspace groups
"""
def get_group_triples(group:Group):
  t = []
  group_uri = f'dbx:group/{group.id}'
  t.append((group_uri, "rdf:label", group.display_name))
  if group.roles and len(group.roles) > 0:
    for role in group.roles:
      t.append((group_uri, 'dbx:iam', role.value))
  return t

def get_group_member_triples(group:Group):
  t = []
  group_uri = f'dbx:group/{group.id}'
  if group.members and len(group.members) > 0:
    for member in group.members:
      member_uri = f'dbx:user/{member.value}'
      t.append((member_uri, 'dbx:memberOf', group_uri))
  return t

def get_user_triples(user:User):
  t = []
  user_uri = f'dbx:user/{user.id}'
  t.append((user_uri, "dcterms:valid", str(user.active).lower()))
  t.append((user_uri, "rdf:label", user.display_name))
  t.append((user_uri,"dbx:email", user.emails[0].value))
  if user.groups and len(user.groups) > 0:
    for group in user.groups:
      t.append((user_uri, "dbx:memberOf", f'dbx:group/{group.value}'))
  if user.roles and len(user.roles) > 0:
    for role in user.roles:
      t.append((user_uri, "dbx:instanceProfile", f'{role.value}'))
  return t

def walk_groups(client):
  triples=[]
  group_list = client.groups.list()
  for group in group_list:
    triples = triples + get_group_triples(group)
    triples = triples + get_group_member_triples(group)
  return triples

def walk_users(client):
  triples=[]
  index = 0
  while True:
    user_list = client.users.list(count=count,start_index=index)
    if 0 == len(user_list):
      break
    for user in user_list:
      triples = triples + get_user_triples(user)
    index = index + len(user_list)
  return triples, index