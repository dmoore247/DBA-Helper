import logging
import requests
import time
from utils import coalesce
import pprint
pp = pprint.PrettyPrinter(indent=2,width=150)
from databricks.sdk import WorkspaceClient

def extract_clusters(host, token):

    triples = []
    w = WorkspaceClient(host=host, token=token)
    print(f'Clusters#: {len(w.clusters.list())}')
    
    for counter, c in enumerate(w.clusters.list()):
        clusterid = f'dbx:/clusters/{c.cluster_id}'
        triples.append((clusterid,'rdf:label',c.cluster_name))
        if c.aws_attributes and c.aws_attributes.instance_profile_arn:
            triples.append((clusterid,'dbx:role',c.aws_attributes.instance_profile_arn))
        p = w.permissions.get('clusters',c.cluster_id)
        for i,acl in enumerate(p.access_control_list):
            principal = coalesce(acl.group_name, acl.service_principal_name, acl.user_name)
            
            clusteracl = f'dbx:clusterACL/{c.cluster_id}/{i}'
            triples.append((clusterid,'dbx:acl',clusteracl))
            triples.append((clusteracl, 'dbx:principal', principal))
            for j,perms in enumerate(acl.all_permissions):
                perm = f'dbx:permission/{c.cluster_id}/acl:{i}/perm:{j}'
                triples.append((principal,'dbx:permission', perm))
                triples.append((perm,'dbx:permission/level',str(perms.permission_level.value)))
                triples.append((perm,'dbx:permission/inherited',str(perms.inherited)))
                if perms.inherited and perms.inherited_from_object:
                    triples.append((perm,'dbx:permission/inheritedFrom', f'dbx:{perms.inherited_from_object[0][1:]}'))
        if counter > 100: break
    return triples