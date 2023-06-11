from utils import coalesce

def getMetastoreURI(spark, namespace:str, host:str) -> str:
    """The Hive metatstore is either:
      1. The Default Databricks metastore use the orgid (workspaceid) iff the connectionURL and has_glue_catalog are None
      2. External HMS if connectionURL is not None
      3. Glue Catalog if glue_catalog is enabled
    """
    orgid = spark.conf.get('spark.databricks.clusterUsageTags.orgId')
    region = spark.conf.get('spark.databricks.clusterUsageTags.region')
    connectionURL = spark.conf.get('javax.jdo.option.ConnectionURL', None)
    remoteURL = spark.conf.get('spark.hadoop.hive.metastore.uris', None)
    glue_catalog = spark.conf.get('spark.databricks.hive.metastore.glueCatalog.enabled', None)
    glue_catalog_id = spark.conf.get('spark.hadoop.hive.metastore.glue.catalogid', None)
    try:
        aws_account_id = boto3.client('sts').get_caller_identity().get('Account')
    except:
        aws_account_id = None
        pass

    my_hms = None
    
    if glue_catalog:
        my_hms = f"{namespace}:hms/glue-{coalesce(glue_catalog_id,aws_account_id)}/{region}"
    
    if connectionURL:
        my_hms = f"{namespace}:hms/external-{connectionURL}"

    if remoteURL:
        my_hms = f"{namespace}:hms/remote-{remoteURL}"
  
    if not glue_catalog and not connectionURL and not remoteURL:
        my_hms = f"{namespace}:hms/{host}"
    
    return my_hms

def getHMSTriples(spark, namespace:str, host:str) -> list[tuple[str,str,str]]:
    my_hms = getMetastoreURI(spark,namespace,host)
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
    orgid = spark.conf.get('spark.databricks.clusterUsageTags.orgId')
    
    my_cluster = f"{namespace}:clusters/{cluster_id}"
    my_workspace = f"{namespace}:workspaces/{orgid}"
    
    triples = []
    
    triples.append((my_cluster, "rdf:type", f"{namespace}:types/cluster"))
    triples.append((my_cluster,f"rdfs:label",f"\"{cluster_name}\""))

    triples.append((my_workspace,f"rdf:has",my_cluster))
    triples.append((my_workspace, "rdf:type", f"{namespace}:types/workspace"))
   
    triples.append((my_hms, "rdf:type", f"{namespace}:types/hms"))
    triples.append((my_cluster,f"prov:used",my_hms))

    return triples