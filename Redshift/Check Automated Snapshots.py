import boto3, datetime

def getClusters(region):
    """
    :return:dictionary of clusters in this account
    """

    awsClient = boto3.client('redshift',region_name=region)
    clusters = awsClient.describe_clusters()
    clustersIdentifire={}
    for clusterIndex in range(len(clusters["Clusters"])):
        if clusters["Clusters"][clusterIndex]["AutomatedSnapshotRetentionPeriod"] > 0 and clusters["Clusters"][clusterIndex]['ClusterStatus'] == 'available':
            clustersIdentifire[clusters["Clusters"][clusterIndex]['ClusterIdentifier']]={"Backup Enabled":1}
        else:
            clustersIdentifire[clusters["Clusters"][clusterIndex]['ClusterIdentifier']] = {"Backup Enabled": 0}

    return clustersIdentifire

def getClusterThatHaveAutomatedSnapshots(region):
    '''
    :return: a list of all clusters that have recent Automated Snapshots
    '''
    diff = datetime.datetime.now() - datetime.timedelta(days=1)
    awsClient = boto3.client('redshift',region_name=region)
    clusters=[]
    snapshots = awsClient.describe_cluster_snapshots(StartTime=diff.strftime("%Y-%m-%dT%H:%M:%SZ"),SnapshotType="automated")#,ClusterIdentifier=clusterName,SnapshotType="automated")
    for i in range(len(snapshots["Snapshots"])):
        clusters.append(snapshots["Snapshots"][i]['ClusterIdentifier'])
    clusters.sort()
    return clusters

def getMostRecentSnapshotForCluster(clusterIdentifier,region):
    '''
    :param clusterIdentifier:string
    :return: Most Recent Snapshot name, and time
    '''
    awsClient = boto3.client('redshift',region_name=region)
    data={}
    snapshots = awsClient.describe_cluster_snapshots(ClusterIdentifier=clusterIdentifier)
    for response in snapshots['Snapshots']:
        data[response['SnapshotCreateTime']] = {'SnapshotIdentifier':response['SnapshotIdentifier'], 'SnapshotType':response['SnapshotType'], 'SnapshotCreateTime':response['SnapshotCreateTime']}

    # print(max(data.keys()))
    return data[max(data.keys())]
    # print(len(data))
    # print(data)




def log(clusterName, now, mostRecentSnapshotCreationName,mostRecentSnapshotCreationTime,mostRecentSnapshotCreationType,logLevel="NOTE",stringToAdd='',regionName=''):
    '''
    This function will add new record to the log file
    :param clusterName:cluster name
    :param logLevel: Note, Error, Warning,
    :param now: DateTime Now
    :param lastSnapshotCreationTime: Date of the last created snapshot
    :return: 0 if the script wrote the log successfully, 1 if the log had an error.
    '''
    # print(now.strftime("%m-%d-%y %H:%M:%S"))
    logDate = now.strftime("%m-%d-%Y %H:%M:%S")
    mostRecentSnapshotCreationTime = mostRecentSnapshotCreationTime.strftime("%m-%d-%Y %H:%M:%S")
    line = '' + logDate + ' [' + logLevel + '] cluster "' + clusterName + '" in region "'+regionName+'" didn`t create an Automated snapshot for more than 24 hours.' + '\n'
    print(line)

    # try:
    #     log = open("Redshift snapshot monitoring.log",'a')
    #     log.writelines(line)
    #     log.close()
    # except FileNotFoundError:
    #     log = open("Redshift snapshot monitoring.log", 'x')
    #     log.writelines(line)
    #     log.close()


def singleRegionExecution(region_name):
    allClusters=getClusters(region_name)
    clustersThatHaveRecentAutomatedSnapshots = getClusterThatHaveAutomatedSnapshots(region_name)
    # allClusters.keys().sort()
    # clustersThatHaveRecentAutomatedSnapshots.sort()

    # print allClusters.keys()
    # print clustersThatHaveRecentAutomatedSnapshots
    for cluster in sorted(allClusters.keys()):
        if cluster not in clustersThatHaveRecentAutomatedSnapshots:
            recentSnapshotDetails = getMostRecentSnapshotForCluster(cluster,region_name)
            log(clusterName=cluster,now=datetime.datetime.now(),mostRecentSnapshotCreationName=recentSnapshotDetails['SnapshotIdentifier'],mostRecentSnapshotCreationTime=recentSnapshotDetails['SnapshotCreateTime'],logLevel='WARNING',mostRecentSnapshotCreationType=recentSnapshotDetails["SnapshotType"],regionName=region_name)


def executeGlobally():
    client = boto3.client('ec2',region_name='us-east-1')
    regions = [region['RegionName'] for region in client.describe_regions()['Regions']]
    for region in regions:
        print("Checking region " + region)
        singleRegionExecution(region)


# main(region_name='us-east-1')\
executeGlobally()
# getMostRecentSnapshotForCluster('redshift2')