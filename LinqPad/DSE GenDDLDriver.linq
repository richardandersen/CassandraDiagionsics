<Query Kind="Program" />

void Main()
{
	
}

void GetNodeInfoFromDSE(string dseConnectionString,
						string dseUserName,
						string dseUserPassword,
						string[] excludeCQLDDLKeySpaces,
						bool getDDL,
						string cqlDDLLocalFilePath,
						List<NodeInfo> nodeInfoList)
{
	using (var dseCluster = AddCredentials(Cluster
											.Builder()
											//.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
											.WithConnectionString(dseConnectionString)
											.WithoutRowSetBuffering()
											.WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One))
											.WithCompression(CompressionType.LZ4),
											dseUserName,
											dseUserPassword)
											.Build())
	{
		CurrentDSEClusterName = dseCluster.Metadata.ClusterName;
		foreach (var dseNode in dseCluster.Metadata.AllHosts())
		{
			nodeInfoList.Add(new NodeInfo() { IPAddress = dseNode.Address.Address.ToString(), DCName = dseNode.Datacenter, CassandraVersion = dseNode.CassandraVersion.ToString() });
		}

		if (getDDL && !string.IsNullOrEmpty(cqlDDLLocalFilePath))
		{
			var ifilepathCQLDDL = Common.File.BaseFile.Make(string.Format(cqlDDLLocalFilePath, CurrentDSEClusterName));

			if (ifilepathCQLDDL.Exist())
			{
				ifilepathCQLDDL.Truncate();
			}
			else
			{
				ifilepathCQLDDL.Create();
			}

			using (var ddlWriteToFile = ifilepathCQLDDL.OpenTextWriter())
			{
				ddlWriteToFile.WriteLine("//ClusterName: '{0}' DataCenters: {1}",
											CurrentDSEClusterName,
											string.Join(", ", nodeInfoList.Select(item => item.DCName).DuplicatesRemoved(item => item)));
				ddlWriteToFile.WriteLine();

				foreach (var keySpace in dseCluster.Metadata.GetKeyspaces())
				{
					if (!excludeCQLDDLKeySpaces.Contains(keySpace))
					{
						DescribeKeySpaces(dseCluster, keySpace, ddlWriteToFile);
					}
				}
			}
		}
	}
}


//CREATE KEYSPACE ksName WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 3, 'DC2': 3}  AND durable_writes = true;
//CREATE KEYSPACE ksName WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}  AND durable_writes = true;

public string MakeKeySpaceReplication(KeyspaceMetadata ksInfo)
{
	var stReplication = new StringBuilder();

	stReplication.AppendFormat("{{'class': '{0}'", ksInfo.StrategyClass);

	foreach (var element in ksInfo.Replication)
	{
		stReplication.AppendFormat(", '{0}': {1}", element.Key, element.Value);
	}

	stReplication.Append("}");

	return stReplication.ToString();
}

public void DescribeKeySpaces(Cluster dseCluster, string keySpace, StreamWriter ddlWriteToFile)
{
	if (!string.IsNullOrEmpty(keySpace))
	{
		var ksInfo = dseCluster.Metadata.GetKeyspace(keySpace);

		ddlWriteToFile.WriteLine("//Keyspace: '{0}'", keySpace);
		ddlWriteToFile.WriteLine();

		ddlWriteToFile.WriteLine("CREATE KEYSPACE IF NOT EXISTS {0} WITH replication = {1} AND durable_writes = {2};",
									keySpace,
									ConvertAllKeyspacesToSimpleStrategy
										? string.Format("{{'class': 'SimpleStrategy', 'replication_factor': {0} }}", KeyspaceDefaultReplicationFator)
										: MakeKeySpaceReplication(ksInfo),
									ksInfo.DurableWrites);

		if (ConvertAllKeyspacesToSimpleStrategy)
		{
			ddlWriteToFile.WriteLine("//CREATE KEYSPACE IF NOT EXISTS {0} WITH replication = {1} AND durable_writes = {2};",
										keySpace,
										MakeKeySpaceReplication(ksInfo),
										ksInfo.DurableWrites);
		}

		ddlWriteToFile.WriteLine();

		foreach (var tblName in dseCluster.Metadata.GetTables(keySpace).Dump())
		{
			//dseCluster.Metadata.GetTable(keySpace, tblName).Dump();
			//cluster.Metadata.GetTable(keySpace, tblName).Options.Dump();
		}
	}
}

//CREATE TABLE ks1.user_institutions(
//	guid text,
//	source text,
//	entitynumber text,
//	PRIMARY KEY((guid, source), entitynumber)
//) WITH CLUSTERING ORDER BY(entitynumber ASC)
//    AND bloom_filter_fp_chance = 0.01
//    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
//    AND comment = ''
//    AND compaction = { 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' }
//    AND compression = { 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor' }
//    AND dclocal_read_repair_chance = 0.1
//    AND default_time_to_live = 0
//    AND gc_grace_seconds = 864000
//    AND max_index_interval = 2048
//    AND memtable_flush_period_in_ms = 0
//    AND min_index_interval = 128
//    AND read_repair_chance = 0.0
//    AND speculative_retry = '99.0PERCENTILE';

public string MakeColumns(TableColumn[] columns)
{

}

public void DescribeTable(Cluster dseCluster, KeyspaceMetadata keySpace, TableMetadata cqlTable, StreamWriter ddlWriteToFile)
{
	if (cqlTable != null)
	{
		ddlWriteToFile.WriteLine();

		ddlWriteToFile.WriteLine("CREATE TABLE IF NOT EXISTS {0}.{1} (\n\r{2}\tPRIMARY KEY({3})\n\r) WITH {4};",
									keySpace.Name,
									cqlTable.Name
									);
	}
}
// Define other methods and classes here
