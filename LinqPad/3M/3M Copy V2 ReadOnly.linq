<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\DataStax 2.5.0\Cassandra.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Shared.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Singleton.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\DataStax 2.1.5\src\Cassandra\bin\Release\Crc32C.NET.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\DataStax 2.1.5\src\Cassandra\bin\Release\LZ4.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\DataStax 2.1.5\src\Cassandra\bin\Release\Snappy.NET.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>Common</Namespace>
  <Namespace>Common.Patterns</Namespace>
</Query>


void Main()
{
	var connectionStringSource = "Contact Points=10.200.241.2"; //10.200.240.251"; //192.168.117.130";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	string userNameSource = "";
	string passwordSource = "";
	string dcNameSource = ""; //DataCenter Name
	var consistencyLevelSource = ConsistencyLevel.LocalOne;
	var compressionSource = CompressionType.LZ4;
	int pageSizeSource = 125;
	
	var connectionStringTarget = "Contact Points=192.168.5.130"; //10.200.240.251"; //192.168.117.130";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	string userNameTarget = "";
	string passwordTarget = "";
	string dcNameTarget = ""; //DataCenter Name
	var consistencyLevelTarget = ConsistencyLevel.LocalOne;
	var compressionTarget = CompressionType.LZ4;
	int pageSizeTarget = 500;
	
	using (var clusterSource = AddCredentials(Cluster
											.Builder()
											.WithConnectionString(connectionStringSource)
											.WithoutRowSetBuffering()
											.WithLoadBalancingPolicy(new TokenAwarePolicy(string.IsNullOrEmpty(dcNameSource)
																							? (ILoadBalancingPolicy) new RoundRobinPolicy()
																							: (ILoadBalancingPolicy) new DCAwareRoundRobinPolicy(dcNameSource)))
											.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(consistencyLevelSource).SetPageSize(pageSizeSource))
											//.WithSSL()
											.WithCompression(compressionSource),
											userNameSource,
											passwordSource
											)
							.Build())
	using (var sessionSource = clusterSource.Connect())
	using (var clusterTarget = AddCredentials(Cluster
											.Builder()
											.WithConnectionString(connectionStringTarget)
											.WithoutRowSetBuffering()
											.WithLoadBalancingPolicy(new TokenAwarePolicy(string.IsNullOrEmpty(dcNameTarget)
																							? (ILoadBalancingPolicy) new RoundRobinPolicy()
																							: (ILoadBalancingPolicy) new DCAwareRoundRobinPolicy(dcNameTarget)))
											.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(consistencyLevelTarget).SetPageSize(pageSizeTarget))
											//.WithSSL()
											.WithCompression(compressionTarget),
											userNameTarget,
											passwordTarget
											)
							.Build())
	using (var sessionTarget = clusterTarget.Connect())
	{
		var insertPrepareStmt = sessionTarget.Prepare("insert into v2_readonly.document (\"Id\", \"EprsVersionNumber\", \"CreateDateStamp\", \"EncounterId\", \"ExternalDocId\", \"LastUpdate\", \"PersonId\", \"RecordType\", \"Status\", \"XmlRecordLength\", \"XmlRecord\") values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		var insertTasks = new List<System.Threading.Tasks.Task>();
		
		using (var rowSet = sessionSource.Execute("select \"Id\", \"EprsVersionNumber\", \"CreateDateStamp\", \"EncounterId\", \"ExternalDocId\", \"LastUpdate\", \"PersonId\", \"RecordType\", \"Status\", \"XmlRecordLength\", \"XmlRecord\"  from v2_readonly.documentwolrgrecs;"))
		{
			foreach (var row in rowSet)
			{
				insertTasks.Add(sessionTarget.ExecuteAsync(insertPrepareStmt.Bind(row.ToArray())));
			}
		}
		
		insertTasks.ForEach(task => task.Wait());
	}
	
}

Builder AddCredentials(Builder clusterBuilder, string userName, string password)
{
	if(!string.IsNullOrEmpty(userName))
	{
		clusterBuilder = clusterBuilder.WithCredentials(userName, password);
	}
	
	return clusterBuilder;
}

// Define other methods and classes here
