<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\RunTime Test\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\RunTime Test\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\RunTime Test\Cassandra.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\RunTime Test\Crc32C.NET.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\RunTime Test\LZ4.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\RunTime Test\Snappy.NET.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>System.Threading</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.117.131";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M; 192.168.58.131; 169.10.60.206
	var cqlString = "select value from \"KSV1_VOLATILE\".\"DocumentAudit\" where column1 = textasblob('XmlRecord') allow filtering;"; //KS055298; KSV1_VOLATILE; KS055221
	int cqlQueryPagingSize = 1000; 
	var queryConsistencyLevel = ConsistencyLevel.One;
	
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.WithCompression(CompressionType.Snappy)
							.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(ConsistencyLevel.One).SetPageSize(1000).SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
							.WithLoadBalancingPolicy(new RetryLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()), new ConstantReconnectionPolicy(5000)))
							.Build())
	using (var session = cluster.Connect())
	{
	
     	Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
            
		int maxXmlRecrodLength = 0;
		
		session.ExecuteAsync(new SimpleStatement(cqlString).SetConsistencyLevel(queryConsistencyLevel).SetPageSize(cqlQueryPagingSize))
					.ContinueWith(taskItem =>
									{ 
										maxXmlRecrodLength = taskItem.Result.Max (r => ((byte[])r["value"]).Length);
									})
					.Wait();
									
		maxXmlRecrodLength.ToString("Bytes: ###,###,##0").Dump();
																			
	}		
	
}



// Define other methods and classes here