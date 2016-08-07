<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.1\src\Cassandra.Data\bin\Release\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.1\src\Cassandra.Data.Linq\bin\Release\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.1\src\Cassandra.Data\bin\Release\Cassandra.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.1\src\Cassandra.Data\bin\Release\Crc32C.NET.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.1\src\Cassandra.Data\bin\Release\LZ4.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.1\src\Cassandra.Data\bin\Release\Snappy.NET.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\packages\Microsoft.Tpl.Dataflow.4.5.14\lib\portable-net45+win8\System.Threading.Tasks.Dataflow.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
  <Namespace>System.Threading.Tasks.Dataflow</Namespace>
</Query>

void Main()
{
	var qryConnectionString = "Contact Points=169.10.60.206"; //"Contact Points=192.168.190.132"; Contact Points=127.0.0.1 WIN-DCE12X-3M
	var insConnectionString = "Contact Points=192.168.171.131"; 
	var cqlString = "select blobastext(key), value from \"KS055221\".\"Document\" where column1 = textasblob('XmlRecord') allow filtering;"; //KS055298 KSV1_VOLATILE 
	var cqlInsertString = "insert into test20140731.xmlrecord (id, value) values(?, ?);";  
	
	using (var qryCluster = Cluster
							.Builder()
							.WithConnectionString(qryConnectionString)
							.WithoutRowSetBuffering()
							.WithCompression(CompressionType.Snappy)
							.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(ConsistencyLevel.One).SetPageSize(1000).SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
							.WithLoadBalancingPolicy(new RetryLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()), new ConstantReconnectionPolicy(5000)))
							.Build())
	using (var qrySession = qryCluster.Connect())
	using(var rowSet = qrySession.Execute(cqlString))
	using (var insCluster = Cluster
							.Builder()
							.WithConnectionString(insConnectionString)
							.WithCompression(CompressionType.Snappy)
							//.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(ConsistencyLevel.Any).SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
							.WithLoadBalancingPolicy(new RetryLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()), new ConstantReconnectionPolicy(5000)))
							.Build())
	using (var insSession = insCluster.Connect())
	{
		int nbrInserts = 0;
		var insertSimpleStmt = new SimpleStatement(cqlInsertString);
		var qryRowBlock = new BatchedJoinBlock<Tuple<string,object>, Exception>(100);
		var insertBlock = new ActionBlock< Tuple< IList<Tuple<string,object>>, IList<Exception> >>(qryData =>
												{	
													var batchInsert = new BatchStatement();
													
													foreach (var qryTuple in qryData.Item1)
													{
														batchInsert.Add(insertSimpleStmt.Bind(qryTuple.Item1, qryTuple.Item2));
														++nbrInserts;
													}
													
													insSession.Execute(batchInsert);
												}
											);
		
		qryRowBlock.LinkTo(insertBlock);
		
		qryRowBlock.Completion.ContinueWith(delegate { insertBlock.Complete(); });
		
		var execTask = qrySession.ExecuteAsync(new SimpleStatement(cqlString))
									.ContinueWith(taskResult => 
													{
														foreach (Row row in taskResult.Result)
														{
															try
															{
																qryRowBlock.Target1.Post(new Tuple<string,object>(row.GetValue<string>("key"), row["value"]));
															}
															catch(System.Exception ex)
															{
																qryRowBlock.Target2.Post(ex);
															}
														}
														qryRowBlock.Complete();
													});
		execTask.Wait();
        insertBlock.Completion.Wait();
	}
	
}

// Define other methods and classes here