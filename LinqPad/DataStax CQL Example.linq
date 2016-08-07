<Query Kind="Program">
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>System.Threading</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>

void Main()
{
	var connectionStringV1 = "Contact Points=10.200.241.2, 10.200.241.3"; //Connection String, can also include userName, password, port, and keyspace key words
	var myPrimaryKey = "15e3b781-72e7-4a08-8fcb-f25bf93ac523";
	var readConsistencyLevel = ConsistencyLevel.LocalQuorum;
	var writeConsistencyLevel = ConsistencyLevel.LocalQuorum;
	int pageSize = 100;
	var dataCenterName = "DC1";
	var transportCompressionType = CompressionType.LZ4;
	
	//Turn On the Drivers Diagnositics 
	Cassandra.Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Verbose;
	System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener());

	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionStringV1) //Can use AddContactPoint or AddContactPoints
							.WithoutRowSetBuffering()
							//.WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("DC1"))
							.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
							.WithQueryOptions((new QueryOptions()).SetPageSize(pageSize))
							.WithCompression(transportCompressionType)
							//.WithCredentials("User Name", "Passord")
							.Build())
	using (var session = cluster.Connect())
	{
		var preparedStmt = session.Prepare("select * from \"MyKeySpace\".\"MyTable\" where \"MyPrimaryKey\" = ? limit 1")
									.SetConsistencyLevel(readConsistencyLevel); //When using mixed case or upper case you must escape the keyspace, table, or column names. When using lowercase name excaping is not required!
		
		//Prepare using a bind plus the SetRoutingKey using the TokenAwarePolicy
		var bindingWRoutingKeyStmt = preparedStmt.Bind(myPrimaryKey)
										.SetRoutingKey(new RoutingKey() { RawRoutingKey = System.Text.Encoding.UTF8.GetBytes(myPrimaryKey) }); //Be carefull of endian
									
		var resultSet = session.Execute(bindingWRoutingKeyStmt);
		
		resultSet.Dump(2); //Dump is NOT part of the driver. It will display the row returned.
		
		//Use ExecAsysn Form (TPL) where one row is returned and update that row
		var readupdateTask = session.ExecuteAsync(preparedStmt.Bind(myPrimaryKey))
                              		.ContinueWith(taskResult
                                                		=>
                                                   		{
                                                    		//Need to enhance exception handling and Cancel or use TPL Filters
                                                           	if (taskResult.Exception != null)
                                                           	{
                                                          		throw taskResult.Exception;
                                                         	}
                                                      		else if (taskResult.IsCanceled)
                                                          	{
                                                            	throw new TaskCanceledException();
                                                          	}
 
                                                         	var firstRow = taskResult.Result.FirstOrDefault(); //Should only be one row anyway

															if(firstRow != null) //if null no rows are returned
															{
																var id = firstRow.GetValue<string>("MyPrimaryKey");

																return session.ExecuteAsync(new SimpleStatement("Update \"MyKeySpace\".\"MyTable\" set \"Field1\" = ? where \"MyPrimaryKey\" = ?")
																								.Bind("NewValue", id)
																								.SetConsistencyLevel(writeConsistencyLevel));
															}
															
															return taskResult; //if no rows returned, just return the already executed read task!
                                                     	},
                                                     	TaskContinuationOptions.AttachedToParent)
                                 	.Unwrap();

		readupdateTask.Wait(); //Wait for the read/upate to complete
	}
}