<Query Kind="Program">
  <Reference Relative="Cassandra.Data.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.dll</Reference>
  <Reference Relative="Cassandra.Data.Linq.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.Linq.dll</Reference>
  <Reference Relative="Cassandra.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.dll</Reference>
  <Reference Relative="Crc32C.NET.dll">&lt;MyDocuments&gt;\LINQPad Queries\Crc32C.NET.dll</Reference>
  <Reference Relative="LZ4.dll">&lt;MyDocuments&gt;\LINQPad Queries\LZ4.dll</Reference>
  <Reference Relative="Snappy.NET.dll">&lt;MyDocuments&gt;\LINQPad Queries\Snappy.NET.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>System.Net.NetworkInformation</Namespace>
</Query>

void Main()
{
	var hostConnectionArray = new string[] { "10.200.241.16", "10.200.241.18" };
	string dataCenterName = "DC1"; //If not null, the DC Aware Policy is used. If null, the driver's default policies are used.
	string userName = null;
	string password = null;
	string validateKeySpace = "v2_readonly";
	int defaultRowLimitSize = 6; //Should be the number of nodes in the cluster plus 1 for proper testing. For small cluster, doubling the number of nodes plus 1 might provide better reuslts for testing node access patterns.
									//For volumn testing it is suggested to use an approate value for readPageSize. It default to this value.
	int hostConnectionPort = 9042;
	var readConsistency = ConsistencyLevel.LocalQuorum;
	var compression = CompressionType.Snappy;
	bool useSSL = false; //nop right now
	
	//CQL Diagnostics Options
	bool reportAllKeyspaces = false; //If true, all keyspaces and tables within the cluster are reported. If false, only the kesyapce defined in validateKeySpace is reported
	bool enableCQLPlan = false; //If true, each CQL statement will include a detail plan which includes server side information
	bool enableCQLPlanForKeyedQuries = false; //if true, each CQL statement that use a keyed value will include a detailed plan which includes server side information
	bool enableHostQueried = true; //If true, the host where the query was sent is displayed plus if any additional hosts are tried and failed
	bool enableQueryConsistenceLevelObtained = true; //If true, displays the level of consistence that was obtained on the server
	bool enableDriveDiagnostics = false; //If true, the internal driver diagnostics information is displayed. Note that this may display duplicate information!
	
	//Entity Test/Diagnostics Options
	bool enablePersonTest = false;
	bool enableEncounterTest = false;
	bool enableDocumentTest = true;
	bool enableClaimTest = false;
	
	int readPageSize = defaultRowLimitSize;
	
	//CQL String Statements
	var cqlStringPerson = string.Format("select \"Id\", \"EprsVersionNumber\" from {0}.person limit {1};",
											validateKeySpace,
											defaultRowLimitSize);
	var cqlStringPersonKeyedQuery = string.Format("select \"Id\", \"EprsVersionNumber\" from {0}.person where \"Id\" = ?;",
														validateKeySpace);
	var cqlStringEncounter = string.Format("select \"Id\", \"EprsVersionNumber\" from {0}.encounter limit {1};",
											validateKeySpace,
											defaultRowLimitSize);
	var cqlStringEncounterKeyedQuery = string.Format("select \"Id\", \"EprsVersionNumber\" from {0}.encounter where \"Id\" = ?;",
														validateKeySpace);
	var cqlStringDocument = string.Format("select \"Id\", \"EprsVersionNumber\" from {0}.document limit {1};",
											validateKeySpace,
											defaultRowLimitSize);
	var cqlStringDocumentKeyedQuery = string.Format("select \"Id\", \"EprsVersionNumber\" from {0}.document where \"Id\" = ?;",
														validateKeySpace);
	var cqlStringClaim = string.Format("select \"EncounterId\", \"Version\" from {0}.claim limit {1};",
											validateKeySpace,
											defaultRowLimitSize);
	var cqlStringClaimKeyedQuery = string.Format("select \"EncounterId\", \"Version\" from {0}.claim where \"EncounterId\" = ?;",
													validateKeySpace);
	
	var ignoreKeyspaces = new string[] { "system_traces", "OpsCenter", "system_auth", "system" };
	
	if(enableDriveDiagnostics)
	{
		Console.WriteLine("Enabling Driver Diagnostics");
		Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
		//System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener()); 
	}
		
	//Build the cluster and connect
	Console.WriteLine("Building Connection with Hosts: {0}\n\tPort: {1}\n\tData Center: {2}\n\tUser Name and Password Present: {3}\n\tSSL Enabled: {7}\n\tKeyspace: {4}\n\tCompression: {5}\n\tConsistency: {6}",
						string.Join(",", hostConnectionArray),
						hostConnectionPort,
						dataCenterName,
						string.IsNullOrEmpty(userName) ? "No" : "Yes",
						validateKeySpace,
						compression,
						readConsistency,
						useSSL);
						
	using (var cluster = AddCredentials(Cluster
											.Builder()
											.WithConnectionString(string.Format("Contact Points={0}; Port={1}",
																					string.Join(",", hostConnectionArray),
																					hostConnectionPort))
											.WithoutRowSetBuffering()
											.WithLoadBalancingPolicy(string.IsNullOrEmpty(dataCenterName)
																				? Cassandra.Policies.DefaultLoadBalancingPolicy
																				: new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
											.WithQueryOptions(new QueryOptions().SetConsistencyLevel(readConsistency).SetPageSize(readPageSize)),
												userName,
												password,
												null, //validateKeySpace,
												useSSL,
												null)
											.Build())
	using (var session = cluster.Connect())
	{
		Console.WriteLine("Connection Built, session created, and connected to C* cluster" +
							"\n\tCluster: {0}" +
							"\n\tBinary Protocol Version: {1}" +
							"\n\tDefault Keyspace: {2}" +
							"\n\tConsistency Level: {3}" +
							"\n\tQuery Page Size: {4}" +
							"\n\tPolicy: \"{5}\"" +
							"\tLoad Balancing Policy: \"{6}\"" +
							"\tReconnection Policy: \"{7}\"" +
							"\tRetry Policy: \"{8}\"",
								cluster.Metadata.ClusterName,
								session.BinaryProtocolVersion,
								session.Keyspace ?? "<none>",
								session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(),
								session.Cluster.Configuration.QueryOptions.GetPageSize(),
								session.Cluster.Configuration.Policies.GetType().Name,
								session.Cluster.Configuration.Policies.LoadBalancingPolicy.GetType().Name,
								session.Cluster.Configuration.Policies.ReconnectionPolicy.GetType().Name,
								session.Cluster.Configuration.Policies.RetryPolicy.GetType().Name
							);
		
		cluster.Metadata.HostsEvent += Metadata_HostsEvent;
		
		Console.WriteLine("Dumping Node Metadata Information:");
		foreach (var nodeInfo in session.Cluster.AllHosts())
		{
			nodeInfo.Dump(nodeInfo.Address.ToString(), 1);
		}
		
		Console.WriteLine("Dumping Keyspace Metadata Information:");
		foreach (var keyspaceName in reportAllKeyspaces 
										? cluster.Metadata.GetKeyspaces()
										: (ICollection<string>) new List<string>() { validateKeySpace })
		{
			if(ignoreKeyspaces == null || !ignoreKeyspaces.Contains(keyspaceName))
			{
				var keyspaceInfo = cluster.Metadata.GetKeyspace(keyspaceName);
				keyspaceInfo.Dump(keyspaceName);
				keyspaceInfo.GetTablesMetadata().Dump(string.Format("Table Metadata from KeySpace {0}", keyspaceName));
			}
		}
		
		Console.WriteLine("Starting Entry Testing...");
		
		if(enablePersonTest)
		{
			PerformEntityTest(session, cqlStringPerson, cqlStringPersonKeyedQuery, "Person", enableCQLPlan, enableHostQueried, enableQueryConsistenceLevelObtained, enableCQLPlanForKeyedQuries);
		}
		else
		{
			Console.WriteLine("++ Not Performing Person Entity Testing!");
		}
		
		if(enableEncounterTest)
		{
			PerformEntityTest(session, cqlStringEncounter, cqlStringEncounterKeyedQuery, "Encounter", enableCQLPlan, enableHostQueried, enableQueryConsistenceLevelObtained, enableCQLPlanForKeyedQuries);
		}
		else
		{
			Console.WriteLine("++ Not Performing Encounter Entity Testing!");
		}
		
		if(enableDocumentTest)
		{
			PerformEntityTest(session, cqlStringDocument, cqlStringDocumentKeyedQuery, "Document", enableCQLPlan, enableHostQueried, enableQueryConsistenceLevelObtained, enableCQLPlanForKeyedQuries);
		}
		else
		{
			Console.WriteLine("++ Not Performing Document Entity Testing!");
		}
		
		if(enableClaimTest)
		{
			PerformEntityTest(session, cqlStringClaim, cqlStringClaimKeyedQuery, "Claim", enableCQLPlan, enableHostQueried, enableQueryConsistenceLevelObtained, enableCQLPlanForKeyedQuries);
		}
		else
		{
			Console.WriteLine("++ Not Performing Claim Entity Testing!");
		}
	}
}

Builder AddCredentials(Builder clusterBuilder, string userName, string password, string defaultKeyspace, bool useSSL, PoolingOptions poolingOptions)
{
	if(!string.IsNullOrEmpty(userName))
	{
		clusterBuilder = clusterBuilder.WithCredentials(userName, password);
	}
	
	if(!string.IsNullOrEmpty(defaultKeyspace))
	{
		clusterBuilder = clusterBuilder.WithDefaultKeyspace(defaultKeyspace);
	}
	
	if(poolingOptions != null)
	{
		clusterBuilder = clusterBuilder.WithPoolingOptions(poolingOptions);
	}
	
	return clusterBuilder;
}

void DisplayQueryInfo(string cqlString, ExecutionInfo queryInfo, bool enableCQLPlan, bool enableHostQueried, bool enableConsistanceLevel)
{
	if(enableConsistanceLevel || enableCQLPlan || enableHostQueried)
	{
		Console.WriteLine("\tTrace Information for CQL Statement \"{0}\"", cqlString);
	}
	
	if(enableHostQueried)
	{
		var triedHosts = queryInfo.TriedHosts == null || queryInfo.TriedHosts.Count == 0 
							? null
							: queryInfo.TriedHosts.Where(ipAddress => ipAddress != queryInfo.QueriedHost);
							
		Console.WriteLine("\t\t\tQuery Sent to Node {0} (Tried the following hosts but failed: {1})", 
							queryInfo.QueriedHost,
							triedHosts == null || triedHosts.Count() == 0 
								? "<No Failures>"
								: string.Join(", ", triedHosts));
	}
	
	if(enableConsistanceLevel)
	{
		Console.WriteLine("\t\t\tObtained Consistency Level: {0}", queryInfo.AchievedConsistency);
	}
	
	if(enableCQLPlan)
	{
		queryInfo.QueryTrace.Dump("\t\t\t", 2);
	}
}

void PerformEntityTest(ISession session, string selectCQL, string selectCQLKeyed, string entity, bool enableCQLPlan, bool enableHostQueried, bool enableQueryConsistenceLevelObtained, bool enableCQLPlanForKeyedQuries, bool enableRoutingKey = false)
{	
	if(string.IsNullOrEmpty(selectCQL))
	{
		Console.WriteLine("++ Not Performing {0} Entity Testing!", entity);
	}
	else
	{
		Console.WriteLine("== Performing {0} Entity Testing at {1:HH:MM:ss}!\n\tPreparing CQL Statements", entity, DateTime.Now);
		
		var selectCQLPrepare = session.Prepare(selectCQL);
		var selectCQLKeyedQueryPrepare = session.Prepare(selectCQLKeyed);
		
		Console.WriteLine("\tPrepares Completed\n\tStarting Select for Primary Query");
		
		IStatement boundStmt = selectCQLPrepare.Bind();
		
		if(enableCQLPlan)
		{
			boundStmt = boundStmt.EnableTracing();
		}
		
		var resultSet = session.Execute(boundStmt);
		
		DisplayQueryInfo(selectCQL, resultSet.Info, enableCQLPlan, enableHostQueried, enableQueryConsistenceLevelObtained);
		
		var rows = resultSet.GetRows().ToArray();
		var pkValues = new List<string>();
		int nbrRows = 0;
		int nbrKeyedRows = 0;
		
		Console.WriteLine("\t{1} Keys Retrieved, Starting Select for one Partition Key Value.{0}",
							enableRoutingKey ? " Routing Key is Enabled!" : string.Empty,
							rows.Length);
		
		foreach (var row in rows)
		{
			++nbrRows;
			
			boundStmt = selectCQLKeyedQueryPrepare.Bind(row.GetValue<string>(0));
			
			if(enableRoutingKey)
			{
				boundStmt = ((BoundStatement) boundStmt).SetRoutingKey(new RoutingKey() { RawRoutingKey = System.Text.Encoding.UTF8.GetBytes(row.GetValue<string>(0)) });
			}
			
			if(enableCQLPlanForKeyedQuries)
			{
				boundStmt = boundStmt.EnableTracing();
			}
		
			var keyedResultSet = session.Execute(boundStmt);
			
			DisplayQueryInfo(string.Format("{0}({1})", selectCQLKeyed, row.GetValue<string>(0)), keyedResultSet.Info, enableCQLPlanForKeyedQuries, enableHostQueried, enableQueryConsistenceLevelObtained);
		
			var firstRow = keyedResultSet.GetRows().FirstOrDefault();
			
			if(firstRow != null && firstRow.GetValue<string>(0) == row.GetValue<string>(0))
			{
				++nbrKeyedRows;
			}
			else
			{
				Console.WriteLine("**** Warning: Row was not found or keys did not match! Trying to Match Key: {0} but Found {1}",
									row.GetValue<string>(0),
									firstRow == null ? "<No Row Found>" : firstRow.GetValue<string>(0));
			}
		}
		
		Console.WriteLine("== {0} Table Results; Total Rows Read: {1:###,###,##0}; Rows Read via Key: {2:###,###,##0} (Row Counts should match);\tEnded at {3:HH:MM:ss}", entity, nbrRows, nbrKeyedRows, DateTime.Now);	
	}
}

void Metadata_HostsEvent(object sender, HostsEventArgs e)
{
	Console.WriteLine("**** Warning: Node {0} has a Host Event of \"{2}\"", e.IPAddress, e.What);
}