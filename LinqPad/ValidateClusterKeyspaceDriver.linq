<Query Kind="Program">
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>System.Net.NetworkInformation</Namespace>
  <Namespace>System.Net.Security</Namespace>
  <Namespace>System.Security.Cryptography.X509Certificates</Namespace>
</Query>

/*
	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
	INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A 
	PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
	HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
	CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
	OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
	
	Written by Richard Andersen DataStax
*/
void Main()
{
	var hostConnectionArray = new string[] { "192.168.247.61" }; //An array of C* nodes used to connect to the cluster
	string dataCenterName = "DC1"; //If not null, the DC Aware Policy is used. If null, the driver's default policies are used.
	string userName = null;
	string password = null;
	string validateKeySpace = "testks";
	int defaultRowLimitSize = 4; //Should be the number of nodes in the cluster plus 1 for proper testing. For small cluster, doubling the number of nodes plus 1 might provide better reuslts for testing node access patterns.
									//For volumn testing it is suggested to use an approate value for readPageSize. It default to this value.
	int hostConnectionPort = 9042;
	int readPageSize = defaultRowLimitSize;
	var readConsistency = ConsistencyLevel.LocalQuorum;
	var compression = CompressionType.LZ4;
	bool useSSL = false; 
	string[] sslCertPaths = null; //A list of certificate paths where the certs can be found that were generated on the C* node.
	string[] sslCertPasswords = null; //A list of corresponding passwords assoicated with the certificates paths.
	
	//CQL Diagnostics Options
	bool reportAllKeyspaces = false; //If true, all keyspaces and tables within the cluster are reported. If false, only the kesyapce defined in validateKeySpace is reported
	bool enableCQLPlan = true; //If true, each CQL statement will include a detail plan which includes server side information
	bool enableCQLPlanForKeyedQuries = true; //if true, each CQL statement that use a keyed value will include a detailed plan which includes server side information
	bool enableHostQueried = true; //If true, the host where the query was sent is displayed plus if any additional hosts are tried and failed
	bool enableQueryConsistenceLevelObtained = true; //If true, displays the level of consistence that was obtained on the server
	bool enableDriveDiagnostics = false; //If true, the internal driver diagnostics information is displayed. Note that this may display duplicate information!

	//CQL String Statements
	var cqlStringQuery = string.Format("select * from {0}.testtbl limit {1};",
											validateKeySpace,
											defaultRowLimitSize);
	var cqlStringKeyedQuery = string.Format("select * from {0}.testtbl where pkuuid = ? and colint = ? limit {1};",
											validateKeySpace,
											defaultRowLimitSize);
	
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
												null,
												sslCertPaths,
												sslCertPasswords)
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
		
		PerformEntityTest(session, cqlStringQuery, cqlStringKeyedQuery, "CQL Test", enableCQLPlan, enableHostQueried, enableQueryConsistenceLevelObtained, enableCQLPlanForKeyedQuries);
	}
}

Builder AddCredentials(Builder clusterBuilder, string userName, string password, string defaultKeyspace, bool useSSL, PoolingOptions poolingOptions, string[] certPaths, string[] certPasswords)
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
	
	if(useSSL && certPaths != null && certPaths.Length > 0)
	{
		var certs = new X509CertificateCollection();
	
		for(int nIndex = 0; nIndex < certPaths.Length; ++nIndex)
		{
			certs.Add(new X509Certificate(certPaths[nIndex],
											certPasswords == null || certPasswords.Length == 0 || certPasswords.Length >= nIndex
												? null
												: certPasswords[nIndex]));		
		}
		
		RemoteCertificateValidationCallback callback = (s, cert, chain, policyErrors) =>
		{				
				//A "real" certificate was given!
				if (policyErrors == SslPolicyErrors.None)
				{
					return true; 
				}
				
				//Since this is a "Global" and locally crated certificate the CN names will not match and the certificate chaining will be incorrect
				//Policy Errors will be RemoteCertificateNameMismatch and RemoteCertificateChainErrors
				//Can add some additional checking like the cert.IssuerName, etc.
				if (policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateChainErrors) && 
					chain.ChainStatus.Length == 1 && 
					chain.ChainStatus[0].Status == X509ChainStatusFlags.UntrustedRoot)
				{
					//Console.WriteLine("True");
					return true;
				}
								
				return false;
		};
		var sslOptions = new SSLOptions().SetCertificateCollection(certs).SetRemoteCertValidationCallback(callback);
		
		clusterBuilder = clusterBuilder.WithSSL(sslOptions);
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
			
			boundStmt = selectCQLKeyedQueryPrepare.Bind(row.GetValue<Guid>(0), row.GetValue<int>(1));
			
			if(enableRoutingKey)
			{
				boundStmt = ((BoundStatement) boundStmt).SetRoutingKey(new RoutingKey() { RawRoutingKey = System.Text.Encoding.UTF8.GetBytes(row.GetValue<string>(0)) });
			}
			
			if(enableCQLPlanForKeyedQuries)
			{
				boundStmt = boundStmt.EnableTracing();
			}
		
			var keyedResultSet = session.Execute(boundStmt);
			
			DisplayQueryInfo(string.Format("{0}({1}, {2})", selectCQLKeyed, row.GetValue<Guid>(0), row.GetValue<int>(1)), keyedResultSet.Info, enableCQLPlanForKeyedQuries, enableHostQueried, enableQueryConsistenceLevelObtained);
		
			var firstRow = keyedResultSet.GetRows().FirstOrDefault();
			
			if(firstRow != null && firstRow.GetValue<Guid>(0) == row.GetValue<Guid>(0) && firstRow.GetValue<int>(1) == row.GetValue<int>(1))
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
	Console.WriteLine("**** Warning: Node {0} has a Host Event of \"{2}\"", e.Address, e.What);
}