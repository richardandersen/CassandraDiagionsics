<Query Kind="Program">
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
</Query>

void Main()
{
	var hostConnectionArray = new string[] { "192.168.117.130" }; //, "10.200.241.16", "10.200.241.18" };
	string dataCenterName = null; //"DC1"; //If not null, the DC Aware Policy is used. If null, the driver's default policies are used.
	string userName = null;
	string password = null;

	var hosts = Util.ReadLine("Enter C* Node(s) Address for Cluster Keyspaces that willbe trucated", string.Join(", ", hostConnectionArray));

	//CQL String Statements
	var cqlStatements = new string[] {
										"TRUNCATE  {0}.person;",
										"TRUNCATE {0}.personbymrn;",
										"TRUNCATE {0}.encounter;",
										"TRUNCATE {0}.encounterbydischargedate;",
										"TRUNCATE {0}.encounterbyvisit;",
										"TRUNCATE {0}.encountersupportingdocument;",
										"TRUNCATE {0}.document;",
										"TRUNCATE {0}.documentbyexternaldocid;",
										"TRUNCATE {0}.supportingdocument;",
										"TRUNCATE {0}.claim"
									};
	var ignoreKeyspaces = new string[] { "system_traces", "OpsCenter", "system_auth", "system" };
	bool enableDriveDiagnostics = false;
	int hostConnectionPort = 9042;
	var readConsistency = ConsistencyLevel.LocalQuorum;
	var compression = CompressionType.Snappy;
	bool useSSL = false;
	bool autoDetectEPRSCQLKeyspaces = true;
	var useCQLTablesNameForAutoDetect = new string[] {"person",
														"personbymrn",
														"encounter",
														"encounterbydischargedate",
														"encounterbyvisit",
														"encountersupportingdocument",
														"document",
														"documentbyexternaldocid",
														"supportingdocument" };

	if (enableDriveDiagnostics)
	{
		Console.WriteLine("Enabling Driver Diagnostics");
		Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
		//System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener()); 
	}

	//Build the cluster and connect
	Console.WriteLine("Building Connection with Hosts: {0}\n\tPort: {1}\n\tData Center: {2}\n\tUser Name and Password Present: {3}\n\tSSL Enabled: {6}\n\tCompression: {4}\n\tConsistency: {5}",
						string.Join(",", hostConnectionArray),
						hostConnectionPort,
						dataCenterName,
						string.IsNullOrEmpty(userName) ? "No" : "Yes",
						compression,
						readConsistency,
						useSSL);

	using (var cluster = AddCredentials(Cluster
											.Builder()
											.WithConnectionString(string.Format("Contact Points={0}; Port={1}",
																					hosts,
																					hostConnectionPort))
											.WithoutRowSetBuffering()
											.WithLoadBalancingPolicy(string.IsNullOrEmpty(dataCenterName)
																				? Cassandra.Policies.DefaultLoadBalancingPolicy
																				: new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName))),
												//.WithQueryOptions(new QueryOptions().SetConsistencyLevel(readConsistency).SetPageSize(readPageSize)),
												userName,
												password,
												null,
												useSSL,
												null)
											.Build())
	using (var session = cluster.Connect())
	{
		Console.WriteLine("Connection Built, session created, and connected to C* cluster" +
							"\n\tCluster: {0}" +
							"\n\tBinary Protocol Version: {1}",
								cluster.Metadata.ClusterName,
								session.BinaryProtocolVersion
							);

		cluster.Metadata.HostsEvent += Metadata_HostsEvent;

		Console.WriteLine("Dumping Keyspace Information...");

		var keyspacesTruncate = cluster.Metadata.GetKeyspaces().Where(ksName => ignoreKeyspaces == null || !ignoreKeyspaces.Contains(ksName));

		if (autoDetectEPRSCQLKeyspaces)
		{
			keyspacesTruncate = from ksName in keyspacesTruncate
								let tables = session.Cluster.Metadata.GetTables(ksName)
								where useCQLTablesNameForAutoDetect.All(tn => tables.Contains(tn))
								select ksName;
		}

		var anw = Util.ReadLine(string.Format("\n\nDo you really wish to truncate the following keyspaces in cluster \"{0}\".\n\tKeyspaces are:\n\t\t\t{1}\n\tEnter \"Yes\" to Continue or \"No\" to exist (default is \"Yes\")",
												cluster.Metadata.ClusterName,
												string.Join(",\n\t\t\t", keyspacesTruncate)), "yes");
		Console.WriteLine();
		Console.WriteLine();

		if (anw == "y" || anw == "yes" || string.IsNullOrEmpty(anw))
		{
			var taskList = new List<Task>(cqlStatements.Length * keyspacesTruncate.Count());

			foreach (var keyspaceName in keyspacesTruncate)
			{
				Console.WriteLine("Truncating Keyspace {0}", keyspaceName);
				foreach (var cqlStmt in cqlStatements)
				{
					taskList.Add(TruncateKeySpace(session, keyspaceName, cqlStmt));
				}
			}

			taskList.ForEach(task => { if (task != null) { task.Wait(); } });
		}
		else
		{
			Console.WriteLine("\nExiting Application");
		}
	}
}

Builder AddCredentials(Builder clusterBuilder, string userName, string password, string defaultKeyspace, bool useSSL, PoolingOptions poolingOptions)
{
	if (!string.IsNullOrEmpty(userName))
	{
		clusterBuilder = clusterBuilder.WithCredentials(userName, password);
	}

	if (!string.IsNullOrEmpty(defaultKeyspace))
	{
		clusterBuilder = clusterBuilder.WithDefaultKeyspace(defaultKeyspace);
	}

	if (poolingOptions != null)
	{
		clusterBuilder = clusterBuilder.WithPoolingOptions(poolingOptions);
	}

	return clusterBuilder;
}

Task TruncateKeySpace(ISession session, string ksName, string cqlStatement)
{
	var cqlFmtStmt = string.Format(cqlStatement, ksName);
	try
	{
		return session.ExecuteAsync(new SimpleStatement(cqlFmtStmt))
				.ContinueWith(result =>
								{
									if (result.IsFaulted || result.Exception != null)
									{
										Console.WriteLine("CQL Statement \"{0}\" Failed with Excelption {1} with Message \"{2}\". Exception is Ignored!",
															cqlFmtStmt,
															result.Exception == null ? "Faulted" : result.Exception.GetType().Name,
															result.Exception == null ? "N/A" : result.Exception.Message);

										result.Exception.Dump(1);
									}
								},
								TaskContinuationOptions.AttachedToParent);
	}
	catch (Exception ex)
	{
		Console.WriteLine("CQL Statement \"{0}\" Failed with Excelption {1} with Message \"{2}\". Task not created and Exception is Ignored!",
							cqlFmtStmt,
							ex.GetType().Name,
							ex.Message);

		ex.Dump(1);
	}

	return null;
}

void Metadata_HostsEvent(object sender, HostsEventArgs e)
{
	Console.WriteLine("**** Warning: Node {0} has a Host Event of \"{2}\"", e.IPAddress, e.What);
}