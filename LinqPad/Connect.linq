<Query Kind="Program">
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>Cassandra.Mapping</Namespace>
  <Namespace>Cassandra.Mapping.Attributes</Namespace>
  <Namespace>Cassandra.Mapping.TypeConversion</Namespace>
  <Namespace>Cassandra.Mapping.Utils</Namespace>
  <Namespace>Cassandra.Serialization</Namespace>
</Query>

void Main()
{
	var hostConnectionArray = new string[] { "192.168.247.61" }; //, "10.200.241.16", "10.200.241.18" };
	string dataCenterName = null; //If not null, the DC Aware Policy is used. If null, the driver's default policies are used.
	string userName = null;
	string password = null;

	var hosts = Util.ReadLine("Enter C* Node(s) Address for Cluster Keyspaces that willbe trucated", string.Join(", ", hostConnectionArray));

	bool enableDriveDiagnostics = false;
	int hostConnectionPort = 9042;
	var readConsistency = ConsistencyLevel.LocalQuorum;
	var compression = CompressionType.Snappy;
	bool useSSL = false;
	
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
		//
		//		Console.WriteLine("Dumping Keyspace Information...");
		//
		//		cluster.Metadata.ClusterName.Dump("ClusterName");
		//		cluster.Metadata.GetKeyspaces().Dump("KeySpaces");
		//		cluster.Metadata.AllHosts().Dump("AllHosts");

		var rows = session.Execute(new SimpleStatement("select * from keyspace1.standard1 limit 1").SetConsistencyLevel(ConsistencyLevel.LocalOne));

		rows.Dump();
		
		session.Execute(new SimpleStatement("truncate keyspace1.standard1"));
		
		Console.WriteLine("\nExiting Application");
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

void Metadata_HostsEvent(object sender, HostsEventArgs e)
{
	Console.WriteLine("**** Warning: Node {0} has a Host Event of \"{1}\"", e.Address, e.What);
}