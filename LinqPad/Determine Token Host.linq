<Query Kind="Program">
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
</Query>

void Main()
{
	var connectionStringV1 = "Contact Points=10.200.241.2"; //192.168.117.128";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	var guid = "15e3b781-72e7-4a08-8fcb-f25bf93ac523";
	
	Cassandra.Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Verbose;
	Cassandra.Diagnostics.CassandraPerformanceCountersEnabled = true;
	Cassandra.Diagnostics.CassandraStackTraceIncluded = true;

	System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.ConsoleTraceListener());

	using (var clusterV1 = Cluster
							.Builder()
							.WithConnectionString(connectionStringV1)
							.WithoutRowSetBuffering()
							//.WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("DC1"))
							.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("DC1")))
							.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(ConsistencyLevel.One).SetPageSize(10))
							//.WithCredentials("none", "none")
							.Build())
	using (var sessionV1 = clusterV1.Connect())
	{
		
		Console.WriteLine("Before Prepare");
		
		var stmt = sessionV1.Prepare("select * from v2_readonly.document where \"Id\" = ?");
		
		Console.WriteLine("After Prepare; Before Bind");
		
		var y = stmt.Bind(guid)
					.SetRoutingKey(new RoutingKey() { RawRoutingKey = System.Text.Encoding.UTF8.GetBytes(guid) });
									
		Console.WriteLine("After Bind; Before Execute");
		
		var x = sessionV1.Execute(y);
		
		Console.WriteLine("After Excute; Before Dump");
		
		x.Dump(2);
		
		Console.WriteLine("After Dump");
	}	
}