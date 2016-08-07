<Query Kind="Program">
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>System.Net.Security</Namespace>
  <Namespace>System.Security.Cryptography.X509Certificates</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=10.200.241.2"; //Change IP Address or a list of ip addresses seperated by a comma
	var dataCenterName = "DC1"; //Change to DC Name

	Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;	
	
	using (var cluster = Cluster
							.Builder()
							.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()							
							.WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.Quorum))
							.Build())
	using (var session = cluster.Connect())
	{
		Console.WriteLine("Connected!");
		
		
		
		Console.WriteLine("Connection Done");
	}		
}