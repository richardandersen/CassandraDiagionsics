<Query Kind="Program">
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <Namespace>Cassandra</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.126.132"; //10.200.241.2"; //Change IP Address or a list of ip addresses seperated by a comma
	var dataCenterName = "datacenter1"; //Change to DC Name

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
		
		var resultSet = session.Execute("INSERT INTO rha_test.test1tbl(id, version, status) VALUES ('A', 1, 'thrid') IF NOT EXISTS"); 
		var firstRow = resultSet.GetRows().First(); //Should always be at least one row, but you never know...
              
		if(firstRow.GetValue<bool>(0))
     	{
    		///Row was indeed inserted	
			Console.WriteLine("Inserted");
			firstRow.Dump();
  		}
  		else //Should be the already existing row!!!
     	{
     		Console.WriteLine("Already Exists (below is the existing row)");
           	firstRow.Dump();
    	}
	}
	
	Console.WriteLine("Connection Done");
}

// Define other methods and classes here
