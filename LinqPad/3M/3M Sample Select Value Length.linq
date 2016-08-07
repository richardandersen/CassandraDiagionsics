<Query Kind="Program">
  <Reference Relative="Cassandra.Data.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.dll</Reference>
  <Reference Relative="Cassandra.Data.Linq.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.Linq.dll</Reference>
  <Reference Relative="Cassandra.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.dll</Reference>
  <Reference Relative="LZ4.dll">&lt;MyDocuments&gt;\LINQPad Queries\LZ4.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
</Query>

void Main()
{
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString("Contact Points=192.168.200.132")
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
							.WithQueryOptions((new Cassandra.QueryOptions())
												.SetConsistencyLevel(ConsistencyLevel.Quorum)
												.SetSerialConsistencyLevel(ConsistencyLevel.Serial)
												.SetPageSize(1000))
							.Build())
	using (var session = cluster.Connect())
	using (var rowSet = session.Execute("select \"Id\", \"XmlRecord\" from v2_volatile.encounter;"))
	{
		
		
		//not sure what 3M actually wants but here is a list of ids and lenghts
		var idLenCollection = new List<Tuple<string,int>>();
		
		foreach (var row in rowSet.GetRows())
		{
			idLenCollection.Add(new Tuple<string,int>(row.GetValue<string>(0), row.GetValue<string>(1).Length));
		}
		
		idLenCollection.OrderBy (lc => lc.Item2).Dump(1);
	}
}

// Define other methods and classes here
