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
  <Namespace>System.Threading.Tasks</Namespace>
</Query>


void Main()
{
	
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new TokenAwarePolicy(string.IsNullOrEmpty(dcName)
																			? (ILoadBalancingPolicy)new RoundRobinPolicy()
																			: (ILoadBalancingPolicy)new DCAwareRoundRobinPolicy(dcName)))
							.WithQueryOptions((new QueryOptions())
													.SetConsistencyLevel(defsultConsistencyLevel)
													.SetPageSize(defaultPageSize))
							//.WithPoolingOptions(poolingOptions)
							//.WithSSL()
							.WithCompression(compression)
							.WithCredentials(userName, password)
							.Build())
	using (var session = cluster.Connect())
	{
		var query = from user in users
					where user.Group == "admin"
					select user;
					
		IEnumerable<User> adminUsers = query.Execute();

	}
}

