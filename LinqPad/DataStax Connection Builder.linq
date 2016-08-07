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
  <Namespace>Cassandra.Serialization.Primitive</Namespace>
  <Namespace>LZ4</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.117.132"; //10.200.240.251"; //192.168.117.130";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	string userName = "";
	string password = "";
	string dcName = ""; //DataCenter Name
	string keySpaceNameOrAll = "\"KSV1_VOLATILE\""; //If null or empty string, all Thrift keyspaces will be used. If a name, only that keyspace will be used.
	var consistencyLevel = ConsistencyLevel.LocalOne;
	var compression = CompressionType.Snappy;
	int pageSize = 500;
	bool checkDataConsistence = true;
	bool runConcurrently = false;

	var idxMrnPersonCQL = "select blobastext(key), blobastext(column1), blobastext(value) from {0}.\"idxMrnPerson\";";
	var idxPersonIdEncounterCQL = "select blobastext(key), blobastext(column1), blobastext(value) from {0}.\"idxPersonIdEncounter\" where key = textasblob(?);";
	var encounterTableCQL = "select blobastext(key), blobastext(column1), blobastext(value) from {0}.\"Encounter\" where key = textasblob(?) and column1 in (textasblob('VisitNumber'), textasblob('Status'), textasblob('EPRSVersion'), textasblob('PersonID'), textasblob('FacilityID'));";


	var possibleExceptions = new List<Tuple<string, string, object>>();

	using (var cluster = AddCredentials(Cluster
											.Builder()
											.WithConnectionString(connectionString)
											.WithoutRowSetBuffering()
											.WithLoadBalancingPolicy(new TokenAwarePolicy(string.IsNullOrEmpty(dcName)
																							? (ILoadBalancingPolicy)new RoundRobinPolicy()
																							: (ILoadBalancingPolicy)new DCAwareRoundRobinPolicy(dcName)))
											.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(consistencyLevel).SetPageSize(pageSize))
											//.WithSSL()
											.WithCompression(compression),
											userName,
											password
											)
							.Build())
	using (var session = cluster.Connect())
	{
		
		foreach (var keySpace in string.IsNullOrEmpty(keySpaceNameOrAll) ? GetKeySpaces(session, null) : (IEnumerable<string>)new string[] { keySpaceNameOrAll })
		{
		}
	}
}

Builder AddCredentials(Builder clusterBuilder, string userName, string password)
{
	if (!string.IsNullOrEmpty(userName))
	{
		clusterBuilder = clusterBuilder.WithCredentials(userName, password);
	}

	return clusterBuilder;
}

public IEnumerable<string> GetKeySpaces(ISession session, string mustHaveTable)
{
	var keySpaces = new List<string>();

	foreach (var keySpaceName in session.Cluster.Metadata.GetKeyspaces())
	{
		if (keySpaceName.StartsWith("KS") && (mustHaveTable == null || session.Cluster.Metadata.GetTables(keySpaceName).Contains(mustHaveTable)))
		{
			Console.Write(keySpaceName + ", ");
			keySpaces.Add(keySpaceName);
		}
	}

	return keySpaces;
}

void RunInParallel<T>(IEnumerable<T> collection, bool runInParallel, Action<T> action)
{
	if (runInParallel)
	{
		System.Threading.Tasks.Parallel.ForEach(collection,
													element => action(element));
	}
	else
	{
		foreach (var element in collection)
		{
			action(element);
		}
	}
}

