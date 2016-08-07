<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data.Linq\bin\Release\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data.Linq\bin\Release\Cassandra.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\LZ4.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
</Query>

void Main()
{
	var connectionStringV1 = "Contact Points=192.168.58.132";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	
	using (var clusterV1 = Cluster
							.Builder()
							.WithConnectionString(connectionStringV1)
							.WithoutRowSetBuffering()
							.Build())
	using (var sessionV1 = clusterV1.Connect())
	{
		sessionV1.Execute("create keyspace IF NOT EXISTS test20140731 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
		
		sessionV1.Execute("CREATE TABLE IF NOT EXISTS test20140731.testtable (pk1 blob primary key, v1 blob)");
		
//		sessionV1.Execute("create keyspace test20140731 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
//		
//		sessionV1.Execute("CREATE TABLE test20140731.testtable (pk1 blob primary key, v1 blob)");
//		
		
		sessionV1.Execute("insert into test20140731.testtable (pk1, v1) values(textasblob('5f3c89cd-0c8b-4d80-b24d-2a660932b489'),textasblob('MRNa6f9a987-b450-4043-af04-730833eb0d3'));");
		
		var cqlQuery = new SimpleStatement("select blobastext(pk1), blobastext(v1) from test20140731.testtable where pk1 = textasblob(?);");
		var bindingObjects = new object[] {"5f3c89cd-0c8b-4d80-b24d-2a660932b489"};
		
		var resultset = sessionV1.Execute(cqlQuery.BindObjects(bindingObjects));
		
		resultset.Dump(2);
	}	
}