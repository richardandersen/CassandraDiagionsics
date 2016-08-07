<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\Thrift-Perf-Test\bin\Release\Apache.Cassandra.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\Thrift-Perf-Test\bin\Release\Cassandraemon.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\Thrift-Perf-Test\bin\Release\Thrift.dll</Reference>
  <Namespace>Cassandraemon</Namespace>
  <Namespace>Cassandraemon.Connection</Namespace>
</Query>

void Main()
{
	//var connectionStringV1 = "192.168.117.128";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	
	var builderReadOnly = new CassandraConnectionConfigBuilder
			{
				Hosts = new string[] {  "192.168.117.128" },
				Port = 9160,
				ConsistencyLevel = Apache.Cassandra.ConsistencyLevel.ONE,
				Timeout = TimeSpan.FromSeconds(10),
				IsFramed = true,
				Node = string.Empty,
				RetryCount = -1,
				Keyspace = "v1_readonly"
			};
	var configReadOnly = new CassandraConnectionConfig(builderReadOnly);

	using (var cassandraContextReadOnly = new CassandraContext(configReadOnly))
	{
		var loginInfo = new Dictionary<string,string>();
		loginInfo.Add("username", "testuser");
		loginInfo.Add("password", "12345678");
		
		cassandraContextReadOnly.Login(loginInfo);
		
		var result = from columnEntity in cassandraContextReadOnly.ColumnList
						where columnEntity.ColumnFamily == "Document"
						select columnEntity;
						
		result.First().Dump(1);
						
	}	
}