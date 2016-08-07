<Query Kind="Program">
  <Reference Relative="Cassandra.Data.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.dll</Reference>
  <Reference Relative="Cassandra.Data.Linq.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.Linq.dll</Reference>
  <Reference Relative="Cassandra.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.dll</Reference>
  <Reference Relative="Crc32C.NET.dll">&lt;MyDocuments&gt;\LINQPad Queries\Crc32C.NET.dll</Reference>
  <Reference Relative="EPPlus.dll">&lt;MyDocuments&gt;\LINQPad Queries\EPPlus.dll</Reference>
  <Reference Relative="LZ4.dll">&lt;MyDocuments&gt;\LINQPad Queries\LZ4.dll</Reference>
  <Reference Relative="Snappy.NET.dll">&lt;MyDocuments&gt;\LINQPad Queries\Snappy.NET.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
</Query>

void Main()
{
	var connectionStringV1 = "Contact Points=192.168.117.131";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	
	using (var clusterV1 = Cluster
							.Builder()
							.WithConnectionString(connectionStringV1)
							.WithoutRowSetBuffering()
							.Build())
	using (var sessionV1 = clusterV1.Connect())
	{
		var x1 = sessionV1.Prepare("select * from v2_volatile.person where \"Id\" in ? limit ?");
		
		var x2 = from i in new List<string>{"5f3c89cd-0c8b-4d80-b24d-2a660932b489", "a9010e5a-905d-47b7-8592-9cc115b15391"}
					select i;
					
		sessionV1.Execute(x1.Bind(x2, 1)).Dump();
	}	
}