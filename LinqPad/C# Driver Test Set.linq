<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.2\src\Cassandra.Data.Linq\bin\Release\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.2\src\Cassandra.DSE\bin\Release\Cassandra.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.2\src\Cassandra.DSE\bin\Release\Cassandra.DSE.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.2\src\Cassandra.DSE\bin\Release\Crc32C.NET.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.2\src\Cassandra.DSE\bin\Release\LZ4.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 2.1.2\src\Cassandra.DSE\bin\Release\Snappy.NET.dll</Reference>
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
		var resultset = sessionV1.Execute("select * from v2_volatile.encounter where \"Id\" = '1d8b6836-0fa8-4648-bd56-e35efafaf264'");
		var row = resultset.GetRows().First();
		var firstCollection = row.GetValue<List<string>>("Documents");

		firstCollection.Dump();
		
		var prepare = sessionV1.Prepare("update v2_volatile.encounter set \"Documents\" = ? where \"Id\" = ? and \"EprsVersionNumber\" = ?");
		//var prepare = sessionV1.Prepare("update v2_volatile.encounter set \"Documents\" = \"Documents\" + {?} where \"Id\" = ? and \"EprsVersionNumber\" = ?");
		
		//sessionV1.Execute(prepare.Bind("test" + DateTime.Now.ToString(), row.GetValue<string>("Id"), row.GetValue<int>("EprsVersionNumber")));
		
		var addValue = "test" + DateTime.Now.ToString();
		
		if(!firstCollection.Contains(addValue))
		{
			firstCollection.Add(addValue);
			sessionV1.Execute(prepare.Bind(firstCollection, row.GetValue<string>("Id"), row.GetValue<int>("EprsVersionNumber")));
		}
							
		
		sessionV1.Execute("select * from v2_volatile.encounter where \"Id\" = '1d8b6836-0fa8-4648-bd56-e35efafaf264'")
			.GetRows().First().GetValue<List<string>>("Documents").Dump();
	}	
}