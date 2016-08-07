<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data.Linq\bin\Release\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\Cassandra.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Shared.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Singleton.dll</Reference>
  <Reference>C:\bin\EPPlus\EPPlus.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>Common</Namespace>
  <Namespace>Common.Patterns</Namespace>
</Query>

//Define a class that contains columns that match my CQL Select statement
[Table("emp")] 
[AllowFiltering]
class emp
{
  [PartitionKey(1)]
  public Int32 empid { get; set; }
  [ClusteringKey(0)]
  public string emp_first { get; set; }
  public string emp_last { get; set; }
  public string emp_dept { get; set; }
}

void Main()
{
	var keySpaceName = "LinqTest"; //Change Keysace Name
	var connectionString = "Contact Points=192.168.190.134"; //Change IP Address
	//var dataCenterName = "the name of the local datacenter (as known by Cassandra)."; //Change to DC Name
	
	//CQL parameters
	//ConsistencyLevel? queryConsistencyLevel = ConsistencyLevel.Quorum;
	
	using (var cluster = Cluster
							.Builder()
							//.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.Build())
	using (var session = cluster.Connect())
	{
		session.CreateKeyspaceIfNotExists(keySpaceName);
		session.ChangeKeyspace(keySpaceName);
		
		 var table = session.GetTable<emp>();
         
		 table.CreateIfNotExists(); //Create Table
		 
		 var employs = new List<emp>
                {
                    new emp
                    {
                        empid = 10,
                        emp_first = "Hi",
                        emp_last = "There",
                        emp_dept ="Not for long"  // delete this guy
                    },
                    new emp
                    {
                        empid = 20,
                        emp_first = "Me",
                        emp_last = "Again",
                        emp_dept ="Here to stay"
                    },
                };
 

		Batch batch = session.CreateBatch();
 
		batch.Append(from prolitariate in employs select table.Insert(prolitariate));
 
       	batch.Execute();

		(from row in table where row.empid == 10 select new emp(){emp_first="new dept"}).Update().Execute();
		
	}		
	
}

