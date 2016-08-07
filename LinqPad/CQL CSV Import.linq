<Query Kind="Program">
  <Reference Relative="Cassandra.Data.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.dll</Reference>
  <Reference Relative="Cassandra.Data.Linq.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.Linq.dll</Reference>
  <Reference Relative="Cassandra.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.dll</Reference>
  <Reference Relative="LZ4.dll">&lt;MyDocuments&gt;\LINQPad Queries\LZ4.dll</Reference>
  <Reference>&lt;RuntimeDirectory&gt;\Microsoft.VisualBasic.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Microsoft.VisualBasic.FileIO</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.58.132";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	var csvPath = @"D:\Projects\DataStax\Projects\3M\ChangeHx Rows.csv";
	int nbrInserts = 0;
	
//	//Read CSV and insert into CQL table
//	using (var cluster = Cluster
//							.Builder()
//							.WithConnectionString(connectionString)
//							.WithoutRowSetBuffering()
//							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
//							.Build())
//	using (var session = cluster.Connect())
//	using (TextFieldParser parser = new TextFieldParser(csvPath))
//	{
//		var cqlInsertString = "insert into \"SetPageIssue\".\"ChangeHx\" (key, column1, value) values(textasblob(?), textasblob(?), textasblob(?));";
//	
//		session.Execute("CREATE KEYSPACE IF NOT EXISTS \"SetPageIssue\" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
//		session.Execute("CREATE TABLE IF NOT EXISTS  \"SetPageIssue\".\"ChangeHx\" (key blob, column1 blob, value blob, PRIMARY KEY(key, column1))");
//		
//		var cqlInsert = new SimpleStatement(cqlInsertString);
//		
//	    parser.Delimiters = new string[] { "," };
//	    while (true)
//	    {
//			string[] parts = parser.ReadFields();
//			if (parts == null)
//			{
//				break;
//			}
//			session.Execute(cqlInsert.Bind(parts[0], parts[1], parts[2]));
//			++nbrInserts;
//	    }
//	}
//	
//	Console.WriteLine("Nbr Inserts: {0}", nbrInserts);
//	
	var dateTimeList = new List<string>();
	var startDateTime = new DateTime(2014, 03, 25, 0, 0, 0);
	var endDateTime = new DateTime(2014, 03, 26, 0, 0, 0);
	int nRows = 0;
	
	for (; startDateTime < endDateTime; startDateTime = startDateTime.Add(new TimeSpan(1, 0, 0)))
	{
		dateTimeList.Add(string.Format("0|{0:yyyyMMddHHmm}", startDateTime));
	}
	
	string.Join(",",dateTimeList.Select (tl => string.Format("textasblob('{0}')", tl))).Dump();
	
//   var cqlString = "select blobastext(key), blobastext(column1), blobastext(value) from \"SetPageIssue\".\"ChangeHx\" where key = textasblob(?);"; //KS055298 
//
//	using (var cluster = Cluster
//							.Builder()
//							.WithConnectionString(connectionString)
//							.WithoutRowSetBuffering()
//							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
//							.Build())
//	using (var session = cluster.Connect())
//	{
//		var simpleStatement = new SimpleStatement(cqlString);
//		
//		foreach (var dateTimeString in dateTimeList)
//		{
//			var rowset = session.Execute(simpleStatement.Bind(dateTimeString));
//			
//			nRows += rowset.Count();
//		}
//	}
//	
//	Console.WriteLine("Nbr of Records Read without SetPageSize: {0}", nRows);
//	
//	nRows = 0;
//	
//	using (var cluster = Cluster
//							.Builder()
//							.WithConnectionString(connectionString)
//							.WithoutRowSetBuffering()
//							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
//							.Build())
//	using (var session = cluster.Connect())
//	{
//		var simpleStatement = new SimpleStatement(cqlString);
//		simpleStatement.SetPageSize(500);
//		
//		foreach (var dateTimeString in dateTimeList)
//		{
//			var rowset = session.Execute(simpleStatement.Bind(dateTimeString));
//			
//			nRows += rowset.Count();
//		}
//	}
//	
//	Console.WriteLine("Nbr of Records Read WITH SetPageSize: {0}", nRows);
//	
}



// Define other methods and classes here
