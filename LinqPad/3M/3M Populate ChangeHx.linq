<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data.Linq\bin\Release\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\Cassandra.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Shared.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Singleton.dll</Reference>
  <Reference Relative="EPPlus.dll">&lt;MyDocuments&gt;\LINQPad Queries\EPPlus.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\LZ4.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>Common</Namespace>
  <Namespace>Common.Patterns</Namespace>
  <Namespace>OfficeOpenXml</Namespace>
</Query>

Stopwatch stopWatch;
Boolean makeChanges = true; // Change to true to actually update entity  (default to read only)
bool verbose = false;

void Main()
{
	string keySpaceNameDefault = "KSV1_VOLATILE"; //"KS00000X"; //Change Keysace Name or null to indicate all key spaces in the cluster.
	var connectionString = "Contact Points=192.168.58.131"; //Change IP Address or a list of ip addresses seperated by a comma
	var dataCenterName = ""; //Change to DC Name

	var cqlTableName = "Document";
	int cqlQueryPagingSize = 1000; 
	ConsistencyLevel? queryConsistencyLevel = ConsistencyLevel.Quorum;
	
	using (var cluster = Cluster
							.Builder()
							.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.Build())
	using (var session = cluster.Connect())
	{
		//var results = new List<myrow>();
			
		foreach (var keySpaceName in string.IsNullOrEmpty(keySpaceNameDefault) ? GetKeySpaces(session,cqlTableName) : (IEnumerable<string>) new string[] {keySpaceNameDefault} )
		{
			stopWatch = new Stopwatch();
   			stopWatch.Start();
			
			Console.WriteLine("================================== {0} - {1} =====================================", keySpaceName, (makeChanges?"WRITE":"READONLY"));
	
			foreach (var entity in new List<string>() {"Person","Encounter","Document"})
			{
				DoWork(session, keySpaceName, entity, cqlQueryPagingSize, queryConsistencyLevel);
			}		
		}
		
		stopWatch.Stop();
		// Get the elapsed time as a TimeSpan value.
		TimeSpan ts = stopWatch.Elapsed;
	
		// Format and display the TimeSpan value.
		string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
			ts.Hours, ts.Minutes, ts.Seconds,
			ts.Milliseconds / 10);
		Console.WriteLine("RunTime " + elapsedTime);
	}		
}

long ToDWBucketFormat(DateTime date)
{   
	date = date.AddMinutes(-date.Minute % 60); // Round down to nearest 10 min  00,10,20,30,40,50
	return Convert.ToInt64(date.ToString("yyyyMMddHHmm"));
}
               
void DoWork(ISession session, string keySpaceName, string tableName, int queryPageSize = int.MaxValue, ConsistencyLevel? queryConsistencyLevel = null)
{
	var cqlString = string.Format("select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"{1}\";", keySpaceName, tableName);
	var cqlStringText = string.Format("select key, column1, value from \"{0}\".\"{1}\";", keySpaceName, tableName);
    var onlyTheseColumns = new string[] {"EprsVersion", "DateStamp"};
    var cnt=0;
	var entityType = (new List<string>() {"Person","Encounter","Document"}).FindIndex(i=> i==tableName);
	try
	{		
		var cqlStatement = new SimpleStatement(entityType == 1  ? cqlStringText : cqlString)
								.SetPageSize(queryPageSize)
								.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
								.SetConsistencyLevel(queryConsistencyLevel);
		Console.WriteLine("Processing {0}...", tableName);
		using(var rowSet = session.Execute(cqlStatement))	
		{
			string partitionKey = null;
			var dict = new Dictionary<string, string>();
			
			foreach(var key in onlyTheseColumns) dict.Add(key, "");
			
			foreach (var row in rowSet.GetRows())
			{
			    if (partitionKey==null) partitionKey = row.GetValue<string>(0); // Prime
				if (partitionKey != row.GetValue<string>(0))	
				{
				
					if (dict["DateStamp"]=="") dict["DateStamp"]=DateTime.Now.ToString();				
					var rowkey=string.Format("{0}|{1}", entityType, ToDWBucketFormat(Convert.ToDateTime(dict["DateStamp"])));
					var value = dict["EprsVersion"];
					
					WriteColumn(session, keySpaceName, "ChangeHx", rowkey, string.Format("{0}:{1}", partitionKey, value), "", queryConsistencyLevel);	
					
					if ((++cnt % 10000)==0) 
					{
						TimeSpan ts1 = stopWatch.Elapsed;
						string elapsedTime1 = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts1.Hours, ts1.Minutes, ts1.Seconds, ts1.Milliseconds / 10);
						Console.WriteLine("Processed: {0} records. {1}", cnt, elapsedTime1);
					}
					foreach (var key in dict.Keys.ToList()) dict[key]="";
					partitionKey = row.GetValue<string>(0);
				}
				var columnName = row.GetValue<string>(1);
				if(onlyTheseColumns != null && !onlyTheseColumns.Contains(columnName))
				{
					continue;
				}
				dict[columnName] = row.GetValue<string>(2);
			}
			
			TimeSpan ts = stopWatch.Elapsed;
			string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
			Console.WriteLine("Processed: {0} {1} records {2}", cnt, tableName, elapsedTime);
			
		}		
	}
	catch(System.Exception e)
	{
		e.Dump(1);
		Console.WriteLine("Skipping CQL \"{0}\" due to Exception \"{1}\" ({2})", cqlString, e.GetType().Name, e.Message);
	}
}
void WriteColumn(ISession session, string keySpacename, string table, string key, string column , string value, ConsistencyLevel? queryConsistencyLevel = null, bool retry=true)
{
	var cqlSelectString = string.Format("insert into \"{0}\".\"{1}\" (key, column1, value) values ( textasblob(\'{2}\'), textasblob(\'{3}\'), textasblob(\'{4}\'));", keySpacename, table, key, column, value);
	var cqlStatement = new SimpleStatement(cqlSelectString)
								.SetPageSize(100)
								.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
								.SetConsistencyLevel(queryConsistencyLevel);
	
	try 
	{
		if (verbose) Console.WriteLine("\tWriteColumn: {0,-20} {1,-30} {2,-15} {3} len={4}", table, key, column, (value.Length>50? value.Substring(0,50):value), value.Length);
		if (!makeChanges) return;
		
		using (var rowSet = session.Execute(cqlStatement)) {}
	}
	catch (InvalidQueryException e)
    {
		if (e.Message == "unconfigured columnfamily ChangeHx" && retry)
		{
			var cql= string.Format("CREATE TABLE  \"{0}\".\"{1}\" (key blob, column1 blob, value blob, PRIMARY KEY (key, column1)) WITH COMPACT STORAGE", keySpacename, "ChangeHx");
			try {session.Execute(cql);}
			catch (System.Exception e1) {
				Console.WriteLine("Unable to create columnfamily: {0} \n{1}", e1.Message, cql);
				throw;
			}
			WriteColumn(session, keySpacename, table, key, column, value, queryConsistencyLevel, false);			
			return;
		}
		Console.WriteLine("WriteColumn: Wrong Message: {0}", e.Message);
        throw;
	}
	catch(System.Exception e) {
		e.Dump(1);
		Console.WriteLine("Skipping CQL \"{0}\" due to Exception \"{1}\" ({2})", cqlSelectString, e.GetType().Name, e.Message);
		throw;
	}
}


IEnumerable<string> GetKeySpaces(ISession session, string mustHaveTable)
{
	var keySpaces = new List<string>();
	
	foreach(var keySpaceName in session.Cluster.Metadata.GetKeyspaces())
	{
		if(mustHaveTable == null || session.Cluster.Metadata.GetTables(keySpaceName).Contains(mustHaveTable))
		{
			if (keySpaceName.StartsWith("KS")) 
			  keySpaces.Add(keySpaceName);
		}
	}
	
	return keySpaces;
}

// Define other methods and classes here