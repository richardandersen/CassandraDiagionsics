<Query Kind="Program">
  <Reference Relative="Apache.Cassandra.dll">&lt;MyDocuments&gt;\LINQPad Queries\Apache.Cassandra.dll</Reference>
  <Reference Relative="Cassandra.Data.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.dll</Reference>
  <Reference Relative="Cassandra.Data.Linq.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.Linq.dll</Reference>
  <Reference Relative="Cassandra.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.dll</Reference>
  <Reference Relative="Cassandraemon.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandraemon.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Path.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Shared.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Singleton.dll</Reference>
  <Reference Relative="Crc32C.NET.dll">&lt;MyDocuments&gt;\LINQPad Queries\Crc32C.NET.dll</Reference>
  <Reference Relative="EncounterDeleteDefineIndexes.dll">&lt;MyDocuments&gt;\LINQPad Queries\EncounterDeleteDefineIndexes.dll</Reference>
  <Reference Relative="EPPlus.dll">&lt;MyDocuments&gt;\LINQPad Queries\EPPlus.dll</Reference>
  <Reference Relative="log4net.dll">&lt;MyDocuments&gt;\LINQPad Queries\log4net.dll</Reference>
  <Reference Relative="LZ4.dll">&lt;MyDocuments&gt;\LINQPad Queries\LZ4.dll</Reference>
  <Reference Relative="Snappy.NET.dll">&lt;MyDocuments&gt;\LINQPad Queries\Snappy.NET.dll</Reference>
  <Reference>&lt;RuntimeDirectory&gt;\System.Threading.Tasks.dll</Reference>
  <Reference Relative="Thrift.dll">&lt;MyDocuments&gt;\LINQPad Queries\Thrift.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Common</Namespace>
  <Namespace>Common.Patterns</Namespace>
  <Namespace>OfficeOpenXml</Namespace>
  <Namespace>System.Threading</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>

// Create a report to detect duplicate visit numbers
// Our definition of duplicate is that the same visitId/Fac pair is used on more than
// one person.
//
// Symptom : We belive that EPPR v1 would allow the same VisitId/FacId pair to be used more
//           than once.  We need a report to scan the database for this condition and let us
//           know if the problem exists, and how many instances.
//
// Solution:
// Query the Encounter table, build a dictionary keyed by VisitId whose value is a list of 
// PersonId's w/ that VisitId.  Then we can filter out records with a list lenght > 1.

Stopwatch stopWatch;
bool verbose = false;

ConsistencyLevel READ_CONSISTENCY = ConsistencyLevel.One;


	
void Main()
{
	string keySpaceName = "KSV1_VOLATILE";//"KS055298";//"KS00f334";//"KS00g898";	
	var connectionString = "Contact Points=192.168.117.132"; //Change IP Address or a list of ip addresses seperated by a comma
	//var connectionString = "Contact Points=169.10.60.206";//127.0.0.1"; //Change IP Address or a list of ip addresses seperated by a comma
	var excelTargetFilePath = new Common.File.FilePathAbsolute(string.Format(@"[DesktopDirectory]\3MLinqPadReports\MsnFacilityVisit {0:yyyyMMdd}.xlsx.",
																									DateTime.Now)); //xlsx;
	var excelWorkSheetName = keySpaceName;
	
	excelTargetFilePath.ParentDirectoryPath.Create();
	
	using (var excelPkg = new ExcelPackage(excelTargetFilePath.FileInfo()))
	using (var cluster = Cluster
							.Builder()
							//.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
							.WithCompression(CompressionType.LZ4)
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.Build())
	using (var session = cluster.Connect())
	{
			stopWatch = new Stopwatch();
   			stopWatch.Start();
	
			// The Encounter table has secondary indexes, we need to remove them to see all of the columns in CQL3
			// this using clause is a little magic we got from DataStax consulting to remove them if present and restores them
			if (verbose) Console.WriteLine("Before: "+dispElapsedTime(stopWatch.Elapsed));
			using (var encounterdeletedefineInstance = EncounterDeleteDefineIndexes.Factory.CrateInstance(connectionString, "", "", keySpaceName)
														//.WithLogger(Logger) //Log4Net, not required
															.RestoreOnDispose() //Restores the index on display. In this case on exist of using
															.PerformAction(true, (instance) => instance.ExecuteDelete())) //Execute the Delete Index as part of the using
			{
			if (verbose) Console.WriteLine("After: "+dispElapsedTime(stopWatch.Elapsed));
			
			Dictionary<string, List<string>> idxMrnByPerson = null;
			Dictionary<string, List<string>> idxPersonByEncounter = null;
			//Dictionary<string, object> allPerson = null;			
			Dictionary<string, object> allEncounter = null;
			
			Parallel.Invoke(
			() => { allEncounter = LoadAllTableData(session, keySpaceName, "Encounter","DateStamp,DischargeDate,EprsVersion,LastUpdate,PatientType,Status,FacilityID,VisitNumber,PersonID");},			
			//() => { allPerson = LoadAllTableData(session, keySpaceName, "Person", "MRN,FacilityID");},
			() => { idxMrnByPerson = LoadIndex(session, keySpaceName, "idxMrnPerson");},	
			() => { idxPersonByEncounter = LoadIndex(session, keySpaceName, "idxPersonIdEncounter");}	
			);
			
			var DupMrnDifferentFacilitySameVisit = idxMrnByPerson.SelectMany(s => s.Value.Select(v => { var keys = s.Key.Split('|'); return new { Mrn = keys[0], Facility=keys[1], PersonId=v}; }))
															.Join(idxPersonByEncounter.SelectMany(s => s.Value.Select(v => new { PersonId=s.Key, EncounterId=v})),
																	outerItem => outerItem.PersonId,
																	innerItem => innerItem.PersonId,
																	(innerItem, outerItem) => new { innerItem.Mrn, innerItem.Facility, innerItem.PersonId, outerItem.EncounterId })
															.Join(allEncounter,
																	outerItem => outerItem.EncounterId,
																	(encounterId, outerItem, encounterRecord) => 
																		{
																			((Dictionary<string,string>)encounterRecord)["EncounterId"] = encounterId;
																			((Dictionary<string,string>)encounterRecord)["Mrn"] = outerItem.Mrn;
																			((Dictionary<string,string>)encounterRecord)["MrnFacility"] = outerItem.Facility;
																			return new { outerItem.Mrn, 
																							outerItem.Facility, 
																							VisitNumber=TryGetValue((Dictionary<string,string>)encounterRecord,"VisitNumber").Item2,
																							outerItem.PersonId,
																							outerItem.EncounterId,
																							EncounterRecord=(Dictionary<string,string>)encounterRecord };
																		})
															.DuplicatesWithRecord(groupSelector => groupSelector.Mrn + "|" + groupSelector.VisitNumber)
															.SelectMany (mrnValue => mrnValue.Value)
															.DuplicatesRemoved(groupSelector => groupSelector.Mrn + "|" + groupSelector.Facility);
						
			
			DupMrnDifferentFacilitySameVisit.Dump(string.Format("Duplicate Mrn, Different Facility, Same Visit Count {0}", DupMrnDifferentFacilitySameVisit.Count()));
			
			if(excelPkg != null && 1string.IsNullOrEmpty(excelWorkSheetName))
			{
				var workBook = excelPkg.Workbook.Worksheets[excelWorkSheetName];
				if(workBook == null)
				{
					workBook = excelPkg.Workbook.Worksheets.Add(excelWorkSheetName);
				}
				
				var loadRange = workBook.Cells["A1"].LoadFromCollection(DupMrnDifferentFacilitySameVisit
																			.Select(dmdfsv => new
																								{								
																									Mrn=TryGetValue(dmdfsv.EncounterRecord, "Mrn").Item2,
																									FacilityID=TryGetValue(dmdfsv.EncounterRecord, "FacilityID").Item2,
																									VisitNumber=TryGetValue(dmdfsv.EncounterRecord, "VisitNumber").Item2,
																									PersonID=TryGetValue(dmdfsv.EncounterRecord, "PersonID").Item2,
																									EncounterId=TryGetValue(dmdfsv.EncounterRecord, "EncounterId").Item2,
																									DateStamp=TryGetValue(dmdfsv.EncounterRecord, "DateStamp").Item2,
																									DischargeDate=TryGetValue(dmdfsv.EncounterRecord, "DischargeDate").Item2,
																									EprsVersion=TryGetValue(dmdfsv.EncounterRecord, "EprsVersion").Item2,																								
																									LastUpdate=TryGetValue(dmdfsv.EncounterRecord, "LastUpdate").Item2,
																									PatientType=TryGetValue(dmdfsv.EncounterRecord, "PatientType").Item2,																								
																									Status=TryGetValue(dmdfsv.EncounterRecord, "Status").Item2,																									
																									MrnFacility=TryGetValue(dmdfsv.EncounterRecord, "MrnFacility").Item2
																								}), true);
			
				Console.WriteLine("Number of Cells loaded into WorkBook \"{0}\" is {1:###,###,##0}", excelWorkSheetName, loadRange.LongCount());
				
				excelPkg.Save();
			}
			}
			stopWatch.Stop();
			Console.WriteLine("RunTime " + dispElapsedTime(stopWatch.Elapsed));		
	}
}

string dispElapsedTime(TimeSpan ts) { return String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);}

Dictionary<string, List<Dictionary<string,string>>> LoadAllTableDataToTuple(ISession session, string keySpaceName, string table, string colList="")
{	
	var stopWatch = new Stopwatch();
	stopWatch.Start();
	
	var resultSet = new Dictionary<string, List<Dictionary<string,string>>>();

	//Console.WriteLine("LoadAllTableData {0}...", table);
	var cqlSelectString = "";
	if (!string.IsNullOrEmpty(colList))
	{ 
		var onlyTheseColumns = colList.Split(',');
		cqlSelectString = string.Format("select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"{1}\" where column1 in ({2}) ALLOW FILTERING;", 
											keySpaceName, 
											table, 
											string.Join(",", onlyTheseColumns.Select(i => string.Format("textasblob('{0}')", i))));	
	}
	else
	{
		cqlSelectString = string.Format("select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"{1}\";", keySpaceName, table);
	}
	
	
	try
	{		
		var cnt=0;
		if (verbose) cqlSelectString.Dump();
		var cqlStatement = new SimpleStatement(cqlSelectString)
									.SetPageSize(1250)
									.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
									.SetConsistencyLevel(READ_CONSISTENCY);		
		using(var rowSet = session.Execute(cqlStatement))	 
		{
			string partitionKey=null;
			var dict = new Dictionary<string, string>();
			
			foreach (var row in rowSet.GetRows()) 
			{
				if (partitionKey==null) partitionKey = row.GetValue<string>(0); // Prime
				if (partitionKey != row.GetValue<string>(0))	
				{						
					dict["EncounterId"]=partitionKey;
					
					var pair = ExtractTuple(dict);
					pair.Dump();
					if (resultSet.ContainsKey(pair.Key))
						resultSet[pair.Key].Add(pair.Value);
					else
						resultSet.Add(pair.Key, new List<Dictionary<string,string>>(){pair.Value});					
					
					if ((++cnt % 10000)==0) 
					{
						if (verbose) Console.WriteLine("\tLoaded: {0} {2} records.  {1}", cnt, dispElapsedTime(stopWatch.Elapsed), table);
					}
					
					// Reset						
					partitionKey = row.GetValue<string>(0);
					dict = new Dictionary<string, string>();
				}
			    var column = row.GetValue<string>(1);
				var value = row.GetValue<string>(2);
				dict[column] = ((column=="XmlRecord")?string.Format("{0}|{1}", value.Length, value.GetHashCode()):value);
			}
			
			if (partitionKey!=null)
			{	dict["EncounterId"]=partitionKey;
				var pair = ExtractTuple(dict);
				if (resultSet.ContainsKey(pair.Key))
					resultSet[pair.Key].Add(pair.Value);
				else
					resultSet.Add(pair.Key, new List<Dictionary<string,string>>(){pair.Value});	
								
				cnt++;
			}	
		}
		Console.WriteLine("\tLoaded: {0} {2} records.  {1}", cnt, dispElapsedTime(stopWatch.Elapsed), table);
		
	}
	catch(System.Exception e)
	{
		e.Dump(1);
		Console.WriteLine("LoadAllTableData: Skipping CQL \"{0}\" due to Exception \"{1}\" ({2})", cqlSelectString, e.GetType().Name, e.Message);
		throw e;
	}
	return resultSet;
}

Tuple<bool,V> TryGetValue<K,V>(IDictionary<K,V> dictionary, K keyValue)
{
	V temp = default(V);
	
	if(dictionary.TryGetValue(keyValue, out temp))
	{
		return new Tuple<bool,V>(true, temp);
	}
	
	return new Tuple<bool,V>(false,temp);
}

KeyValuePair<string, Dictionary<string,string>> ExtractTuple(Dictionary<string,string> dict)
{
	string tmp=string.Empty;
	var VisitNumber = dict.TryGetValue("VisitNumber", out tmp) ?  tmp : null;
	tmp=string.Empty;
	var FacilityId = dict.TryGetValue("FacilityID", out tmp) ?  tmp : null;
	
	var key = string.Format("{0}|{1}", VisitNumber, FacilityId);
	var pair = new KeyValuePair<string, Dictionary<string,string>>(key, dict);
	return pair;
}

// Write custom extension methods here. They will be available to all queries.
//
static public IEnumerable<string> GetKeySpaces(ISession session, string mustHaveTable)
{
	var keySpaces = new List<string>();
	
	foreach(var keySpaceName in session.Cluster.Metadata.GetKeyspaces())
	{
		if( keySpaceName.StartsWith("KS") && (mustHaveTable == null || session.Cluster.Metadata.GetTables(keySpaceName).Contains(mustHaveTable)))
		{ 
			Console.Write(keySpaceName + ", ");			
			keySpaces.Add(keySpaceName);
		}
	}
	
	return keySpaces;
}

HashSet<string> LoadAllTableKeys(ISession session, string keySpaceName, string table)
{	
	var stopWatch = new Stopwatch();
	stopWatch.Start();
	
	var resultSet = new HashSet<string>();

	//Console.WriteLine("LoadAllTableKeys {0}...", table);
	var cqlSelectString = string.Format("select blobastext(key) from \"{0}\".\"{1}\";", keySpaceName, table);
	try
	{		
		var cnt=0;
		var cqlStatement = new SimpleStatement(cqlSelectString)
									.SetPageSize(15000)
									.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
									.SetConsistencyLevel(ConsistencyLevel.One);		
		using(var rowSet = session.Execute(cqlStatement))	 
		{
			string partitionKey=null;
			
			foreach (var row in rowSet.GetRows()) 
			{
				if (partitionKey==null) partitionKey = row.GetValue<string>(0); // Prime
				if (partitionKey != row.GetValue<string>(0))	
				{
					resultSet.Add(partitionKey);				  	
					// Reset						
					partitionKey = row.GetValue<string>(0);
					
					if ((++cnt % 10000)==0) 
					{
						if (verbose) Console.WriteLine("\tLoaded: {0} {2} keys.  {1}", cnt, dispElapsedTime(stopWatch.Elapsed), table);
					}
				}				
			}
			resultSet.Add(partitionKey);
			cnt++;
		}
		Console.WriteLine("\tLoaded: {0} {2} keys.  {1}", cnt, dispElapsedTime(stopWatch.Elapsed), table);
		
	}
	catch(System.Exception e)
	{
		e.Dump(1);
		Console.WriteLine("LoadAllTableKeys: Skipping CQL \"{0}\" due to Exception \"{1}\" ({2})", cqlSelectString, e.GetType().Name, e.Message);
		throw e;
	}
	return resultSet;
}

static public string LoadColumn(ISession session, string keySpacename, string columnFamily, string partitionKey, string column)
	{
		var dict = new Dictionary<string, string>();			
		var cqlSelectString = string.Format("select blobastext(value) from \"{0}\".\"{1}\" where \"key\" = textasblob('{2}') and \"column1\" = textasblob('{3}');", keySpacename, columnFamily, partitionKey, column);
		var result = "";
		try
		{		
			var cqlStatement = new SimpleStatement(cqlSelectString)
										.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
										.SetConsistencyLevel(ConsistencyLevel.Quorum);
			
			using(var rowSet = session.Execute(cqlStatement))	 
			{
				foreach (var row in rowSet.GetRows()) 
				{
					result= row.GetValue<string>(0);	
				}
			}
		}
		catch(System.Exception e)
		{
			e.Dump(1);
			Console.WriteLine("LoadRow: Skipping CQL \"{0}\" due to Exception \"{1}\" ({2})", cqlSelectString, e.GetType().Name, e.Message);
		}
		return result;		
	}
Dictionary<string, object> LoadAllTableData(ISession session, string keySpaceName, string table, string colList="")
{	
	var stopWatch = new Stopwatch();
	stopWatch.Start();
	
	var resultSet = new Dictionary<string, object>();

	//Console.WriteLine("LoadAllTableData {0}...", table);
	var cqlSelectString = "";
	if (!string.IsNullOrEmpty(colList))
	{ 
		var onlyTheseColumns = colList.Split(',');
		cqlSelectString = string.Format("select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"{1}\" where column1 in ({2}) ALLOW FILTERING;", 
											keySpaceName, 
											table, 
											string.Join(",", onlyTheseColumns.Select(i => string.Format("textasblob('{0}')", i))));	
	}
	else
	{
		cqlSelectString = string.Format("select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"{1}\";", keySpaceName, table);
	}
	
	
	try
	{		
		var cnt=0;
		var cqlStatement = new SimpleStatement(cqlSelectString)
									.SetPageSize(1250)
									.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
									.SetConsistencyLevel(READ_CONSISTENCY);		
		using(var rowSet = session.Execute(cqlStatement))	 
		{
			string partitionKey=null;
			var dict = new Dictionary<string, string>();
			
			foreach (var row in rowSet.GetRows()) 
			{
				if (partitionKey==null) partitionKey = row.GetValue<string>(0); // Prime
				if (partitionKey != row.GetValue<string>(0))	
				{
					resultSet.Add(partitionKey, dict);				  	
					// Reset						
					partitionKey = row.GetValue<string>(0);
					dict = new Dictionary<string, string>();
					
					if ((++cnt % 10000)==0) 
					{
						if (verbose) Console.WriteLine("\tLoaded: {0} {2} records.  {1}", cnt, dispElapsedTime(stopWatch.Elapsed), table);
					}
				}
			    var column = row.GetValue<string>(1);
				var value = row.GetValue<string>(2);
				dict[column] = ((column=="XmlRecord")?string.Format("{0}|{1}", value.Length, value.GetHashCode()):value);
			}
			if (partitionKey!=null) {
				resultSet.Add(partitionKey, dict);
				cnt++;
			}
		}
		Console.WriteLine("\tLoaded: {0} {2} records.  {1}", cnt, dispElapsedTime(stopWatch.Elapsed), table);
		
	}
	catch(System.Exception e)
	{
		e.Dump(1);
		Console.WriteLine("LoadAllTableData: Skipping CQL \"{0}\" due to Exception \"{1}\" ({2})", cqlSelectString, e.GetType().Name, e.Message);
		throw e;
	}
	return resultSet;
}
	
Dictionary<string, List<string>> LoadIndex(ISession session, string keySpaceName, string table)
{
	var stopWatch = new Stopwatch();
	stopWatch.Start();
 	var resultSet= new Dictionary<string, List<string>>();
   	//Console.WriteLine("LoadIndex {0}...", table);
   	var cqlSelectString = string.Format("select blobastext(key), blobastext(value) from \"{0}\".\"{1}\";", keySpaceName, table);
	try
	{		
		var cnt=0;
		var cqlStatement = new SimpleStatement(cqlSelectString)
									.SetPageSize(10000)
									.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
									.SetConsistencyLevel(READ_CONSISTENCY);		
		using(var rowSet = session.Execute(cqlStatement))	 
		{
			foreach (var row in rowSet.GetRows()) 
			{
				List<string> list=null;
				if (resultSet.TryGetValue(row.GetValue<string>(0), out list))
					list.Add(FixUpGUID(row.GetValue<string>(1)));
				else
					resultSet.Add(row.GetValue<string>(0), new List<string>(){ FixUpGUID(row.GetValue<string>(1))} );
					
				if ((++cnt % 10000)==0) 
				{
					TimeSpan ts1 = stopWatch.Elapsed;
					string elapsedTime1 = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts1.Hours, ts1.Minutes, ts1.Seconds, ts1.Milliseconds / 10);
					if (verbose) Console.WriteLine("\tLoaded: {0} {2} records.  {1}", cnt, elapsedTime1, table);
				}
			}			
		}
		Console.WriteLine("\tLoaded: {0} {2} records.  {1}", cnt, dispElapsedTime(stopWatch.Elapsed), table);
		
	}
	catch(System.Exception e)
	{
		e.Dump(1);
		Console.WriteLine("LoadIndex: Skipping CQL \"{0}\" due to Exception \"{1}\" ({2})", cqlSelectString, e.GetType().Name, e.Message);
		throw e;
	}
   return resultSet;
}

string FixUpGUID(string guid)
{
	if(guid.Length == 36)
	{
		return guid;
	}
	
	return string.Format("{0}-{1}-{2}-{3}-{4}",
							guid.Substring(0,8),
							guid.Substring(8,4),
							guid.Substring(12,4),
							guid.Substring(16,4),
							guid.Substring(20));
}
// Define other methods and classes here