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
  <Namespace>OfficeOpenXml</Namespace>
</Query>

void Main()
{
	string keySpaceNameDefault = "KSV1_VOLATILE"; //Change Keysace Name or null to indicate all key spaces in the cluster.
	var connectionString = "Contact Points=192.168.190.133"; //Change IP Address or a list of ip addresses seperated by a comma
	var dataCenterName = "the name of the local datacenter (as known by Cassandra)."; //Change to DC Name
	
	//CQL parameters
	//var cqlString = "select key, column1, value from \"{0}\".\"Encounter\" where column1 in ('FacilityID', 'VisitNumber', 'PersonID', 'DateStamp') allow filtering;";
	//var cqlString = "select key, column1, value from \"{0}\".\"Encounter\";";
	//var cqlString = "select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"Encounter\" where column1 in (textasblob('FacilityID'), textasblob('VisitNumber'), textasblob('PersonID'), textasblob('DateStamp')) allow filtering;";
	var cqlString = "select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"Encounter\";";
	var cqlTableName = "Encounter";
	var dataTableName = "V1Encounter";
	int cqlQueryPagingSize = 1000; 
	ConsistencyLevel? queryConsistencyLevel = ConsistencyLevel.Quorum;
	
	//Excel Parameters
	var excelWorkSheetName = "DuplicateVisitFacility";
	var excelDirectory = Path.GetDirectoryName(Util.CurrentQueryPath);
	
	using (var cluster = Cluster
							.Builder()
							.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.Build())
	using (var session = cluster.Connect())
	{
		foreach (var keySpaceName in string.IsNullOrEmpty(keySpaceNameDefault) ? GetKeySpaces(session,cqlTableName) : (IEnumerable<string>) new string[] {keySpaceNameDefault} )
		{
	
			Console.WriteLine("==================================");
			
			var cqlSelectString = string.Format(cqlString, keySpaceName);
			var excelFilePath = string.Format(@"{0}\{1} Duplicates.xlsx", excelDirectory, keySpaceName);	
			var excelFile = new FileInfo(excelFilePath);
	
			var recordVisitNumberFacilityIds = ReadThriftRowIntoRecordsSaveToDataTable(session,
																						cqlSelectString,
																						dataTableName,
																						false,
																						cqlQueryPagingSize,
																						queryConsistencyLevel,
																						new string[] {"VisitNumber",
																										"FacilityID",
																										"PersonID",
																										"DateStamp"}).AsEnumerable()
													.Select (row => new { VisitNumber = row.Field<string>("VisitNumber"), 
																			FacilityId = row.Field<string>("FacilityID"),
																			PersionId = row.Field<string>("PersonID"),
																			EncounerId = row.Field<string>("PartitionKey"),
																			DateStamp = row.Field<string>("DateStamp")});
			
			if(recordVisitNumberFacilityIds.Count() > 0)
			{
				var duplicateValues = recordVisitNumberFacilityIds.DuplicatesWithRecord(record => new { VisitNumber = record.VisitNumber, FacilityId = record.FacilityId });
			
				Console.WriteLine("Number of Duplicate {0:###,###,##0}", duplicateValues.Count());
				
				//duplicateValues.Dump();
				
				using (var excelPkg = new ExcelPackage(excelFile))
				{
					var workBook = excelPkg.Workbook.Worksheets[excelWorkSheetName];
					if(workBook == null)
					{
						workBook = excelPkg.Workbook.Worksheets.Add(excelWorkSheetName);
					}
					
					var loadRange = workBook.Cells["A1"].LoadFromCollection(duplicateValues.SelectMany(item => item.Value)
																					.OrderBy (item => item.VisitNumber)
																					.ThenBy (item => item.FacilityId), true);
				
					Console.WriteLine("Duplicate Values Loaded into Excel File \"{0}\"", excelFilePath);
					Console.WriteLine("Number of Cells loaded into WorkBook \"{0}\" is {1:###,###,##0}",
										excelWorkSheetName,
										loadRange.LongCount());
					
					excelPkg.Save();
				}
			}
			else
			{
				Console.WriteLine("Skipping CQL \"{0}\" since it had no rows returned", cqlSelectString);
			}
		}
	}		
	
}

DataTable ReadThriftRowIntoRecordsSaveToDataTable(ISession session,
													string cqlString,
													string dataTableName,
													bool treatColumnsAsPositional = false,
													int queryPageSize = int.MaxValue,
													ConsistencyLevel? queryConsistencyLevel = null,
													IEnumerable<string> onlyTheseColumns = null)
{
	var dataTable = new System.Data.DataTable(dataTableName);
	
	try
	{		
		var cqlStatement = new SimpleStatement(cqlString)
								.SetPageSize(queryPageSize)
								.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
								.SetConsistencyLevel(queryConsistencyLevel);
		var partitionKeyColumn = dataTable.Columns.Add("PartitionKey", typeof(string));
		
		partitionKeyColumn.AllowDBNull = false;
		partitionKeyColumn.Unique = true;
		dataTable.PrimaryKey = new System.Data.DataColumn[] { partitionKeyColumn };
		
		using(var rowSet = session.Execute(cqlStatement))	
		{
			string partitionKey;
			string columnName;
			System.Data.DataRow dataRow;
			System.Data.DataColumn dataColumn;
			var initialNbrColumns = dataTable.Columns.Count;
			var columnPos = initialNbrColumns;
			
			foreach (var row in rowSet.GetRows())
			{
				partitionKey = row.GetValue<string>(0);
				columnName = row.GetValue<string>(1);
				
				if(onlyTheseColumns != null
						&& !onlyTheseColumns.Contains(columnName))
				{
					continue;
				}
				
				dataRow = dataTable.Rows.Find(partitionKey);
				
				if(dataRow == null)
				{
					columnPos = initialNbrColumns;
				}
				else
				{
					++columnPos;
				}
				
				if(treatColumnsAsPositional)
				{
					if(columnPos >= dataTable.Columns.Count)
					{
						dataColumn = dataTable.Columns.Add(string.Format("Col{0}", columnPos), typeof(string));
						dataColumn.AllowDBNull = true;
						dataColumn.DefaultValue = null;
					}
					else
					{
						dataColumn = dataTable.Columns[columnPos];
					}
				}
				else if(dataTable.Columns.Contains(columnName))
				{
					dataColumn = dataTable.Columns[columnName];
				}
				else
				{
					dataColumn = dataTable.Columns.Add(row.GetValue<string>(1), typeof(string));
					dataColumn.AllowDBNull = true;
					dataColumn.DefaultValue = null;
				}
				
				if(dataRow == null)
				{
					dataRow = dataTable.NewRow();
					dataRow[partitionKeyColumn] = partitionKey;
					
					dataTable.Rows.Add(dataRow);
				}
				
				dataRow[dataColumn] = row.GetValue<string>(2);
				
			}
			
			dataTable.AcceptChanges();
			
			var dtError = dataTable.GetErrors();
			
			if(dtError.Length > 0)
			{
				dataTable.GetErrors().Dump();
			}
			
			Console.WriteLine("Number of Records for CQL \"{0}\" is {1:###,###,##0}", cqlString, dataTable.Rows.Count);
		}		
	}
	catch(System.Exception e)
	{
		e.Dump(1);
		Console.WriteLine("Skipping CQL \"{0}\" due to Exception \"{1}\" ({2})", cqlString, e.GetType().Name, e.Message);
		dataTable.Clear();
		dataTable.AcceptChanges();
	}
	
	return dataTable;
}

IEnumerable<string> GetKeySpaces(ISession session,
									string mustHaveTable)
{
	var keySpaces = new List<string>();
	
	foreach(var keySpaceName in session.Cluster.Metadata.GetKeyspaces())
	{
		if(mustHaveTable == null || session.Cluster.Metadata.GetTables(keySpaceName).Contains(mustHaveTable))
		{
			keySpaces.Add(keySpaceName);
		}
	}
	
	return keySpaces;
}

// Define other methods and classes here