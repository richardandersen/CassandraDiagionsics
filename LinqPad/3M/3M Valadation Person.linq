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
	var v1KeySpaceName = "KSV1_VOLATILE"; //Change Keysace Name
	var v1ConnectionString = "Contact Points=192.168.190.132"; //Change IP Address
	
	//CQL parameters
	//var v1CqlString = string.Format("select key, column1, value from \"{0}\".\"Encounter\" where column1 in ('FacilityID', 'VisitNumber', 'PersonID', 'DateStamp') allow filtering;", keySpaceName); 
	//var v1CqlString = string.Format("select key, column1, value from \"{0}\".\"Encounter\";", keySpaceName);
	//var v1CqlString = string.Format("select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"Encounter\" where column1 in (textasblob('FacilityID'), textasblob('VisitNumber'), textasblob('PersonID'), textasblob('DateStamp')) allow filtering;", keySpaceName); 
	var v1CqlString = string.Format("select blobastext(key), blobastext(column1), blobastext(value) from \"{0}\".\"Encounter\";", keySpaceName);
	int cqlQueryPagingSize = 1000; 
	ConsistencyLevel? queryConsistencyLevel = ConsistencyLevel.Quorum;
	
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.Build())
	using (var session = cluster.Connect())
	{
		var recordVisitNumberFacilityIds = ReadThriftRowIntoRecordsSaveToDataTable(session,
																					cqlString,
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
	
}

void ReadThriftRowIntoRecordsValadate(ISession session,
													string cqlString,												
													int queryPageSize = int.MaxValue,
													ConsistencyLevel? queryConsistencyLevel = null,
													IEnumerable<string> onlyTheseColumns = null)
{
	var dataTable = new System.Data.DataTable();
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
		System.Data.DataRow prevDataRow = null;
		System.Data.DataColumn dataColumn;
		int nRecordCount = 0;
		
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
			
			if(dataRow == null && prevDataRow != null)
			{
				
			}
						
			if(dataTable.Columns.Contains(columnName))
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
				++nRecordCount;
			}
			else
			{
				prevDataRow = dataRow;
			}
			
			dataRow[dataColumn] = row.GetValue<string>(2);
			
		}
		
		Console.WriteLine("Number of Records for CQL \"{0}\" is {1:###,###,##0}", cqlString, dataTable.Rows.Count);
		
	}
}

// Define other methods and classes here