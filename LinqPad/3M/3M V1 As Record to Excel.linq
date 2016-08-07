<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\CassandraCSharpDriver.1.0.4\lib\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\CassandraCSharpDriver.1.0.4\lib\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\CassandraCSharpDriver.1.0.4\lib\Cassandra.dll</Reference>
  <Reference>C:\bin\EPPlus\EPPlus.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>OfficeOpenXml</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.190.132";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	var cqlString = "select blobasText(key), blobasText(column1), blobasText(value) from \"KSV1_VOLATILE\".\"PersonAudit\";"; //KS055298 
	//var cqlString = "select key, column1, value from \"KSV1_VOLATILE\".\"EncounterAudit\";"; //KS055298 
	var excelWorkSheetName = "PersonAudit";

	var targetFilePath = @"D:\Projects\DataStax\Projects\3M\Version1 Person Record Dump.xlsx";
	
	var excelFile = new FileInfo(targetFilePath);
	
	using (var excelPkg = new ExcelPackage(excelFile))
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
							.Build())
	using (var session = cluster.Connect())
	{
		ReadThriftRowIntoRecordsSaveToExcel(excelPkg,
											session,
											cqlString,
											excelWorkSheetName);
											
		excelPkg.Save();
	}		
	
}

void ReadThriftRowIntoRecordsSaveToExcel(ExcelPackage excelPkg, 
											Session session,
											string cqlString,
											string excelWorkSheetName,
											bool treatColumnsAsPositional = false)
{
	var dataTable = new System.Data.DataTable(excelWorkSheetName);
	
	var partitionKeyColumn = dataTable.Columns.Add("PartitionKey", typeof(string));
	partitionKeyColumn.AllowDBNull = false;
	partitionKeyColumn.Unique = true;
	dataTable.PrimaryKey = new System.Data.DataColumn[] { partitionKeyColumn };
	
	using(var rowSet = session.Execute(cqlString))	
	{
		var workBook = excelPkg.Workbook.Worksheets[excelWorkSheetName];
		if(workBook == null)
		{
			workBook = excelPkg.Workbook.Worksheets.Add(excelWorkSheetName);
		}
	
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
		
		var loadRange = workBook.Cells["A1"].LoadFromDataTable(dataTable, true);
		
		Console.WriteLine("Number of Cells loaded into WorkBook \"{0}\" is {1:###,###,##0}", excelWorkSheetName, loadRange.LongCount());
	}
}

// Define other methods and classes here