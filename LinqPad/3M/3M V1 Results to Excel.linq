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
	var connectionString = "Contact Points=192.168.190.133"; //"Contact Points=192.168.190.132"; Contact Points=127.0.0.1 WIN-DCE12X-3M
	var cqlString = "select blobasText(key), blobasText(column1), blobasText(value) from \"KSV1_VOLATILE\".\"EncounterAudit\";"; //KS055298 KSV1_VOLATILE 
	//var cqlString = "select key, column1, value from \"KSV1_VOLATILE\".\"Encounter\";"; //KS055298 
	
	var excelWorkSheetName = "EncounterAudit";

	var targetFilePath = @"D:\Projects\DataStax\Projects\3M\Version1 Data Dump.xlsx";
	
	var excelFile = new FileInfo(targetFilePath);
	
	using (var excelPkg = new ExcelPackage(excelFile))
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
							.Build())
	using (var session = cluster.Connect())
	using(var rowSet = session.Execute(cqlString))	
	{
		var workBook = excelPkg.Workbook.Worksheets[excelWorkSheetName];
		if(workBook == null)
		{
			workBook = excelPkg.Workbook.Worksheets.Add(excelWorkSheetName);
		}
	
		var rows = from row in rowSet.GetRows()
					select new { key=row.GetValue<string>(0),
									colum=row.GetValue<string>(1),
									value=row.GetValue<string>(2) 
								};
	
		workBook.Cells["A1"].LoadFromCollection(rows);
		
        excelPkg.Save();
	}		
	
}

// Define other methods and classes here