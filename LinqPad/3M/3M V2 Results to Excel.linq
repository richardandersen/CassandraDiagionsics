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
	var connectionString = "Contact Points=192.168.190.134";//"Contact Points=127.0.0.1"; WIN-DCE2X-3M; 192.168.190.134;169.10.60.206
	var cqlString = "select * from v2_volatile.person;"	; //v2_volatile.encounter
	
	var excelWorkSheetName = "Person"; //No longer than 25 characters
	bool includeCollectionsAsWorkSheets = false; //If false no collection columns will be dumped into a worksheet
	int[] onlyIncludeCollectionsForRowNbrs = new int[0]; //new int[] { 1, 10, 15 }; //If empty array all rows, if null no rows
	
	var targetFilePath = @"D:\Projects\DataStax\Projects\3M\Person CheckSum Record Test.xlsx";
	
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
		ReadCQLSaveToExcel(excelPkg,
							session,
							cqlString,
							excelWorkSheetName,
							includeCollectionsAsWorkSheets,
							onlyIncludeCollectionsForRowNbrs);
							
		excelPkg.Save();
	}
}

void ReadCQLSaveToExcel(ExcelPackage excelPkg, 
						Session session,
						string cqlString,
						string excelWorkSheetName,
						bool includeCollectionsAsWorkSheets = false, //If false no collection columns will be dumped into a worksheet
						int[] onlyIncludeCollectionsForRowNbrs = null
					)
{
	var dataTable = new System.Data.DataTable(excelWorkSheetName);
	var dataColumnCollections = new Dictionary<System.Data.DataColumn, Tuple<string,System.Type>>();
	
	using (var rowSet = session.Execute(cqlString))
	{
		var workBook = excelPkg.Workbook.Worksheets[excelWorkSheetName];
		if (workBook == null)
		{
			workBook = excelPkg.Workbook.Worksheets.Add(excelWorkSheetName);
		}
		
		System.Data.DataRow dataRow = null;
		System.Reflection.MethodInfo methodInfoCollectionCount = null;
		System.Reflection.MethodInfo methodInfoLoadFromCollection = null;
	
		foreach (var row in rowSet.GetRows())
		{
			//If null, must be the first time into the loop. Need to create columns
			if (dataRow == null)
			{
				foreach (var column in rowSet.Columns)
				{
					if (column.TypeCode == ColumnTypeCode.Set
							|| column.TypeCode == ColumnTypeCode.Map)
					{
						dataColumnCollections.Add(dataTable.Columns.Add(string.Format("{0} ({1} Number of Elements)",
																						column.Name,
																						column.TypeCode),
																			typeof(long)),
																			new Tuple<string,Type>(column.Name,
																									column.Type));
	
						if (methodInfoCollectionCount == null)
						{
							var methods = typeof(System.Linq.Enumerable).GetMethods(System.Reflection.BindingFlags.Public
																					| System.Reflection.BindingFlags.Static);
	
							methodInfoCollectionCount = methods.Where(item => item.IsGenericMethodDefinition 
																		&& item.Name == "LongCount" 
																		&& item.GetParameters().Length == 1).FirstOrDefault();
	
							if (includeCollectionsAsWorkSheets && onlyIncludeCollectionsForRowNbrs != null)
							{
								methods = typeof(OfficeOpenXml.ExcelRangeBase).GetMethods();
	
								methodInfoLoadFromCollection = methods.Where(item => item.IsGenericMethodDefinition
																				&& item.Name == "LoadFromCollection"
																				&& item.GetParameters().Length == 1).FirstOrDefault();
							}
						}
					}
					else
					{
						dataTable.Columns.Add(column.Name, column.Type);
					}
				}
			}
	
			dataRow = dataTable.NewRow();
	
			dataTable.Rows.Add(dataRow);
	
			for (int colIndex = 0; colIndex < row.Count(); ++colIndex)
			{
				Tuple<string, Type> collectionColumn;
	
				if (dataColumnCollections.TryGetValue(dataTable.Columns[colIndex], out collectionColumn))
				{
					var rowCollectionValue = row.GetValue(collectionColumn.Item2, colIndex);
					var rowNbr = dataTable.Rows.Count;
	
					if (rowCollectionValue == null || methodInfoCollectionCount == null)
					{
						dataRow[colIndex] = DBNull.Value;
					}
					else
					{
						System.Reflection.MethodInfo generic = methodInfoCollectionCount.MakeGenericMethod(rowCollectionValue.GetType().GenericTypeArguments[0]);
						dataRow[colIndex] = generic.Invoke(null, new object[] { rowCollectionValue });
	
						if (methodInfoLoadFromCollection != null
								&& (onlyIncludeCollectionsForRowNbrs.Length == 0
										|| onlyIncludeCollectionsForRowNbrs.Contains(rowNbr)))
						{
							var collectionWorkBookName = string.Format("{0} {1}{2}", 
																			excelWorkSheetName,
																			collectionColumn.Item1,
																			rowNbr);
							if (collectionWorkBookName.Length >= 30)
							{
								collectionWorkBookName = string.Format("{0} {1} {2}",
																			excelWorkSheetName,
																			colIndex,
																			rowNbr);
							}
	
							var collectionWorkBook = excelPkg.Workbook.Worksheets[collectionWorkBookName];
							if (collectionWorkBook == null)
							{
								collectionWorkBook = excelPkg.Workbook.Worksheets.Add(collectionWorkBookName);
							}
	
							generic = methodInfoLoadFromCollection.MakeGenericMethod(rowCollectionValue.GetType().GenericTypeArguments[0]);
							generic.Invoke(collectionWorkBook.Cells["A2"], new object[] { rowCollectionValue });
																
							var excelHyperLnk = new ExcelHyperLink(string.Format("{0}!A{1}", excelWorkSheetName, rowNbr + 1),
																	string.Format("Go To Associated Row (Nbr {1}/\"{2}\") in Master Sheet \"{0}\"",
																						excelWorkSheetName,
																						rowNbr,
																						dataRow[0]));
	
							collectionWorkBook.Cells["A1"].Style.Font.UnderLine = true;
							collectionWorkBook.Cells["A1"].Style.Font.Color.SetColor(System.Drawing.Color.Blue);
	
							collectionWorkBook.Cells["A1"].Hyperlink = excelHyperLnk;
						}
					}
				}
				else
				{                            
					var rowValue = row.GetValue(dataTable.Columns[colIndex].DataType, colIndex);
	
					if(rowValue == null)
					{
						dataRow[colIndex] = DBNull.Value;
					}
					else
					{
						dataRow[colIndex] = rowValue;
					}
				}
			}
	
		}
	
		dataTable.AcceptChanges();      
		
		var dtError = dataTable.GetErrors();
		
		if(dtError.Length > 0)
		{
			dataTable.GetErrors().Dump();
		}
		
		Console.WriteLine("Number of Records for CQL \"{0}\" is {1}", cqlString, dataTable.Rows.Count);
		
		var loadRange = workBook.Cells["A1"].LoadFromDataTable(dataTable, true);
		
		Console.WriteLine("Number of Cells loaded into WorkBook \"{0}\" is {1:###,###,##0}", excelWorkSheetName, loadRange.LongCount());
	}
}