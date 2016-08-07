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
	var connectionStringV1 = "Contact Points=192.168.190.133";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	var connectionStringV2 = "Contact Points=192.168.190.134";//"Contact Points=127.0.0.1"; WIN-DCE2X-3M; 192.168.190.134;169.10.60.206
	
	var targetFilePath = @"D:\Projects\DataStax\Projects\3M\Excel Validation\Person Validation.xlsm"; //xlsx;
	
	var cqlV1PersonAudit = "select blobasText(key), blobasText(column1), blobasText(value) from \"KSV1_VOLATILE\".\"PersonAudit\";"; //KS055298 
	var cqlV1Person = "select blobasText(key), blobasText(column1), blobasText(value) from \"KSV1_VOLATILE\".\"Person\";"; 
	var cqlV1idxPersonIdEncounter = "select blobasText(key), blobasText(column1), blobasText(value) from \"KSV1_VOLATILE\".\"idxPersonIdEncounter\";"; 
	
	var excelWorkSheetNameV1PersonAudit = "V1.PersonAudit";
	var excelWorkSheetNameV1Person = "V1.Person";
	var excelWorkSheetNameV1idxPersonIdEncounter = "V1.idxPersonIdEncounter";
	
	var cqlV2Person = "select * from v2_volatile.person;";  //v2_volatile
	var cqlV2PersonByMrn = "select * from v2_volatile.personbymrn;"; 
	
	var excelWorkSheetNameV2Person = "V2.person";
	var excelWorkSheetNameV2PersonByMrn = "V2.personbymrn";
	
	
	var excelFile = new FileInfo(targetFilePath);
	
	using (var excelPkg = new ExcelPackage(excelFile))
	using (var clusterV1 = Cluster
							.Builder()
							.WithConnectionString(connectionStringV1)
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
							.Build())
	using (var clusterV2 = Cluster
							.Builder()
							.WithConnectionString(connectionStringV2)
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
							.Build())
	using (var sessionV1 = clusterV1.Connect())
	using (var sessionV2 = clusterV2.Connect())
	{
		ReadThriftRowIntoRecordsSaveToExcel(excelPkg,
											sessionV1,
											cqlV1PersonAudit,
											excelWorkSheetNameV1PersonAudit);
											
		ReadThriftRowIntoRecordsSaveToExcel(excelPkg,
											sessionV1,
											cqlV1Person,
											excelWorkSheetNameV1Person);
		
		ReadThriftRowIntoRecordsSaveToExcel(excelPkg,
											sessionV1,
											cqlV1idxPersonIdEncounter,
											excelWorkSheetNameV1idxPersonIdEncounter,
											true);
											
		ReadCQLSaveToExcel(excelPkg,
							sessionV2,
							cqlV2Person,
							excelWorkSheetNameV2Person);
							
		ReadCQLSaveToExcel(excelPkg,
							sessionV2,
							cqlV2PersonByMrn,
							excelWorkSheetNameV2PersonByMrn);
		
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