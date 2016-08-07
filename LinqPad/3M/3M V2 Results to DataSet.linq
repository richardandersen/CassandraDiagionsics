<Query Kind="Program">
  <Reference Relative="Cassandra.Data.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.dll</Reference>
  <Reference Relative="Cassandra.Data.Linq.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.Linq.dll</Reference>
  <Reference Relative="Cassandra.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.dll</Reference>
  <Reference Relative="Common.Functions.dll">&lt;MyDocuments&gt;\LINQPad Queries\Common.Functions.dll</Reference>
  <Reference Relative="Common.Patterns.Shared.dll">&lt;MyDocuments&gt;\LINQPad Queries\Common.Patterns.Shared.dll</Reference>
  <Reference Relative="Common.Patterns.Singleton.dll">&lt;MyDocuments&gt;\LINQPad Queries\Common.Patterns.Singleton.dll</Reference>
  <Reference Relative="EPPlus.dll">&lt;MyDocuments&gt;\LINQPad Queries\EPPlus.dll</Reference>
  <Reference Relative="LZ4.dll">&lt;MyDocuments&gt;\LINQPad Queries\LZ4.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>OfficeOpenXml</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.190.134";//"Contact Points=127.0.0.1"; WIN-DCE2X-3M; 192.168.190.134;169.10.60.206
	var cqlString = "select * from v2_volatile.encounter;"	; //v2_volatile.encounter
	
	var dataTableName = "Encounter";
	bool includeCollectionsAsDataTables = false; //If false no collection columns will be dumped into a seperate datatable saved within the DataSet
	int[] onlyIncludeCollectionsForRowNbrs = new int[0]; //new int[] { 1, 10, 15 }; //If empty array all rows, if null no rows
	
	var dataSet = new DataSet();
	
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
							.Build())
	using (var session = cluster.Connect())
	{
		var personDataSet = ReadCQLSaveToDataTable(dataSet,
													session,
													cqlString,
													dataTableName,
													includeCollectionsAsDataTables,
													onlyIncludeCollectionsForRowNbrs).Tables[dataTableName].AsEnumerable()
							.Where (x => (x.Field<DateTimeOffset?>("LastUpdate").HasValue 
												? x.Field<DateTimeOffset?>("LastUpdate").Value.Date
												: DateTime.MinValue)  >= new DateTime(2014, 1, 1) 
											&& (x.Field<DateTimeOffset?>("LastUpdate").HasValue 
													? x.Field<DateTimeOffset?>("LastUpdate").Value.Date
													: DateTime.MaxValue) <= new DateTime(2014, 6, 1))
							.Count ()
//							.GroupBy (row => row.Field<string>("FacilityId"))
//							.Select(groupStatus => new { Status = groupStatus.Key, 
//															Count = groupStatus.Count() //,
//															//Ids = groupStatus.ToList()
//															//				.Select (r => r.Field<string>("Id"))
//															//				.Distinct() 
//														})
							.Dump();
	}
	
	//dataSet.Tables[dataTableName].Rows[0].Dump();
	//dataSet.Tables[2].Rows[0].Dump();
	//dataSet.Tables[2].TableName.Dump();
}

DataSet ReadCQLSaveToDataTable(DataSet dataSet, 
									ISession session,
									string cqlString,
									string dataTableName,
									bool includeCollectionsAsDataTables = false, //If false no collection columns will be dumped into a DataTable
									int[] onlyIncludeCollectionsForRowNbrs = null
								)
{
	var existingDataTable = dataSet.Tables.Contains(dataTableName);
	var dataTable = existingDataTable ? dataSet.Tables[dataTableName] : new System.Data.DataTable(dataTableName);
	var dataColumnCollections = new Dictionary<System.Data.DataColumn, Tuple<string,System.Type>>();
	
	if(existingDataTable)
	{
		dataTable.Clear();
	}
	else
	{
		dataSet.Tables.Add(dataTable);
	}
	
	using (var rowSet = session.Execute(cqlString))
	{
		System.Data.DataRow dataRow = null;
		System.Reflection.MethodInfo methodInfoCollectionCount = null;
		
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
						dataColumnCollections.Add(dataTable.Columns.Add(string.Format("{0} ({1} Type Column; Number of Elements)",
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
	
						if (includeCollectionsAsDataTables
								&& (onlyIncludeCollectionsForRowNbrs.Length == 0
										|| onlyIncludeCollectionsForRowNbrs.Contains(rowNbr)))
						{
							var collectionDataTableName = string.Format("{0}.{1}.{2}", 
																			dataTableName,
																			collectionColumn.Item1,
																			rowNbr);
						
							var collectionDataTable = Common.DataTableExtensions.ConvertToDataTable((System.Collections.IEnumerable) rowCollectionValue, collectionDataTableName);
	
							if(dataSet.Tables.Contains(collectionDataTableName))
							{
								dataSet.Tables.Remove(collectionDataTableName);
							}
							
							collectionDataTable.AcceptChanges();
							
							var collectionDTErrors = collectionDataTable.GetErrors();
		
							if(collectionDTErrors.Length > 0)
							{
								collectionDTErrors.Dump();
							}
							
							dataSet.Tables.Add(collectionDataTable);
							
							//Console.WriteLine("Number of Records for Collection Column \"{0}\" is {1:###,###,##0}", 
							//					collectionColumn.Item1,
							//					collectionDataTable.Rows.Count);
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
		
		var dtErrors = dataTable.GetErrors();
		
		if(dtErrors.Length > 0)
		{
			dtErrors.Dump();
		}
		
		Console.WriteLine("Number of Records for CQL \"{0}\" is {1:###,###,##0}", cqlString, dataTable.Rows.Count);
		
		return dataSet;
	}
}