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
	var connectionString = "Contact Points=192.168.190.134";
	var cqlString = "select * from \"ExampleDataStaxLINQ\".\"nerdiStuff\";"	;
	
	var dataTableName = "nerdiStuff";	
	var dataSet = new DataSet();
	
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy())
							.Build())
	using (var session = cluster.Connect())
	{
		//Update C* CQL table 
		string keyspaceName = "ExampleDataStaxLINQ";

		session.CreateKeyspaceIfNotExists(keyspaceName);
		session.ChangeKeyspace(keyspaceName);
		
		var table = session.GetTable<NerdMovie>();
		table.CreateIfNotExists();
	
		{
			var batch = session.CreateBatch();
	
			var movies = new List<NerdMovie>()
			{
				new NerdMovie(){ Movie = "Serenity", Maker="20CentFox",  Director = "Joss Whedon", MainActor = "Nathan Fillion", Year = 2005 , exampleSet = new List<string>(){"x","y"}},
				new NerdMovie(){ Movie = "Pulp Fiction", Maker = "Pixar", Director = "Quentin Tarantino", MainActor = "John Travolta", Year = 1994, exampleSet = new List<string>(){"1","2","3"}},
			};
	
			batch.Append(from m in movies select table.Insert(m));
	
			batch.Execute();
		}
			
		//Get Current Rows
		Console.WriteLine("Retrieve C* CQL Rows");
		(from row in table select row).Execute().Dump(); //Using the LinqPad Dump method!!!

	
		Console.WriteLine("Dump C* rows into ADO.Net DataSet");
		ReadCQLSaveToDataTable(dataSet,
								session,
								cqlString,
								dataTableName);
	}
		
	Console.WriteLine("Dump data table nerdiStuff");
	dataSet.Tables[dataTableName].AsEnumerable().Dump();	
}

DataSet ReadCQLSaveToDataTable(DataSet dataSet, 
									ISession session,
									string cqlString,
									string dataTableName,
									bool includeCollectionsAsDataTables = false, //If false no collection columns will be dumped into separate DataTable
									int[] onlyIncludeCollectionsForRowNbrs = null //If null, all rows in the collection columns will be dumped. If an array only the row numbers within the array will be dumped.
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
					if (includeCollectionsAsDataTables
							&& (column.TypeCode == ColumnTypeCode.Set
									|| column.TypeCode == ColumnTypeCode.Map
									|| column.TypeCode == ColumnTypeCode.List))
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
								&& (onlyIncludeCollectionsForRowNbrs == null
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

[AllowFiltering]
[Table("nerdiStuff")]
public class NerdMovie
{
  [ClusteringKey(1)]
  [Column("diri")]
  public string Director { get; set; }

  [Column("mainGuy")]
  public string MainActor;

  [PartitionKey(1)]
  [Column("movieTile")]
  public string Movie;

  [PartitionKey(5)]
  [Column("movieMaker")]
  public string Maker;

  [Column("When-Made")]
  public int? Year { get; set; }

  [Column("List")]
  public List<string> exampleSet = new List<string>();
}