<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data.Linq\bin\Release\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\charp-driver 2.0.2\src\Cassandra.Data\bin\Release\Cassandra.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.190.132";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	//var cqlString = "select blobasText(key), blobasText(column1), blobasText(value) from \"KSV1_VOLATILE\".\"PersonAudit\";"; //KS055298 
	var cqlString = "select key, column1, value from \"KSV1_VOLATILE\".\"Encounter\";"; //KS055298 
	var treatColumnsAsPositional = false; //true only if the column family has many dynamic columns where each column name is unique 
	int cqlQueryPagingSize = 1000; 
	ConsistencyLevel? queryConsistencyLevel = ConsistencyLevel.One;
	var dataTableName = "Encounter"; //DataTable Name, shoudld be the same as the Column Family Name
	
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.Build())
	using (var session = cluster.Connect())
	{
		var distinctVisitNumbers = ReadThriftRowIntoRecordsSaveToDataTable(session,
																			cqlString,
																			dataTableName,
																			treatColumnsAsPositional,
																			cqlQueryPagingSize,
																			queryConsistencyLevel).AsEnumerable()
									.Select (row => new { VisitNumber = row.Field<string>("VisitNumber"), PersionId = row.Field<string>("PersonID") })
									.Where (row => row.VisitNumber == "VNUM:1004")
									.OrderBy(obr => string.IsNullOrEmpty(obr.VisitNumber) 
													? obr.VisitNumber 
													: obr.VisitNumber.Split(':')[1]);
									
		distinctVisitNumbers.Dump();
																			
	}		
	
}

DataTable ReadThriftRowIntoRecordsSaveToDataTable(ISession session,
													string cqlString,
													string dataTableName,
													bool treatColumnsAsPositional = false,
													int queryPageSize = int.MaxValue,
													ConsistencyLevel? queryConsistencyLevel = null)
{
	var dataTable = new System.Data.DataTable(dataTableName);
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
		
		return dataTable;
	}
}

// Define other methods and classes here