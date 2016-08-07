<Query Kind="Program">
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Collections.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Shared.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Singleton.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\SourceRepository\bin\Release\SourceRepository.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\SourceRepository\bin\Release\System.Threading.Tasks.Dataflow.dll</Reference>
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>Common</Namespace>
  <Namespace>Common.Patterns</Namespace>
  <Namespace>SourceRepository</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.117.132"; //10.200.240.251"; //192.168.117.130";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	string userName = "";
	string password = "";
	string dcName = ""; //DataCenter Name
	string keySpaceNameOrAll = "\"KSV1_VOLATILE\""; //If null or empty string, all Thrift keyspaces will be used. If a name, only that keyspace will be used.
	var consistencyLevel = ConsistencyLevel.LocalOne;
	var compression = CompressionType.Snappy;
	int pageSize = 500;
	bool checkDataConsistence = true;
	bool runConcurrently = false;
	
	//Approach:
	// idxMrnByPerson() => s0.<Mrn, Facility, Person>
	//		Reduce<Keep Duplicate Mrns>(s0) => r1.<Mrn, Facility, Person>
	//			Reduce<Remove Duplicate Mrn and Facilities>(r1) => r2.<Mrn, Facility, Person>
	//				idxPersonByEncounter<Search By Person>(r2) => k1.<Person, Encounter>
	//					Encounter<Search by Encounter>(k1) => k2.<Encounter, Person, Visit>
	//	Join<r2 by r4 on Person>(r2,k2) => j1.<Mrn, Facility, Person, Visit>
	//		
	
	var idxMrnPersonCQL = "select blobastext(key), blobastext(column1), blobastext(value) from {0}.\"idxMrnPerson\";";
	var idxPersonIdEncounterCQL = "select blobastext(key), blobastext(column1), blobastext(value) from {0}.\"idxPersonIdEncounter\" where key = textasblob(?);";
	var encounterTableCQL = "select blobastext(key), blobastext(column1), blobastext(value) from {0}.\"Encounter\" where key = textasblob(?) and column1 in (textasblob('VisitNumber'), textasblob('Status'), textasblob('EPRSVersion'), textasblob('PersonID'), textasblob('FacilityID'));";
	
	
	var possibleExceptions = new Common.Patterns.Collections.ThreadSafe.List<Tuple<string,string,object>>();
	
	using (var cluster = AddCredentials(Cluster
											.Builder()
											.WithConnectionString(connectionString)
											.WithoutRowSetBuffering()
											.WithLoadBalancingPolicy(new TokenAwarePolicy(string.IsNullOrEmpty(dcName)
																							? (ILoadBalancingPolicy) new RoundRobinPolicy()
																							: (ILoadBalancingPolicy) new DCAwareRoundRobinPolicy(dcName)))
											.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(consistencyLevel).SetPageSize(pageSize))
											//.WithSSL()
											.WithCompression(compression),
											userName,
											password
											)
							.Build())
	using (var session = cluster.Connect())
	{
		foreach (var keySpace in string.IsNullOrEmpty(keySpaceNameOrAll) ? GetKeySpaces(session, null) : (IEnumerable<string>) new string[] { keySpaceNameOrAll })
		{
			//Get All Mrn|Facility,Person Ids from idxMrnPerson. Note that the PK is the Mrn and Facility
			var recordMrnFacilityPersons = new List<Tuple<string,string,string>>();
			
			using(var idxMrnPersonResultSet = session.Execute(string.Format(idxMrnPersonCQL, keySpace)))
			{
				foreach (var row in idxMrnPersonResultSet)
				{
					var splitKey = row.GetValue<string>(0).Split('|');
					
					//Item1 = MRN, Item2 = Facility, Item3 = PersonId
					recordMrnFacilityPersons.Add(new Tuple<string,string,string>(splitKey[0],splitKey.ElementAtOrDefault(1),row.GetValue<string>(2)));
				}
			}
			
			//Find Duplicate MRNs then Remove Duplicate Facilities (leaving Same MRN but different Facilities)
			var duplicateMRNWithDifferentFacilities = recordMrnFacilityPersons
														.DuplicatesWithRecord(groupItem => groupItem.Item1) //Keep Duplicate MRNs
														.SelectMany (mrnRec => mrnRec.Value) //Flatten the results into one collection
														.DuplicatesRemoved(groupItem => groupItem.Item1 + "|" + groupItem.Item2); //Remove duplicate MRN that have the same Facility (leaving only differnt facilities)
			
			duplicateMRNWithDifferentFacilities.Dump("MRN, Facility, Person");
			
			//We need to remove any duplicate Person ids from the result and query C* using the idxPersonIdEncounter column family to get the encounter Id
			var getEncountersForPersons = duplicateMRNWithDifferentFacilities.Select (mrnwdf => mrnwdf.Item3).DuplicatesRemoved(groupSelector => groupSelector);
			var idxPersonIdEncounterPrepare = session.Prepare(string.Format(idxPersonIdEncounterCQL, keySpace));
			var encounterPrepare = session.Prepare(string.Format(encounterTableCQL, keySpace));
			var personAssociatedEncounters = runConcurrently
													? (IDictionary<string,DataTable>) new Common.Patterns.Collections.ThreadSafe.Dictionary<string,DataTable>(getEncountersForPersons.Count())
													: (IDictionary<string,DataTable>) new Dictionary<string,DataTable>(getEncountersForPersons.Count());
			
			getEncountersForPersons.Dump("Persons used to Query Assocated Encounters");
			
			RunInParallel(getEncountersForPersons,
							runConcurrently,
							searchForPerson
				=> {
						//Using Person Id, get the associated encounter id which will be used to get the Encounter Record
						using(var idxPersonIdEncounterResultSet = session.Execute(idxPersonIdEncounterPrepare.Bind(searchForPerson)))
						{
							DataTable encounterDataTable = null;
							
							//Start building assoicated Encounter Records
							foreach (var encounterId in idxPersonIdEncounterResultSet.Select(piers => piers.GetValue<string>(1)).DuplicatesRemoved(groupSelector => groupSelector))
							{
								encounterDataTable = ReadThriftRowIntoRecordsSaveToDataTable(session,
																								encounterPrepare.Bind(encounterId),
																								"EncountersAssociatedWithPerson",
																								false,
																								encounterDataTable);
							}
							
							if(checkDataConsistence)
							{
								if(encounterDataTable == null || encounterDataTable.Rows.Count == 0)
								{
									possibleExceptions.Add(new Tuple<string,string,object>(searchForPerson, null, "Person has No associated Encounters"));
								}
								else 
								{
									var invalidEncounters = encounterDataTable.Select().Where(r => ((DataRow) r).Field<string>("PersonID") != searchForPerson);
									
									foreach (var element in invalidEncounters)
									{
										possibleExceptions.Add(new Tuple<string,string,object>(element.Field<string>("PartitionKey"), 
																								searchForPerson, 
																								string.Format("Encounter {0} for Associated Person {1} did not match the PersonID found in the Encounter Record. Value Found is {2}",
																												element.Field<string>("PartitionKey"),
																												searchForPerson,
																												element.Field<string>("PersonID"))));
									}
								}
							}
							
							personAssociatedEncounters.Add(searchForPerson, encounterDataTable);
						}
					}
				); //Dictionary of Person Ids (key) and associated Encounter Records 
			
			personAssociatedEncounters.Dump("Encounter Recods associarted with Person", 1);
			
			//Let us merge the encounter information into our MRN,Facility,Person list creating a new list of MRN,Facility,Person,List<VisitId>
			var listMrnFacilityPersonVisitIds =  duplicateMRNWithDifferentFacilities.Join(personAssociatedEncounters, 
														 									(dupMRN) => dupMRN.Item3, 
														 									(personId, dupMRN, encounterDataTable) =>
																								{
																									return new Tuple<string,string,string,IEnumerable<string>>(dupMRN.Item1,
																																								dupMRN.Item2,
																																								personId,
																																								encounterDataTable
																																									.Select()
																																									.Select (r => ((DataRow)r).Field<string>("VisitNumber")));
																									});
																									
			listMrnFacilityPersonVisitIds.Dump(1);
			
		} //foreach Keyspace
		
		
	}	
}

Builder AddCredentials(Builder clusterBuilder, string userName, string password)
{
	if(!string.IsNullOrEmpty(userName))
	{
		clusterBuilder = clusterBuilder.WithCredentials(userName, password);
	}
	
	return clusterBuilder;
}

public IEnumerable<string> GetKeySpaces(ISession session, string mustHaveTable)
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

void RunInParallel<T>(IEnumerable<T> collection, bool runInParallel, Action<T> action)
{
	if(runInParallel)
	{
		System.Threading.Tasks.Parallel.ForEach(collection,
													element => action(element));
	}
	else
	{
		foreach (var element in collection)
		{
			action(element);
		}
	}
}

DataTable ReadThriftRowIntoRecordsSaveToDataTable(ISession session,
													BoundStatement cqlBoundStmt,
													string dataTableName,
													bool treatColumnsAsPositional = false,
													DataTable useThisDataTable = null)
													//int queryPageSize = int.MaxValue,
													//ConsistencyLevel? queryConsistencyLevel = null)
{
	var dataTable = useThisDataTable ?? new System.Data.DataTable(dataTableName);
//	var cqlStatement = new SimpleStatement(cqlString)
//							.SetPageSize(queryPageSize)
//							.SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
//							.SetConsistencyLevel(queryConsistencyLevel);
	DataColumn partitionKeyColumn;
	
	if(useThisDataTable == null)
	{
		partitionKeyColumn = dataTable.Columns.Add("PartitionKey", typeof(string));
	}
	else
	{
		partitionKeyColumn = dataTable.Columns["PartitionKey"];
	}
	
	partitionKeyColumn.AllowDBNull = false;
	partitionKeyColumn.Unique = true;
	dataTable.PrimaryKey = new System.Data.DataColumn[] { partitionKeyColumn };
	
	using(var rowSet = session.Execute(cqlBoundStmt))	
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
		
		//Console.WriteLine("Number of Records for Table \"{0}\" is {1:###,###,##0}", dataTableName, dataTable.Rows.Count);
		
		return dataTable;
	}
}