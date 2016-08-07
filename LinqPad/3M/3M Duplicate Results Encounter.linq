<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Client\csharp-driver 1_0_4\Cassandra.Data\bin\Debug\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 1_0_4\Cassandra.Data.Linq\bin\Debug\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Client\csharp-driver 1_0_4\Cassandra\bin\Debug\Cassandra.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Shared.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Singleton.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\SourceRepository.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\System.Threading.Tasks.Dataflow.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Common</Namespace>
  <Namespace>SourceRepository</Namespace>
</Query>

void Main()
{
	var cancelSource = new CancellationTokenSource();

	using (var cassandraCluster = Cluster.Builder().WithConnectionString("Contact Points=WIN-DCE12X-3M").WithoutRowSetBuffering().WithLoadBalancingPolicy(new Cassandra.RoundRobinPolicy()).Build())
	{
		using (var cassandraSession = cassandraCluster.Connect())
		{
			var getRecordAccess = new ProcessRepository(cassandraSession,
														cancelSource.Token,
														"select count(*) from \"KSV1_VOLATILE\".\"Encounter\";",
															"select blobastext(key), blobastext(column1), blobastext(value) from \"KSV1_VOLATILE\".\"DocumentAudit\" LIMIT 40;")
															{
																CacheResultSet = ProcessRepository.CacheResultSetType.LimitedQueueCache,
																CacheResultSetLimit = 500																
															};
			getRecordAccess.StartRowProcessing();
	
			getRecordAccess.CachedResultSet.Count().Dump();
			getRecordAccess.CachedResultSet.Dump(1);
			/*var parsedRecords = from record in getRecordAccess.CachedResultSet
								select new
								{
									EncounterId = record.Key,
									//VisitNumber = record.Value["VisitNumber"],
									//FacilityId = record.Value["FacilityID"]                                            
								};
	
			parsedRecords.Dump(1); */
			//var dupRecords = Common.LinqExtensions.DuplicatesWithRecord(parsedRecords, record => new { record.VisitNumber, record.FacilityId }).ToList();
			
			//dupRecords.Dump();
			
			//var dupCount = Common.LinqExtensions.DuplicatesWithCount(parsedRecords, record => new { record.VisitNumber, record.FacilityId }).ToList();
			
			//dupCount.Dump();
		}
	}
}

// Define other methods and classes here