<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\Cassandra.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\Exceptron.Client.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\Exceptron.Log4Net.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\log4net.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\LZ4.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\SourceRepository.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\EPRS Migration DataStax\EPRSMigrationConsole\bin\Debug\System.Threading.Tasks.Dataflow.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
</Query>

void Main()
{
	var connectionStringV1 = "Contact Points=192.168.58.132";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	var cancellationSource = new CancellationTokenSource();
	
	using (var clusterV1 = Cluster
							.Builder()
							.WithConnectionString(connectionStringV1)
							.WithoutRowSetBuffering()
							.Build())
	using (var sessionV1 = clusterV1.Connect())
	{
		var auditRepsoitory = new SourceRepository.ProcessRepository(sessionV1,
                                                                      cancellationSource.Token,
                                                                     	null,
                                                                      	"select  blobAsText(key), blobAsText(column1), blobAsText(value)  from \"KSV1_VOLATILE\".\"ChangeHx\" where key = textasblob(?);")
                {
                    BlockSize = 100,
                    CQLPageSize = 100,
                    ProcessRecordTransformationSynchronous = false,
                    ProcessRecordBlockSynchronous = true,
                    ClusterName = sessionV1.Cluster.Metadata.ClusterName,
                    ProcessingRecordTransformActionOptions = new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
                    {
                        SingleProducerConstrained = true,
                        CancellationToken = cancellationSource.Token,
                        NameFormat = "RowProcessing<LinqPad>",
                        MaxDegreeOfParallelism = 5,
                        BoundedCapacity = 5
                    },
                    ProcessingRecordBlockActionOptions = new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
                    {
                        SingleProducerConstrained = true,
                        CancellationToken = cancellationSource.Token,
                        NameFormat = "BlockProcessing<linqpad>",
                        MaxDegreeOfParallelism = 5,
                        BoundedCapacity = 5
                    },
                    ThrottleSleepInMS = 250,
                    AsyncBoundedCapacityReEnableSizeTrigger = 80,
                    //Logger = Logger,
                    LoggingTag = "LinqPad Person",
                    ThrftRowTreatedasRecord = true,
                    PartitionKeyQuerySlicerCollection = new List<string>() { "2|201403251400" },
                    EnablePartitionKeyQuerySlicer = true
                };
                
                auditRepsoitory.OnException += OnException;
                auditRepsoitory.OnRecordBlockProcessing += OnRecordBlockProcessing;
                auditRepsoitory.OnEachRecord += OnProcessRecord;

				auditRepsoitory.StartRowProcessing();
	}
}

void OnProcessRecord(object sender, SourceRepository.ProcessRepository.ProcessEachRecordEventArgs eventArgs)
{
	eventArgs.Dump();
}
	
void OnRecordBlockProcessing(object sender, SourceRepository.ProcessRepository.ProcessRecordBlockEventArgs eventArgs)
{
}
	
void OnException(object sender, SourceRepository.ProcessRepository.ExceptionEventArgs eventArgs)
{
	eventArgs.Dump();
}