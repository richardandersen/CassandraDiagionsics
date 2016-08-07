<Query Kind="Program">
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\DataStax-Perf-Test\bin\Release\Cassandra.Data.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\DataStax-Perf-Test\bin\Release\Cassandra.Data.Linq.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\DataStax-Perf-Test\bin\Release\Cassandra.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\DataStax-Perf-Test\bin\Release\Crc32C.NET.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\DataStax-Perf-Test\bin\Release\LZ4.dll</Reference>
  <Reference>D:\Projects\DataStax\Projects\3M\EPRS\Performance\DataStax-Perf-Test\bin\Release\Snappy.NET.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=10.200.240.251";// "Contact Points=192.168.117.128";//"Contact Points=127.0.0.1"; WIN-DCE12X-3M
	var dataCenter = "DC1";

	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.WithQueryOptions((new QueryOptions()).SetConsistencyLevel(ConsistencyLevel.All).SetPageSize(150))
							.WithLoadBalancingPolicy(string.IsNullOrEmpty(dataCenter)
														? Cassandra.Policies.DefaultLoadBalancingPolicy
														: new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenter)))
							.WithCompression(CompressionType.Snappy)
							.Build())
	using (var session = cluster.Connect())
	{
		var toV1 = session.Prepare("insert into v1_readonly.\"Document\" (key, column1, value) values(textasblob(?), textasblob(?), textasblob(?));")
						.SetConsistencyLevel(ConsistencyLevel.All);
		
		session.Execute("truncate v1_readonly.\"Document\";");
		
		var fromV2Table = session.GetTable<Document>(null, "v2_readonly")
							.SetConsistencyLevel(ConsistencyLevel.One);
				
		var fromV2 = from row in fromV2Table
						select row;
		
		foreach (var document in fromV2.Execute())
		{
			 var tasks = new Task[] {
                            session.ExecuteAsync(toV1.Bind(document.GetIdBlob(), 
                                                                    System.Text.UTF8Encoding.UTF8.GetBytes("DateStamp"),
                                                                    document.GetLastUpdateBlob())),
                            session.ExecuteAsync(toV1.Bind(document.GetIdBlob(), 
                                                                    System.Text.UTF8Encoding.UTF8.GetBytes("EprsVersion"),
                                                                    document.GetEprsVersionNumberBlob())),
                            session.ExecuteAsync(toV1.Bind(document.GetIdBlob(), 
                                                                    System.Text.UTF8Encoding.UTF8.GetBytes("ExternalDocId"),
                                                                    document.GetExternalDocIdBlob())),
                            session.ExecuteAsync(toV1.Bind(document.GetIdBlob(), 
                                                                    System.Text.UTF8Encoding.UTF8.GetBytes("RecordType"),
                                                                    document.GetRecordTypeBlob())),
                            session.ExecuteAsync(toV1.Bind(document.GetIdBlob(), 
                                                                    System.Text.UTF8Encoding.UTF8.GetBytes("Status"),
                                                                    document.GetStatusBlob())),
                            session.ExecuteAsync(toV1.Bind(document.GetIdBlob(), 
                                                                    System.Text.UTF8Encoding.UTF8.GetBytes("XmlRecord"), 
                                                                    document.GetXmlBlob()))
                        };
						
			Task.WaitAll(tasks);
		}
	}
}

[Cassandra.Data.Linq.Table("document")]
public class Document
{
  public Document() {}

  [Cassandra.Data.Linq.PartitionKey]
  public string  Id { get; set; }
  [Cassandra.Data.Linq.SecondaryIndex]
  public int EprsVersionNumber { get; set; }
  [Cassandra.Data.Linq.Column]
  public DateTime  CreateDateStamp { get; set; }
  [Cassandra.Data.Linq.Column]
  public string EncounterId { get; set; }
  [Cassandra.Data.Linq.Column]
  public string ExternalDocId { get; set; }
  [Cassandra.Data.Linq.Column]
  public DateTime LastUpdate { get; set; }
  [Cassandra.Data.Linq.Column]
  public string PersonId { get; set; }
  [Cassandra.Data.Linq.Column]
  public string RecordType { get; set; }
  [Cassandra.Data.Linq.Column]
  public string Status { get; set; }
  [Cassandra.Data.Linq.Column]
  public string XmlRecord
  {
      get;
	  set;
  }

  private byte[] _xmlBlob = null;

  public byte[] GetXmlBlob() { return this._xmlBlob == null ?  (this.XmlRecord == null ? null : this._xmlBlob = System.Text.UTF8Encoding.UTF8.GetBytes(this.XmlRecord)) : this._xmlBlob; }
  public byte[] GetIdBlob() { return System.Text.UTF8Encoding.UTF8.GetBytes(this.Id); }
  public byte[] GetLastUpdateBlob() { return System.Text.UTF8Encoding.UTF8.GetBytes(this.LastUpdate.ToString("yyyy-MM-dd HH:mm:ss")); }
  public byte[] GetEprsVersionNumberBlob() { return System.Text.UTF8Encoding.UTF8.GetBytes(this.EprsVersionNumber.ToString()); }
  public byte[] GetExternalDocIdBlob() { return System.Text.UTF8Encoding.UTF8.GetBytes(this.ExternalDocId); }
  public byte[] GetRecordTypeBlob() { return System.Text.UTF8Encoding.UTF8.GetBytes(this.RecordType); }
  public byte[] GetStatusBlob() { return System.Text.UTF8Encoding.UTF8.GetBytes(this.Status); }

  public override string ToString()
  {
      return string.Format("Document< <{0}>, <{1}>, <{2:yyyy-MM-dd HH:mm:ss.fff}>, <XmlLen {3:#,###,###,###,##0}>",
                              this.GetHashCode(),
                              this.Id,
                              this.CreateDateStamp,
                              this._xmlBlob == null ? 0 : this._xmlBlob.Length);
  }
}