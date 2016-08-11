<Query Kind="Program">
  <Reference>C:\bin\Common.Functions.dll</Reference>
  <Reference>C:\bin\Common.Patterns.Shared.dll</Reference>
  <Reference>C:\bin\Common.Patterns.Singleton.dll</Reference>
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <NuGetReference>SSH.NET</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>Common</Namespace>
  <Namespace>Common.Patterns</Namespace>
  <Namespace>Renci.SshNet</Namespace>
  <Namespace>Renci.SshNet.Common</Namespace>
  <Namespace>Renci.SshNet.Compression</Namespace>
  <Namespace>Renci.SshNet.Messages</Namespace>
  <Namespace>Renci.SshNet.Messages.Authentication</Namespace>
  <Namespace>Renci.SshNet.Messages.Connection</Namespace>
  <Namespace>Renci.SshNet.Messages.Transport</Namespace>
  <Namespace>Renci.SshNet.Security</Namespace>
  <Namespace>Renci.SshNet.Security.Cryptography</Namespace>
  <Namespace>Renci.SshNet.Security.Cryptography.Ciphers</Namespace>
  <Namespace>Renci.SshNet.Security.Cryptography.Ciphers.Modes</Namespace>
  <Namespace>Renci.SshNet.Security.Cryptography.Ciphers.Paddings</Namespace>
  <Namespace>Renci.SshNet.Sftp</Namespace>
  <Namespace>System.Net.Security</Namespace>
  <Namespace>System.Security.Cryptography.X509Certificates</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.247.133,192.168.247.61"; //Change IP Address or a list of ip addresses seperated by a comma
	var localLocation = @"C:\Projects\DataStax\Training50\Files";

	var sshNodeUserName = "root";
	var sshNodeUserPasswod = string.Empty; //If null private keys are used
	var sshPrivateKeyFiles = new string[] { @"C:\Projects\DataStax\bootcamp\SSH Instructions and Keys\classkeySearch" };

	//var dataCenterName = "nearby"; //Change to DC Name

	//Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;	

	AuthenticationMethod sshAuthenticationMethod;

	if (sshNodeUserPasswod == null)
	{
		sshAuthenticationMethod = new PrivateKeyAuthenticationMethod("root", sshPrivateKeyFiles.Select(pkf => new PrivateKeyFile(pkf)).ToArray());
	}
	else
	{
		sshAuthenticationMethod = new PasswordAuthenticationMethod(sshNodeUserName, sshNodeUserPasswod);
	}
	
	var connectionInfoS = new ConnectionInfo("192.168.247.133",
													sshNodeUserName,
											 		sshAuthenticationMethod);
																									
	FileUpload(connectionInfoS, "Node2-v211", @"/etc/dse/cassandra", localLocation, "cassandra.yaml", true);
	FileUpload(connectionInfoS, "Node2-v211", @"/etc/dse", localLocation, "dse.yaml", true);
	FileUpload(connectionInfoS, "Node2-v211", @"/etc/dse/cassandra", localLocation, "cassandra-env.sh", true);
	
	using (var cluster = Cluster
							.Builder()
							//.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()							
							//.WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.Quorum))
							.Build())
	{
		foreach (var host in cluster.Metadata.AllHosts())
        {
			var hostInfo = new { DC = host.Datacenter, Address = host.Address.ToString(), CassandraVersion = host.CassandraVersion.ToString() };
			hostInfo.Dump();
			
//			var connectionInfo = new ConnectionInfo(host.Address.Address.ToString(),
//													"root",
//											 		new PrivateKeyAuthenticationMethod("root", new PrivateKeyFile[]{
//																									new PrivateKeyFile(sshPrivateKeyFile)}));
																									
			//FileUpload(connectionInfo, host.Datacenter, @"/etc/cassandra", localLocation, "cassandra.yaml", true);
			//FileDownload(connectionInfo, host.Datacenter, @"/etc/cassandra", localLocation, "cassandra-env.sh");
			//FileDownload(connectionInfo, host.Datacenter, @"/etc/dse/spark", localLocation, "logback-spark-executor.xml");
			
        }
		
		foreach (var element in cluster.Metadata.GetKeyspaces())
		{
			DescribeKeySpaces(cluster, element);	
		}
	}		
}

public void FileUpload(ConnectionInfo connectionInfo, 
						string dataCenter,
						string rmtLocation,
						string localLocation,
						string cassandraFile,
						bool removeBackupFile)
{
	string ipAdress = connectionInfo.Host;

	Console.WriteLine(@"Uploading {0}/{1} for DataCenter {2} Address {3}", rmtLocation, cassandraFile, dataCenter, ipAdress);
	
    using (var sshClient = new SshClient(connectionInfo))
	{
		string cmdStr;

		try
		{
			sshClient.Connect();

			if (removeBackupFile)
			{
				sshClient.CreateCommand(string.Format(@"rm {0}/{1}_*", rmtLocation, cassandraFile)).Execute().Dump();
			}

			if (!string.IsNullOrEmpty(cmdStr = sshClient.CreateCommand(string.Format(@"cp {0}/{1} {0}/{1}_{2:yyMMddHHmmss}", rmtLocation, cassandraFile, DateTime.Now)).Execute()))
			{
				Console.WriteLine("Skipping {0} due to Error of {1}", ipAdress, cmdStr);
				return;
			}
		}
        catch (Exception ex)
		{
			ex.Dump();
			Console.WriteLine("*** ABORTED: {0}",
								string.Format(@"rm {0}/{1}_*", rmtLocation, cassandraFile));
		}
	}

	using (var sftpClient = new SftpClient(connectionInfo))
	{
		try
		{
			sftpClient.Connect();


			using (Stream file1 = File.OpenRead(string.Format(@"{0}\{1}\{2}\{3}-{1}",
																localLocation,
																cassandraFile,
																dataCenter,
																ipAdress)))
			{
				sftpClient.UploadFile(file1, string.Format(@"{0}/{1}", rmtLocation, cassandraFile), true);
			}
		}
        catch (Exception ex)
		{
			ex.Dump();
			Console.WriteLine(@"**ABORTED: Uploading {0}/{1} for DataCenter {2} Address {3} from {4}",
								rmtLocation,
								cassandraFile,
								dataCenter,
								ipAdress,
								string.Format(@"{0}\{1}\{2}\{3}-{1}",
												localLocation,
												cassandraFile,
												dataCenter,
												ipAdress));
		}
	}
}

public void FileDownload(ConnectionInfo connectionInfo, 
							string dataCenter,
							string rmtLocation,
							string localLocation,
							string cassandraFile)
{
	string ipAdress = connectionInfo.Host;

	Console.WriteLine("Downloading {0}/{1} for DataCenter {2} Address {3}", rmtLocation, cassandraFile, dataCenter, ipAdress);

	using (var sshClient = new SftpClient(connectionInfo))
	{
		try
		{	        
			sshClient.Connect();
	
			System.IO.Directory.CreateDirectory(string.Format(@"{0}\{1}\{2}\",
																localLocation,
																cassandraFile,
																dataCenter));
	
			using (Stream file1 = File.OpenWrite(string.Format(@"{0}\{1}\{2}\{3}-{1}",
																localLocation,
																cassandraFile,
																dataCenter,
																ipAdress)))
			{
				sshClient.DownloadFile(string.Format("{0}/{1}", rmtLocation, cassandraFile), file1);
			}
		}
		catch (Exception ex)
		{
			ex.Dump();
			Console.WriteLine("*** ABORTED: Downloading of {0}/{1} for DataCenter {2} Address {3} to {4}",
								rmtLocation,
								cassandraFile,
								dataCenter,
								ipAdress,
								string.Format(@"{0}\{1}\{2}\{3}-{1}",
												localLocation,
												cassandraFile,
												dataCenter,
												ipAdress));
		}
	}
}



public void DescribeKeySpaces(Cluster cluster, string keySpace)
{
	//using (var session = cluster.Connect())
	{
		//session.Cluster.Metadata.GetKeyspace(keySpace).Dump();
		//cluster.Metadata.GetKeyspace(keySpace).Dump();
		foreach (var tblName in cluster.Metadata.GetTables(keySpace))
		{
			cluster.Metadata.GetTable(keySpace, tblName).Options.Dump();
        }
			//var rs = session.ex("describe keyspace keyspace1;");
			
			//rs.First().Dump();
	}
}