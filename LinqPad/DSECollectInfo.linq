<Query Kind="Program">
  <Reference>C:\bin\Common.Functions.dll</Reference>
  <Reference>C:\bin\Common.Path.dll</Reference>
  <Reference>C:\bin\Common.Patterns.Collections.dll</Reference>
  <Reference>C:\bin\Common.Patterns.Shared.dll</Reference>
  <Reference>C:\bin\Common.Patterns.Singleton.dll</Reference>
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <NuGetReference>SSH.NET</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>Common</Namespace>
  <Namespace>Renci.SshNet</Namespace>
</Query>

public class NodeInfo
{
	public string IPAddress;
	public string DCName;
	public string CassandraVersion;
	public string ClusterName;
	public bool StatusUp;
	public List<Exception> Exceptions = new List<Exception>();
}

static string CurrentDSEClusterName;
static string DefaultDSEDCName;

void Main()
{
	//Cassandra/DSE
	var useCassandraRing = true;
	var getCQLDDL = true;
	var dseConnectionString = "Contact Points=192.168.247.60"; //Change IP Address or a list of ip addresses seperated by a comma
	string dseUserName = null;
	string dseUserPassword = null;
	string dseUseThisDataCenter = null;
	var excludeCQLDDLKeySpaces = new string[] { "dse_system",
												"system_auth",
												"system_traces",
												"system",
												"dse_perf",
												"OpsCenter" };

	// path's placeholders
	// {0} -- Nodes's IP-Address
	// {1} -- DSE Datacenter's name (only if conected to DSE; otherwise it will be the default DataCenter Name)
	// {2} -- Cluster Name (only if conected to DSE; otherwise it will be the default Cluster Name)
	// {3} -- cqlsh describe command
	var cqlDDLLocalPath = @"C:\Projects\DataStax\{2}\nodes\{3}\describe-schema.cql";	
	var shCQLDescribe = "cqlsh -e '{3}' -u {1} -p {2} {0}"; //e.g., echo -e 'describe keyspace "keyspace1"' | cqlsh 192.168.247.61
	
	// First Item is the localpath and the second is the remote file to copy on the node...
	// path's placeholders
	// {0} -- Nodes's IP-Address
	// {1} -- DSE Datacenter's name (only if conected to DSE; otherwise it will be the default DataCenter Name)
	// {2} -- Cluster Name (only if conected to DSE; otherwise it will be the default Cluster Name)
	// {3} -- Node's target file (e.g., cassandra.yaml)
	// {4} -- Node's target file and the file's parrent directory (e.g., cassandra/cassandra.yaml)
	// TBD used a wild card in target and map into local.
	var dseConfigFiles = new Tuple<string, string>[] {new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\conf\{4}", @"/etc/dse/cassandra/cassandra.yaml"),
														new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\conf\{4}",@"/etc/dse/cassandra/cassandra-env.sh"),
														new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\log\{4}",@"/etc/dse/cassandra/system.log"),
														new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\log\{4}",@"/etc/dse/cassandra/output.log") };

	//First item is the local path and the second is the Linux node command and arguments
	// Command's placeholders are:
	// {0} -- Nodes's IP-Address
	// {1} -- DSE User Name
	// {2} -- DSE User's Password
	// {3} -- Runtime arguments
	//
	// local path's placeholders
	// {0} -- Nodes's IP-Address
	// {1} -- DSE Datacenter's name (only if conected to DSE; otherwise it will be the default DataCenter Name)
	// {2} -- Cluster Name (only if conected to DSE; otherwise it will be the default Cluster Name)
	// {3} -- Linux node command
	// {4} -- Linux node command's Sub-command (ignore command arguments that begin with "-")
	var dseCommands = new Tuple<string, string>[] { new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\{3}\{4}",
																				@"nodetool -u {1} -pw {2} ring"),
													new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\{3}\{4}",
																				@"nodetool -u {1} -pw {2} cfstats"),
													new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\{3}\{4}",
																				@"nodetool -u {1} -pw {2} info"),
													new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\{3}\{4}",
																				@"nodetool -u {1} -pw {2} tpstats"),
													new Tuple<string,string>(@"C:\Projects\DataStax\{2}\nodes\{0}\{3}\{4}",
																				@"nodetool -u {1} -pw {2} compactionhistory"),
														};

	//If not using the Cassandra Data Center ring node IP addresses, this list will be used instead.
	var remoteHostList = new List<string>() { };
	DefaultDSEDCName = "DCN"; //used as the default name
	CurrentDSEClusterName = "CN"; //set the default cluster name (will be overridden if connected to DSE)

	//SSH
	var sshNodeUserName = "randersen";
	var sshNodeUserPasswod = "ymRA3630"; //If null private keys are used
	var sshPrivateKeyFiles = new string[] { @"C:\Projects\DataStax\BootCamp\SSH Instructions and Keys\classkeySearch" };


	var nodeInfoList = new List<NodeInfo>();
	AuthenticationMethod sshAuthenticationMethod;

	if(string.IsNullOrEmpty(sshNodeUserPasswod))
    {
		sshAuthenticationMethod = new PrivateKeyAuthenticationMethod("root", sshPrivateKeyFiles.Select(pkf => new PrivateKeyFile(pkf)).ToArray());
	}
	else
	{
		sshAuthenticationMethod = new PasswordAuthenticationMethod(sshNodeUserName, sshNodeUserPasswod);
	}
	
	if (useCassandraRing)
	{
		GetNodeInfoFromDSE(dseUseThisDataCenter,
							dseConnectionString,
							dseUserName,
							dseUserPassword,
							excludeCQLDDLKeySpaces,
							getCQLDDL ? shCQLDescribe : null,
							cqlDDLLocalPath,
							nodeInfoList,
							sshAuthenticationMethod);
    }
	else
	{
		foreach (var ipAddress in remoteHostList)
		{
			nodeInfoList.Add(new NodeInfo() { IPAddress = ipAddress, DCName = DefaultDSEDCName, ClusterName = CurrentDSEClusterName });
		}
	}
	
	foreach (var nodeItem in nodeInfoList)
	{
		foreach (var copyFiles in dseConfigFiles)
		{
			var remoteFile = UpdatePathPlaceHolders(copyFiles.Item2, nodeItem);
			var remoteDirectories = StringFunctions.Split(remoteFile, '/');
			var remoteFileName = remoteDirectories.Last();
			var remoteParentFile = remoteDirectories[remoteDirectories.Count - 2] + "/" + remoteFileName;
			var localFile = Common.File.BaseFile.Make(UpdatePathPlaceHolders(copyFiles.Item1, nodeItem, remoteFileName, remoteParentFile));
			
			if (localFile.Exist())
			{
				localFile.Truncate();
			}
			else
			{
				localFile.Create();
			}
			
			FileDownload(nodeItem, localFile, remoteFile, sshAuthenticationMethod);
		}
		
		foreach (var sshCmd in dseCommands)
		{
			var dseCmdPath = sshCmd.Item2;
			var dseCmdArgs = StringFunctions.Split(dseCmdPath, ' ');
			var dseCmd = Path.GetFileName(dseCmdArgs.First());
			var dseSubCmd = dseCmdArgs.Last();
			var localFile = Common.File.BaseFile.Make(UpdatePathPlaceHolders(sshCmd.Item1, nodeItem, dseCmd, dseSubCmd));

			if (localFile.Exist())
			{
				localFile.Truncate();
			}
			else
			{
				localFile.Create();
			}

			using (var file1 = localFile.OpenTextWriter())
			{
				var dseCmdOutput = SSHRunCommand(dseCmdPath, null, dseUserName, dseUserPassword, nodeItem, sshAuthenticationMethod);
				file1.Write(dseCmdOutput);
			}
		}
	}

}

// path's placeholders
// {0} -- Nodes's IP-Address
// {1} -- DSE Datacenter's name (only if conected to DSE; otherwise it will be the default DataCenter Name)
// {2} -- Cluster Name (only if conected to DSE; otherwise it will be the default Cluster Name)
// {3} -- udt1
// {4} -- udt2
string UpdatePathPlaceHolders(string path, NodeInfo nodeInfo, string udt1 = null, string udt2 = null)
{
	return string.Format(path, nodeInfo.IPAddress, nodeInfo.DCName, nodeInfo.ClusterName, udt1, udt2);
}

Builder AddCredentials(Builder clusterBuilder, string userName, string password, string useThisDataCenter)
{
	if (!string.IsNullOrEmpty(userName))
	{
		clusterBuilder = clusterBuilder.WithCredentials(userName, password);
	}

	if (!string.IsNullOrEmpty(useThisDataCenter))
	{
		clusterBuilder = clusterBuilder.WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy(useThisDataCenter));
    }

	return clusterBuilder;
}


void GetNodeInfoFromDSE(string dseUseThisDataCenter,
						string dseConnectionString,
						string dseUserName,
						string dseUserPassword,
						string[] excludeCQLDDLKeySpaces,
						string shCQLDescribe,
						string cqlDDLLocalFilePath,
						List<NodeInfo> nodeInfoList,
						AuthenticationMethod sshAuthenticationMethod)
{
	Console.Write("DSE Connecting using connection string \"{0}\"", dseConnectionString);
	
	using (var dseCluster = AddCredentials(Cluster
											.Builder()											
											.WithConnectionString(dseConnectionString)
											.WithoutRowSetBuffering()
											.WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One))
											.WithCompression(CompressionType.LZ4),
											dseUserName,
											dseUserPassword,
											dseUseThisDataCenter)
											.Build())
	{
		NodeInfo sshCQLDDLNode = null;
		NodeInfo currentNode = null;
		
		CurrentDSEClusterName = dseCluster.Metadata.ClusterName;

		Console.Write("\tConnected to Cluster \"{0}\"", CurrentDSEClusterName);
		Console.WriteLine();
		
		foreach (var dseNode in dseCluster.Metadata.AllHosts())
		{
			nodeInfoList.Add(currentNode = new NodeInfo() { IPAddress = dseNode.Address.Address.ToString(),
															DCName = dseNode.Datacenter,
															CassandraVersion = dseNode.CassandraVersion.ToString(),
															ClusterName = dseCluster.Metadata.ClusterName,
															StatusUp = dseNode.IsUp});

			if (sshCQLDDLNode == null && dseConnectionString.Contains(currentNode.IPAddress))
			{
				sshCQLDDLNode = currentNode;
			}
		}

		if (!string.IsNullOrEmpty(shCQLDescribe) && !string.IsNullOrEmpty(cqlDDLLocalFilePath) && sshCQLDDLNode != null)
		{
			var ifilepathCQLDDL = Common.File.BaseFile.Make(UpdatePathPlaceHolders(cqlDDLLocalFilePath, sshCQLDDLNode, "cqlsh"));

			if (ifilepathCQLDDL.Exist())
			{
				ifilepathCQLDDL.Truncate();
			}
			else
			{
				ifilepathCQLDDL.Create();	
			}

			Console.WriteLine("Obtaining CQL DDL from Cluster \"{0}\" using DSE Node {1} to Local Path \"{2}\"",
								sshCQLDDLNode.ClusterName,
								sshCQLDDLNode.IPAddress,
								ifilepathCQLDDL.PathResolved);

			using (var ddlWriteToFile = ifilepathCQLDDL.OpenTextWriter())
			{
				ddlWriteToFile.WriteLine("//ClusterName: '{0}' DataCenters: {1} DDL Node: {2}",
											sshCQLDDLNode.ClusterName,
											string.Join(", ", nodeInfoList.Select(item => item.DCName).DuplicatesRemoved(item => item)),
											sshCQLDDLNode.IPAddress);
				ddlWriteToFile.WriteLine();
				
				var describekeySpaceNames = new StringBuilder();
				
				foreach (var keySpace in dseCluster.Metadata.GetKeyspaces())
				{
					if (!excludeCQLDDLKeySpaces.Contains(keySpace))
                    {
						describekeySpaceNames.AppendFormat("describe keyspace \"{0}\"", keySpace);
					}
				}
				
				
				var cqlDDL = SSHRunCommand(shCQLDescribe,
											describekeySpaceNames.ToString(),
											dseUserName,
											dseUserPassword,
											sshCQLDDLNode,
											sshAuthenticationMethod);

				if (string.IsNullOrEmpty(cqlDDL))
				{
					Console.WriteLine("Warning: No CQL DDL was returned. This indicates sometype of failure");
				}
				
				ddlWriteToFile.WriteLine(cqlDDL);
			}
		}
	}
}

string SSHRunCommand(string shCommand,
						string shRTArguments,
						string dseUserName,
						string dseUserPassword,
						NodeInfo nodeInfo,
						AuthenticationMethod sshAuthenticationMethod)
{
	var sshConnectionInfo = new ConnectionInfo(nodeInfo.IPAddress,
												sshAuthenticationMethod.Username,
										 		sshAuthenticationMethod);

	using (var sshClient = new SshClient(sshConnectionInfo))
	{
		sshClient.Connect();

		var shCmd = string.Format(shCommand,
									nodeInfo.IPAddress,
									string.IsNullOrEmpty(dseUserName) ? "cassandra" : dseUserName,
									string.IsNullOrEmpty(dseUserPassword) ? "cassandra" : dseUserPassword,
									shRTArguments);
									
        return sshClient.RunCommand(shCmd).Execute();
	}

}

public void FileDownload(NodeInfo nodeInfo,
							IFilePath localFile,
							string remoteFile,
							AuthenticationMethod sshAuthenticationMethod)
{

	Console.WriteLine("Downloading \"{0}\" for Cluster \"{1}\", DataCenter \"{2}\", Address {3}",
						Path.GetFileName(remoteFile),
						nodeInfo.ClusterName,
						nodeInfo.DCName,
						nodeInfo.IPAddress);
						
	var sshConnectionInfo = new ConnectionInfo(nodeInfo.IPAddress,
												sshAuthenticationMethod.Username,
										 		sshAuthenticationMethod);
	

	using (var sshClient = new SftpClient(sshConnectionInfo))
	{
		try
		{
			sshClient.Connect();

			using (Stream file1 = localFile.OpenWrite())
			{
				sshClient.DownloadFile(remoteFile, file1);
			}
		}
		catch (Exception ex)
		{
			ex.Dump();
			Console.WriteLine("*** ABORTED: Downloading of \"{0}\" for Cluster \"{1}\", DataCenter \"{2}\", Address {3} to \"{4}\"",
								remoteFile,
								nodeInfo.ClusterName,
								nodeInfo.DCName,
								nodeInfo.IPAddress,
								localFile.PathResolved);
								
			nodeInfo.Exceptions.Add(ex);
		}
	}
}