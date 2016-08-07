<Query Kind="Program">
  <NuGetReference>CassandraCSharpDriver</NuGetReference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
  <Namespace>System.Security.Cryptography.X509Certificates</Namespace>
  <Namespace>System.Net.Security</Namespace>
</Query>

void Main()
{
	var qryConnectionString = "Contact Points=192.168.126.132"; //10.200.241.2"; //"Contact Points=192.168.190.132"; Contact Points=127.0.0.1 WIN-DCE12X-3M 192.168.126.131
	var cqlString = "select * from v2_readonly.document limit 1"; 
	
	var certs = new X509CertificateCollection();
	
	//Add the C* node certificate(s) plus their password. If there were more than one certificate they can be added here!
    certs.Add(new X509Certificate(@"C:\Users\richard.andersen\Desktop\3Meprscassandra.cer", "cassandra"));
	
  RemoteCertificateValidationCallback callback = (s, cert, chain, policyErrors) =>
  {
		//policyErrors.Dump(1);
		//cert.Dump(1);
		//chain.Dump(1);
		//s.Dump(1);
		
		//A "real" certificate was given!
		if (policyErrors == SslPolicyErrors.None)
		{
			return true; 
		}
		
		//Since this is a "Global" and locally crated certificate the CN names will not match and the certificate chaining will be incorrect
		//Policy Errors will be RemoteCertificateNameMismatch and RemoteCertificateChainErrors
		//Can add some additional checking like the cert.IssuerName, etc.
		if (policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateChainErrors) && 
			chain.ChainStatus.Length == 1 && 
			chain.ChainStatus[0].Status == X509ChainStatusFlags.UntrustedRoot)
		{
			//Console.WriteLine("True");
			return true;
		}
		
		//Recommend to log this since the driver will throw an Host Not Avaiable Exception on these
		return false;
  };
  var sslOptions = new SSLOptions().SetCertificateCollection(certs).SetRemoteCertValidationCallback(callback);
	
	using (var qryCluster = Cluster
							.Builder()
							.WithConnectionString(qryConnectionString)
							.WithSSL(sslOptions)
							.Build())
	using (var qrySession = qryCluster.Connect())
	using(var rowSet = qrySession.Execute(cqlString))
	{
		var rows = from row in rowSet
					select row;
		rows.Dump(1);
		
		rows.Dump(1);
	}
	
}

void Metadata_SameTargetSourceHostsEvent(object sender, HostsEventArgs e)
{
      switch (e.What)
      {
            case HostsEventArgs.Kind.Down:
                   Console.WriteLine("Host \"{0}\" is reporting to be DOWN within Cluster \"{1}\"",
                                                      e.IPAddress,
                                                      ((Cluster) sender).Metadata.ClusterName);
                   break;
            case HostsEventArgs.Kind.Up:
                   Console.WriteLine("Host \"{0}\" is reported as just coming UP within Cluster \"{1}\"",
                                                      e.IPAddress,
                                                      ((Cluster) sender).Metadata.ClusterName);
                   break;
            default:
                   break;
      }
}