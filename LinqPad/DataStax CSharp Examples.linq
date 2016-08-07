<Query Kind="Program">
  <Reference Relative="Cassandra.Data.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.dll</Reference>
  <Reference Relative="Cassandra.Data.Linq.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.Data.Linq.dll</Reference>
  <Reference Relative="Cassandra.dll">&lt;MyDocuments&gt;\LINQPad Queries\Cassandra.dll</Reference>
  <Reference Relative="LZ4.dll">&lt;MyDocuments&gt;\LINQPad Queries\LZ4.dll</Reference>
  <Namespace>Cassandra</Namespace>
  <Namespace>Cassandra.Data</Namespace>
  <Namespace>Cassandra.Data.Linq</Namespace>
</Query>

void Main()
{
	var connectionString = "Contact Points=192.168.190.134";
	int pageSize = 100;
	var dataCenterName = "DC1";
	var transportCompressionType = CompressionType.LZ4;
	var defaultConsistencyLevel = ConsistencyLevel.LocalQuorum;
	
	using (var cluster = Cluster
							.Builder()
							.WithConnectionString(connectionString)
							.WithoutRowSetBuffering()
							.WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenterName)))
							.WithQueryOptions((new QueryOptions()).SetPageSize(pageSize).SetConsistencyLevel(defaultConsistencyLevel))
							.WithCompression(transportCompressionType)
							.Build())
	using (var session = cluster.Connect())
	{
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
				Console.WriteLine("Retrieve Existing Rows");
				(from row in table select row).Execute().Dump(); //Using the LinqPad Dump method!!!

				//Update Year for Pulp Fiction
                var testmovie = new NerdMovie { Year = 2005, Director = "Quentin Tarantino", Movie = "Pulp Fiction", Maker = "Pixar" };
                table.Where(m => m.Movie == testmovie.Movie && m.Maker==testmovie.Maker && m.Director == testmovie.Director).Select(m => new NerdMovie { Year = testmovie.Year }).Update().Execute();

				Console.WriteLine("Updated Year (from 1994 to 2005) on Pulp Fiction based on Movie and Maker (primary keys), and Director (cluster key)");
				(from row in table select row).Execute().Dump(); //Using the LinqPad Dump method!!!

				//Update MainActor for Pulp Fiction
                var anonMovie = new { Director = "Quentin Tarantino", Year = 2005 };
                table.Where(m => m.Movie == "Pulp Fiction" && m.Maker == "Pixar" && m.Director == anonMovie.Director).Select(m => new NerdMovie { Year = anonMovie.Year, MainActor = "George Clooney" }).Update().Execute();

				Console.WriteLine("All rows which includes an update to Pulp Fiction (year from 1994 to 2005)");
				var all = (from row in table select row).Execute().ToList();
				all.Dump(); //Using the LinqPad Dump method!!!

                var all2 = table.Where((m) => CqlToken.Create(m.Movie, m.Maker) > CqlToken.Create("Pulp Fiction", "Pixar")).Execute();
                
				Console.WriteLine("All Rows where greater than Pulp Fiction and Pixar (primary keys)");
				all2.Dump();
				
				Console.WriteLine("All Rows where greater than Pulp Fiction and Pixar (primary keys) (Linq)");
				(from m in table where CqlToken.Create(m.Movie, m.Maker) > CqlToken.Create("Pulp Fiction", "Pixar") select m).Execute().Dump();

                var nmT = (from m in table where m.Director == "Quentin Tarantino" select new ExtMovie { TheDirector = m.MainActor, Size=all.Count, TheMaker = m.Director }).Execute();
				
				Console.WriteLine("Rows for Director Quetin Tarantino using ExtMovie Class");
				nmT.Dump();
				
                var nm1 = (from m in table where m.Director == "Quentin Tarantino" select new { MA = m.MainActor, Z = 10, Y = m.Year }).Execute().ToList();

				Console.WriteLine("Rows for Director Quetin Tarantino using anonymous types");
				nm1.Dump();
				
                var nmX = (from m in table where m.Director == "Quentin Tarantino" select new { m.MainActor, Z = 10, m.Year }).Execute().ToList();

				Console.WriteLine("Rows for Director Quetin Tarantino using anonymous types (version 2)");
				nmX.Dump();
				
				//Update Pulp Fiction Year to 1994
                (from m in table where m.Movie.Equals("Pulp Fiction") && m.Maker.Equals("Pixar") && m.Director == "Quentin Tarantino" select new NerdMovie { Year = 2010 }).Update().Execute();

				Console.WriteLine("Updated Year (from 2005 to 2010) on Pulp Fiction based on Movie and Maker (primary keys), and Director (cluster key)");
				(from row in table select row).Execute().Dump(); //Using the LinqPad Dump method!!!

                table.Where((m) => m.Movie == "Pulp Fiction" && m.Maker == "Pixar" && m.Director == "Quentin Tarantino").Select((m) => new NerdMovie { Year = 1994 }).Update().Execute();

				Console.WriteLine("Updated Year (from 2010 to 1994) on Pulp Fiction based on Movie and Maker (primary keys), and Director (cluster key)");
				(from row in table select row).Execute().Dump(); //Using the LinqPad Dump method!!!

                var nm2 = table.Where((m) => m.Director == "Quentin Tarantino").Select((m) => new { MA = m.MainActor, Y = m.Year }).Execute().ToList();

				Console.WriteLine("Get movies by Director Quentin Tarantino and only return the Actor and Year");
				nm2.Dump();
				
				//Delete Pulp Fiction
				Console.WriteLine("Pulp Fiction");
                (from m in table where m.Movie == "Pulp Fiction" && m.Maker == "Pixar" && m.Director == "Quentin Tarantino" select m).Delete().Execute();

                var nm3 = (from m in table where m.Director == "Quentin Tarantino" select new { MA = m.MainActor, Y = m.Year }).Execute().ToList();
				Console.WriteLine("Get movies by Director Quentin Tarantino and only return the Actor and Year");
				nm3.Dump();
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

public class ExtMovie
{
  public string TheDirector;
  public int Size;
  public string TheMaker;
}