<Query Kind="Program">
  <Reference>C:\bin\Common.Functions.dll</Reference>
  <Reference>C:\bin\Common.Path.dll</Reference>
  <Reference>C:\bin\Common.Patterns.Collections.dll</Reference>
  <Reference>C:\bin\Common.Patterns.Shared.dll</Reference>
  <Reference>C:\bin\Common.Patterns.Singleton.dll</Reference>
  <NuGetReference>EPPlus</NuGetReference>
  <Namespace>Common</Namespace>
  <Namespace>OfficeOpenXml</Namespace>
  <Namespace>System.Data</Namespace>
  <Namespace>System.Threading</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>

//
// DSEParseDiagFiles
// Copyright (C) 2016 Richard H. Andersen Jr.  All rights reserved.
//
// 
//     DSEParseDiagFiles is free software: you can redistribute it and/or modify
//     it under the terms of the GNU Lesser General Public License as published by
//     the Free Software Foundation, either version 3 of the License, or
//     (at your option) any later version.
// 
//     DSEParseDiagFiles is distributed in the hope that it will be useful,
//     but WITHOUT ANY WARRANTY; without even the implied warranty of
//     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//     GNU Lesser General Public License for more details.
// 
//     GNU Lesser General Public License can be reviewed at <http://www.gnu.org/licenses/>.

//This uses EPPlus library (http://epplus.codeplex.com/) which can be downloaded from NuGet and "Common Pattern" library written by Richard Andersen which are included with this application.

const int MaxRowInExcelWorkSheet = 50000; //-1 disabled
const int MaxRowInExcelWorkBook = 500000; //-1 disabled
const int GCPausedFlagThresholdInMS = 5000; //Defines a threshold that will flag a log entry in both the log summary (only if GCInspector.java) and log worksheets
static TimeSpan LogTimeSpanRange = new TimeSpan(2, 0, 0, 0); //Only import log entries for the past timespan (e.g., the last 5 days) based on LogCurrentDate.
static DateTime LogCurrentDate = DateTime.MinValue; //DateTime.Now.Date; //If DateTime.MinValue all log entries are parsed
static int LogMaxRowsPerNode = -1; // -1 disabled
static string[] LogSummaryIndicatorType = new string[] { "WARN", "ERROR" };
static string[] LogSummaryTaskItems = new string[] { "SliceQueryFilter.java", "BatchStatement.java", "CompactionController.java", "HintedHandoffMetrics.java", "GCInspector.java" };
static string[] LogSummaryIgnoreTaskExceptions = new string[] { };
static Tuple<DateTime, TimeSpan>[] LogSummaryPeriods = null; //new Tuple<DateTime, TimeSpan>[] { new Tuple<DateTime,TimeSpan>(new DateTime(2016, 08, 02), new TimeSpan(0, 0, 30, 0)), //From By date/time and aggregation period
																						 //new Tuple<DateTime,TimeSpan>(new DateTime(2016, 08, 1, 0, 0, 0), new TimeSpan(0, 1, 0, 0)),
																						 //new Tuple<DateTime,TimeSpan>(new DateTime(2016, 07, 29, 0, 0, 0), new TimeSpan(1, 0, 0, 0))}; //null disable Summaries.
static Tuple<TimeSpan, TimeSpan>[] LogSummaryPeriodRanges = new Tuple<TimeSpan, TimeSpan>[] { new Tuple<TimeSpan,TimeSpan>(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 0, 15, 0)), //Timespan from Log's Max Date or prevous rang/Period time and aggregation period
																								new Tuple<TimeSpan,TimeSpan>(new TimeSpan(1, 0, 0, 0), new TimeSpan(1, 0, 0, 0)),																						 		
																								new Tuple<TimeSpan,TimeSpan>(new TimeSpan(4, 0, 0, 0), new TimeSpan(7, 0, 0, 0))}; //null disable Summaries.
																						 
//Creates a filter that is used for loading the Cassandra Log Worksheets
// Data Columns are:
//	[Data Center], string, AllowDBNull
//	[Node IPAdress], string
//	[Timestamp], DateTime
//	[Indicator], string (e.g., INFO, WARN, ERROR)
//	[Task], string (e.g., ReadStage, CompactionExecutor)
//	[Item], string (e.g., HintedHandoffMetrics.java, BatchStatement.java)
//	[Exception], string, AllowDBNull (e.g., java.io.IOException)
//	[Exception Description], string, AllowDBNull (e.g., "Caused by: java.io.IOException: Cannot proceed on repair because a neighbor (/10.27.34.54) is dead: session failed")
//	[Assocated Item], string, AllowDBNull (e.g., 10.27.34.54, <keyspace.tablename>)  
//	[Assocated Value], object, AllowDBNull (e.g., <size in MB>, <time in ms>)
//	[Description], string -- log's description
//	[Flagged], bool, AllowDBNull -- if true this log entry was flagged because it matched some criteria (e.g., GC Pauses -- GCInspector.java exceeds GCPausedFlagThresholdInMS)
static string LogExcelWorkbookFilter = string.Empty; //"[Timestamp] >= #2016-08-01#"; //if null no filter is used, if string.Empty the max log timestamp with LogTimeSpanRange is used. 
static bool LoadLogsIntoExcel = true;

void Main()
{
	#region Configuration
	//Location where this application will write or update the Excel file.
	var excelFilePath = @"[DeskTop]\Gamesys.xlsx"; //<==== Should be updated
	
	//If diagnosticNoSubFolders is false:
	//Directory where files are located to parse DSE diagnostics files produced by DataStax OpsCenter diagnostics or a special directory structure where DSE diagnostics information is placed.
	//If the "special" directory is used it must follow the following structure:
	// <MySpecialFolder> -- this is the location used for the diagnosticPath variable
	//    |- <DSENodeIPAddress> (the IPAdress must be located at the beginning or the end of the folder name) e.g., 10.0.0.1, 10.0.0.1-DC1, Diag-10.0.0.1
	//	  |       | - nodetool -- static folder name
	//	  |  	  |	     | - cfstats 	-- This must be the output file from nodetool cfstats (static name)
	//	  |  	  |		 | - ring		-- This must be the output file from nodetool ring (static name)
	//	  |		  |		 | - tpstats
	//	  |		  |		 | - info
	//	  |		  |		 | - compactionhistory
	//	  |  	  | - logs -- static folder name
	//	  |       | 	| - cassandra -- static folder name
	//	  |  				    | - system.log -- This must be the cassandra log file from the node
	//    | - <NextDSENodeIPAdress> -- e.g., 10.0.0.2, 10.0.0.2-DC1, Diag-10.0.0.2
	//
	//If diagnosticNoSubFolders is ture:
	//	All diagnostic files are located directly under diagnosticPath folder. Each file should have the IP Adress either in the beginning or end of the file name.
	//		e.g., cfstats_10.192.40.7, system-10.192.40.7.log, 10.192.40.7_system.log, etc.
	var diagnosticPath = @"[MyDocuments]\LINQPad Queries\DataStax\TestData\gamingactivity-diagnostics-2016_08_10_08_45_40_UTC"; //production_group_v_1-diagnostics-2016_07_04_15_43_48_UTC"; //@"C:\Users\richard\Desktop\datastax"; //<==== Should be Updated 
	var diagnosticNoSubFolders = false; //<==== Should be Updated 
	var parseLogs = true;
	var parseNonLogs = true;
	
	//Excel Workbook names
	var excelWorkSheetRingInfo = "Node Information";
	var excelWorkSheetRingTokenRanges = "Ring Token Ranges";
	var excelWorkSheetCFStats = "CFStats";
	var excelWorkSheetTPStats = "TPStats";
	var excelWorkSheetLogCassandra = "Cassandra Log";
	var excelWorkSheetDDLKeyspaces = "DDL Keyspaces";
	var excelWorkSheetDDLTables = "DDL Tables";
	var excelWorkSheetCompactionHist = "Compaction History";
	var excelWorkSheetYaml = "Settings-Yamls";
	var excelWorkSheetOSMachineInfo= "OS-Machine Info";
	var excelWorkSheetSummaryLogCassandra = "Cassandra Summary Logs";

	List<string> ignoreKeySpaces = new List<string>() { "dse_system", "system_auth", "system_traces", "system", "dse_perf"  }; //MUST BE IN LOWER CASe
	List<string> cfstatsCreateMBColumns = new List<string>() { "memory used", "bytes", "space used", "data size"}; //MUST BE IN LOWER CASE -- CFStats attributes that contains these phrases/words will convert their values from bytes to MB in a separate Excel Column

	//Static Directory/File names
	var diagNodeDir = "nodes";
	var nodetoolDir = "nodetool";
	var dseToolDir = "dsetool";
	var logsDir = "logs";
	var nodetoolRingFile = "ring";
	var dsetoolRingFile = "ring";
	var nodetoolCFStatsFile = "cfstats";
	var nodetoolTPStatsFile = "tpstats";
	var nodetoolInfoFile = "info";
	var nodetoolCompactionHistFile = "compactionhistory";
	var logCassandraDirSystemLog = @".\cassandra\system.log";
	var logCassandraSystemLogFile = "system";
	var confCassandraDir = @".\conf\cassandra";
	var confCassandraFile = "cassandra.yaml";
	var confCassandraType = "cassandra";
	var confDSEDir = @".\conf\dse";
	var confDSEYamlFile = "dse.yaml";
	var confDSEYamlType = "dse yaml";
	var confDSEType = "dse";
	var confDSEFile = "dse";
	var confCassandraYamlFileName = "cassandra";
	var confDSEFileName = "dse";
	var cqlDDLDirFile = @".\cqlsh\describe_schema";
	var cqlDDLDirFileExt = @"*.cql";
	var nodetoolCFHistogramsFile = "cfhistograms"; //this is based on keyspace and table and not sure of the format. HC doc has it as cfhistograms_keyspace_table.txt
	var osmachineFiles = new string[] { "java_heap.json",
										"java_system_properties.json",
										"machine-info.json",
										"os-info.json",
										@".\os-metrics\cpu.json",
										@".\os-metrics\load_avg.json",
										@".\os-metrics\memory.json",
										@".\ntp\ntpstat",
										@".\ntp\ntptime"};

	#endregion
	
	#region Local Variables
	
	//Local Variables used for processing
	bool opsCtrDiag = false;	
	var dtRingInfo = new System.Data.DataTable(excelWorkSheetRingInfo);
	var dtTokenRange = new System.Data.DataTable(excelWorkSheetRingTokenRanges);
	var dtKeySpace = new System.Data.DataTable(excelWorkSheetDDLKeyspaces);
	var dtTable = new System.Data.DataTable(excelWorkSheetDDLTables);
	var cqlHashCheck = new Dictionary < string, int >();
	var dtCFStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var dtTPStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var dtLogsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var dtLogSummaryStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var dtCompHistStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var listCYamlStack = new Common.Patterns.Collections.LockFree.Stack<List<YamlInfo>>();
	var dtYaml = new System.Data.DataTable(excelWorkSheetYaml);
	var dtOSMachineInfo = new System.Data.DataTable(excelWorkSheetOSMachineInfo);

	var includeLogEntriesAfterThisTimeFrame = LogCurrentDate == DateTime.MinValue ? DateTime.MinValue : LogCurrentDate - LogTimeSpanRange;
	
	#endregion
	
	#region Parsing Files
	
	if (includeLogEntriesAfterThisTimeFrame != DateTime.MinValue)
	{
		Console.WriteLine("Warning: Log Entries after \"{0}\" will only be parsed", includeLogEntriesAfterThisTimeFrame);
	}
	else
	{
		Console.WriteLine("Warning: All Log Entries willbe parsed!");
	}

	var diagPath = Common.Path.PathUtils.BuildDirectoryPath(diagnosticPath);
	var logParsingTasks = new Common.Patterns.Collections.ThreadSafe.List<Task>();

	if (diagnosticNoSubFolders)
	{
		#region Parse -- All Files in one Folder
		
		var diagChildren =  diagPath.Children();
		
		//Need to process nodetool ring files first
		var nodetoolRingChildFiles = diagChildren.Where(c => c is IFilePath && c.Name.Contains(nodetoolRingFile));

		if (parseNonLogs && nodetoolRingChildFiles.HasAtLeastOneElement())
		{
			foreach (var element in nodetoolRingChildFiles)
			{
				Console.WriteLine("Processing File \"{0}\"", element.Path);
				ReadRingFileParseIntoDataTables((IFilePath)element, dtRingInfo, dtTokenRange);
				element.MakeEmpty();
			}
		}

		nodetoolRingChildFiles = diagChildren.Where(c => c is IFilePath && c.Name.Contains(dseToolDir + "_" + dsetoolRingFile));

		if (parseNonLogs && nodetoolRingChildFiles.HasAtLeastOneElement())
		{
			foreach (var element in nodetoolRingChildFiles)
			{
				Console.WriteLine("Processing File \"{0}\"", element.Path);
				ReadDSEToolRingFileParseIntoDataTable((IFilePath)element, dtRingInfo);
				element.MakeEmpty();
			}
		}

		IFilePath cqlFilePath;

		if (parseNonLogs && diagPath.Clone().MakeFile(cqlDDLDirFileExt, out cqlFilePath))
		{
			foreach (IFilePath element in cqlFilePath.GetWildCardMatches())
			{
				Console.WriteLine("Processing File \"{0}\"", element.Path);
				ReadCQLDDLParseIntoDataTable(element,
												null,
												null,
												dtKeySpace,
												dtTable,
												cqlHashCheck,
												ignoreKeySpaces);
				element.MakeEmpty();
			}
		}


		Parallel.ForEach(diagChildren, (diagFile) =>
		//foreach (var diagFile in diagChildren)
		{
			if (diagFile is IFilePath && !diagFile.IsEmpty)
			{
				string ipAddress;
				string dcName;
				
				if (DetermineIPDCFromFileName(((IFilePath)diagFile).FileName, dtRingInfo, out ipAddress, out dcName))
				{
					if (parseNonLogs && diagFile.Name.Contains(nodetoolCFStatsFile))
					{
						if (parseNonLogs && string.IsNullOrEmpty(dcName))
						{
							Console.WriteLine("*** Warning: A DataCenter Name was not found for path \"{0}\" in the assocated IP Address in the Ring File.", diagFile.Path);
						}
						
				    	Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var dtCFStats = new System.Data.DataTable(excelWorkSheetCFStats + "-" + ipAddress);
						dtCFStatsStack.Push(dtCFStats);
						ReadCFStatsFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtCFStats, ignoreKeySpaces, cfstatsCreateMBColumns);
					}
					else if (parseNonLogs && diagFile.Name.Contains(nodetoolTPStatsFile))
					{
						if (string.IsNullOrEmpty(dcName))
						{
							Console.WriteLine("*** Warning: A DataCenter Name was not found for path \"{0}\" in the assocated IP Address in the Ring File.", diagFile.Path);
						}
						
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var dtTPStats = new System.Data.DataTable(excelWorkSheetTPStats + "-" + ipAddress);
						dtTPStatsStack.Push(dtTPStats);
						ReadTPStatsFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtTPStats);
					}
					else if (parseNonLogs && diagFile.Name.Contains(nodetoolInfoFile))
					{
						if (string.IsNullOrEmpty(dcName))
						{
							Console.WriteLine("*** Warning: A DataCenter Name was not found for path \"{0}\" in the assocated IP Address in the Ring File.", diagFile.Path);
						}
						
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						ReadInfoFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtRingInfo);
					}
					else if (parseNonLogs && diagFile.Name.Contains(nodetoolCompactionHistFile))
					{
						if (string.IsNullOrEmpty(dcName))
						{
							Console.WriteLine("*** Warning: A DataCenter Name was not found for path \"{0}\" in the assocated IP Address in the Ring File.", diagFile.Path);
						}
						
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var dtCompHist = new System.Data.DataTable(excelWorkSheetCompactionHist + "-" + ipAddress);
						dtCompHistStack.Push(dtCompHist);
						ReadCompactionHistFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtCompHist, dtTable, ignoreKeySpaces);
					}
					else if (parseLogs && diagFile.Name.Contains(logCassandraSystemLogFile))
					{
						if (string.IsNullOrEmpty(dcName))
						{
							Console.WriteLine("*** Warning: A DataCenter Name was not found for path \"{0}\" in the assocated IP Address in the Ring File.", diagFile.Path);
						}
						
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var dtLog = new System.Data.DataTable(excelWorkSheetLogCassandra + "-" + ipAddress);
						DateTime maxLogTimestamp;
						
						dtLogsStack.Push(dtLog);
						ReadCassandraLogParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, includeLogEntriesAfterThisTimeFrame, LogMaxRowsPerNode, dtLog, out maxLogTimestamp);

						if (parseNonLogs && ((LogSummaryPeriods != null && LogSummaryPeriods.Length > 0)
												|| (LogSummaryPeriodRanges != null && LogSummaryPeriodRanges.Length > 0)))
						{
							var dtSummaryLog = new System.Data.DataTable(excelWorkSheetLogCassandra + "-" + ipAddress);
							bool useMaxTimestamp = LogSummaryPeriods == null || LogSummaryPeriods.Length == 0;
							var summaryPeriods = useMaxTimestamp ? new Tuple<DateTime,TimeSpan>[LogSummaryPeriodRanges.Length] : LogSummaryPeriods;

							if (useMaxTimestamp)
							{
								var currentRange = maxLogTimestamp.Date.AddDays(1);

								for (int nIndex = 0; nIndex < summaryPeriods.Length; ++nIndex)
								{
									summaryPeriods[nIndex] = new Tuple<DateTime,TimeSpan>(currentRange,
																							LogSummaryPeriodRanges[nIndex].Item2);
																							
									currentRange = currentRange - LogSummaryPeriodRanges[nIndex].Item1;
								}								
							}
														
							dtLogSummaryStack.Push(dtSummaryLog);
							ParseCassandraLogIntoSummaryDataTable(dtLog,
																	dtSummaryLog,
																	ipAddress,
																	dcName,
																	LogSummaryIndicatorType,
																	LogSummaryTaskItems,
																	LogSummaryIgnoreTaskExceptions,
																	summaryPeriods);
						}
                    }
					else if (parseNonLogs && diagFile.Name.Contains(confCassandraYamlFileName))
					{
						if (string.IsNullOrEmpty(dcName))
						{
							Console.WriteLine("*** Warning: A DataCenter Name was not found for path \"{0}\" in the assocated IP Address in the Ring File.", diagFile.Path);
						}
						
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var yamlList = new List<YamlInfo>();
						listCYamlStack.Push(yamlList);
						ReadYamlFileParseIntoList((IFilePath)diagFile, ipAddress, dcName, confCassandraType, yamlList);
					}
					else if (parseNonLogs && diagFile.Name.Contains(confDSEFileName))
					{
						if (string.IsNullOrEmpty(dcName))
						{
							Console.WriteLine("*** Warning: A DataCenter Name was not found for path \"{0}\" in the assocated IP Address in the Ring File.", diagFile.Path);
						}
						
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var yamlList = new List<YamlInfo>();
						listCYamlStack.Push(yamlList);
						ReadYamlFileParseIntoList((IFilePath)diagFile, 
													ipAddress,
													dcName,
													((IFilePath)diagFile).FileExtension == ".yaml" ? confDSEYamlType : confDSEType,
													yamlList);
					}
				}
				else if(((IFilePath)diagFile).FileExtension.ToLower() != ".cql")
				{
					Console.WriteLine("*** Error: File \"{0}\" was Skipped", diagFile.Path);
				}
			}
		});
		
		#endregion
	}
	else
	{
		#region Parse -- Files located in separate folders
		
		var diagNodePath = diagPath.Clone().AddChild(diagNodeDir) as Common.IDirectoryPath;
		List<Common.IDirectoryPath> nodeDirs = null;

		if (diagNodePath != null && (opsCtrDiag = diagNodePath.Exist()))
		{
			nodeDirs = diagNodePath.Children().Cast<Common.IDirectoryPath>().ToList();
		}
		else
		{
			nodeDirs = diagPath.Children().Cast<Common.IDirectoryPath>().ToList();
		}

		IFilePath filePath = null;

		if (parseNonLogs && nodeDirs.First().Clone().AddChild(nodetoolDir).MakeFile(nodetoolRingFile, out filePath))
		{
			if (filePath.Exist())
			{
				Console.WriteLine("Processing File \"{0}\"", filePath.Path);
				ReadRingFileParseIntoDataTables(filePath, dtRingInfo, dtTokenRange);
			}
		}

		if (parseNonLogs && nodeDirs.First().Clone().AddChild(dseToolDir).MakeFile(dsetoolRingFile, out filePath))
		{
			if (filePath.Exist())
			{
				Console.WriteLine("Processing File \"{0}\"", filePath.Path);
				ReadDSEToolRingFileParseIntoDataTable(filePath, dtRingInfo);
			}
		}

		if (parseNonLogs && nodeDirs.First().Clone().MakeFile(cqlDDLDirFile, out filePath))
		{
			if (filePath.Exist())
			{
				Console.WriteLine("Processing File \"{0}\"", filePath.Path);
				ReadCQLDDLParseIntoDataTable(filePath,
												null,
												null,
												dtKeySpace,
												dtTable,
												cqlHashCheck,
												ignoreKeySpaces);
			}
		}

		Parallel.ForEach(nodeDirs, (element) =>
		//foreach (var element in nodeDirs)
		{
			string ipAddress = null;
			string dcName = null;
			IFilePath diagFilePath = null;
			
			DetermineIPDCFromFileName(element.Name, dtRingInfo, out ipAddress, out dcName);

			if (parseNonLogs && string.IsNullOrEmpty(dcName))
			{
				Console.WriteLine("Warning: DataCenter Name was not found for Path \"{0}\" in the Ring file.", element.Path);
			}

			if (parseNonLogs)
			{
				Console.WriteLine("Processing Files {{{0}}} in directory \"{1}\"",
									string.Join(", ", osmachineFiles),
									element.Path);
									
				ParseOSMachineInfoDataTable(element,
											osmachineFiles,
											ipAddress,
											dcName,
											dtOSMachineInfo);
			}

			if (parseNonLogs && element.Clone().AddChild(nodetoolDir).MakeFile(nodetoolCFStatsFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var dtCFStats = new System.Data.DataTable(excelWorkSheetCFStats + "-" + ipAddress);
					dtCFStatsStack.Push(dtCFStats);
					ReadCFStatsFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtCFStats, ignoreKeySpaces, cfstatsCreateMBColumns);
				}
			}

			if (parseNonLogs && element.Clone().AddChild(nodetoolDir).MakeFile(nodetoolTPStatsFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var dtTPStats = new System.Data.DataTable(excelWorkSheetTPStats + "-" + ipAddress);
					dtTPStatsStack.Push(dtTPStats);
					ReadTPStatsFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtTPStats);
				}
			}

			if (parseNonLogs && element.Clone().AddChild(nodetoolDir).MakeFile(nodetoolInfoFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					ReadInfoFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtRingInfo);
				}
			}

			if (parseNonLogs && element.Clone().AddChild(nodetoolDir).MakeFile(nodetoolCompactionHistFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var dtHistComp = new System.Data.DataTable(excelWorkSheetCompactionHist + "-" + ipAddress);
					dtCompHistStack.Push(dtHistComp);
					ReadCompactionHistFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtHistComp, dtTable, ignoreKeySpaces);
				}
			}

			if (parseLogs && element.Clone().AddChild(logsDir).MakeFile(logCassandraDirSystemLog, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					var logFilePath = (IFilePath) diagFilePath.Clone();
					
					var logTask = Task.Run(() =>
					{
						Console.WriteLine("Processing File \"{0}\"", logFilePath.Path);
						var dtLog = new System.Data.DataTable(excelWorkSheetLogCassandra + "-" + ipAddress);
						DateTime maxLogTimestamp;

						dtLogsStack.Push(dtLog);
						ReadCassandraLogParseIntoDataTable(logFilePath, ipAddress, dcName, includeLogEntriesAfterThisTimeFrame, LogMaxRowsPerNode, dtLog, out maxLogTimestamp);

						if (parseNonLogs && ((LogSummaryPeriods != null && LogSummaryPeriods.Length > 0)
													|| (LogSummaryPeriodRanges != null && LogSummaryPeriodRanges.Length > 0)))
						{
							var dtSummaryLog = new System.Data.DataTable(excelWorkSheetLogCassandra + "-" + ipAddress);
							bool useMaxTimestamp = LogSummaryPeriods == null || LogSummaryPeriods.Length == 0;
							var summaryPeriods = useMaxTimestamp ? new Tuple<DateTime, TimeSpan>[LogSummaryPeriodRanges.Length] : LogSummaryPeriods;

							if (useMaxTimestamp)
							{
								var currentRange = maxLogTimestamp.Date.AddDays(1);

								for (int nIndex = 0; nIndex < summaryPeriods.Length; ++nIndex)
								{
									summaryPeriods[nIndex] = new Tuple<DateTime, TimeSpan>(currentRange,
																								LogSummaryPeriodRanges[nIndex].Item2);

									currentRange = currentRange - LogSummaryPeriodRanges[nIndex].Item1;
								}
							}

							dtLogSummaryStack.Push(dtSummaryLog);
							ParseCassandraLogIntoSummaryDataTable(dtLog,
																	dtSummaryLog,
																	ipAddress,
																	dcName,
																	LogSummaryIndicatorType,
																	LogSummaryTaskItems,
																	LogSummaryIgnoreTaskExceptions,
																	summaryPeriods);
						}
					});
					
					logParsingTasks.Add(logTask);
                }
			}

			if (parseNonLogs && element.Clone().AddChild(confCassandraDir).MakeFile(confCassandraFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var yamlList = new List<YamlInfo>();
					listCYamlStack.Push(yamlList);
					ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, confCassandraType, yamlList);
				}
			}

			if (parseNonLogs && element.Clone().AddChild(confDSEDir).MakeFile(confDSEYamlFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var yamlList = new List<YamlInfo>();
					listCYamlStack.Push(yamlList);
					ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, confDSEYamlType, yamlList);
				}
			}

			if (parseNonLogs && element.Clone().AddChild(confDSEDir).MakeFile(confDSEFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var yamlList = new List<YamlInfo>();
					listCYamlStack.Push(yamlList);
					ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, confDSEType, yamlList);
				}
			}

		});
		
		#endregion
	}

	var runYamlListIntoDT = Task.Run(() => ParseYamlListIntoDataTable(listCYamlStack, dtYaml));

	#endregion

	#region Excel Creation/Formatting

	//Cassandra Log (usually runs longer)
	var runLogToExcel = Task.Run(() =>
	{
		if (LoadLogsIntoExcel)
		{
			var dtFilter = LogExcelWorkbookFilter;

			if (dtFilter == string.Empty)
			{
				var minMaxLogTimestamp = LogCassandraMaxTimestamps.Min();
				
				dtFilter = string.Format("[Timestamp] >= #{0}#", minMaxLogTimestamp.Date - LogTimeSpanRange);
			}
			
			//Need to wait until all logs are parsed...
			logParsingTasks.ForEach(task => task.Wait());

			DTLoadIntoDifferentExcelWorkBook(excelFilePath,
											   excelWorkSheetLogCassandra,
											   dtLogsStack,
											   workSheet =>
												   {
													   workSheet.Cells["C:C"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";
													   workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
													   workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
													   //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
													   workSheet.View.FreezePanes(2, 1);
													   workSheet.Cells["A1:J1"].AutoFilter = true;
													   workSheet.Cells.AutoFitColumns();
												   },
												   MaxRowInExcelWorkBook,
												   MaxRowInExcelWorkSheet,
												   dtFilter);
		}
	});

	//Non-Logs
	if (parseNonLogs)
	{
		var excelFile = Common.Path.PathUtils.BuildFilePath(excelFilePath).FileInfo();
		using (var excelPkg = new ExcelPackage(excelFile))
		{
			//Ring
			if (dtRingInfo.Rows.Count > 0)
			{
				DTLoadIntoExcelWorkBook(excelPkg,
											excelWorkSheetRingInfo,
											dtRingInfo,
											workSheet =>
											{
												workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
												workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
												//workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
												workSheet.View.FreezePanes(2, 1);
												workSheet.Cells["A1:M1"].AutoFilter = true;
												workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0.00";
												workSheet.Cells["J:J"].Style.Numberformat.Format = "#,###,###,##0.00";
												workSheet.Cells["G:G"].Style.Numberformat.Format = "##0.00%";
												workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0";
												workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###,###,##0";
												workSheet.Cells["H:H"].Style.Numberformat.Format = "d hh:mm";

												workSheet.Cells.AutoFitColumns();
											});
			}

			//TokenRing
			if (dtTokenRange.Rows.Count > 0)
			{
				DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkSheetRingTokenRanges,
										dtTokenRange,
										workSheet =>
										{
											workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
											workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
											//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
											workSheet.View.FreezePanes(2, 1);
											workSheet.Cells["A1:F1"].AutoFilter = true;
											workSheet.Cells["C:D"].Style.Numberformat.Format = "0";
											workSheet.Cells["E:E"].Style.Numberformat.Format = "#,###,###,##0";
											workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells.AutoFitColumns();
										});
			}

			//OS/Machine Indo
			if (dtOSMachineInfo.Rows.Count > 0)
			{
				DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkSheetOSMachineInfo,
										dtOSMachineInfo,
										workSheet =>
										{
											workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
											workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
											//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
											workSheet.View.FreezePanes(3, 1);
											
											workSheet.Cells["H1:K1"].Style.WrapText = true;
											workSheet.Cells["H1:K1"].Merge = true;
											workSheet.Cells["H1:K1"].Value = "CPU Load (Percent)";
											workSheet.Cells["H1:H2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
											workSheet.Cells["K1:K2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

											workSheet.Cells["L1:Q1"].Style.WrapText = true;
											workSheet.Cells["L1:Q1"].Merge = true;
											workSheet.Cells["L1:Q1"].Value = "Memory (MB)";
											workSheet.Cells["L1:L2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
											workSheet.Cells["Q1:Q2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

											workSheet.Cells["R1:U1"].Style.WrapText = true;
											workSheet.Cells["R1:U1"].Merge = true;
											workSheet.Cells["R1:U1"].Value = "Java";
											workSheet.Cells["R1:R2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
											workSheet.Cells["U1:U2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;

											workSheet.Cells["V1:Y1"].Style.WrapText = true;
											workSheet.Cells["V1:Y1"].Merge = true;
											workSheet.Cells["V1:Y1"].Value = "Java Non-Heap (MB)";
											workSheet.Cells["V1:V2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;
											workSheet.Cells["Y1:Y2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;

											workSheet.Cells["Z1:AC1"].Style.WrapText = true;
											workSheet.Cells["Z1:AC1"].Merge = true;
											workSheet.Cells["Z1:AC1"].Value = "Java Heap (MB)";
											workSheet.Cells["Z1:Z2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;
											workSheet.Cells["AC1:AC2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

											workSheet.Cells["AD1:AK1"].Style.WrapText = true;
											workSheet.Cells["AD1:AK1"].Merge = true;
											workSheet.Cells["AD1:AK1"].Value = "NTP";
											workSheet.Cells["AD1:AD2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
											workSheet.Cells["AK1:AK2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

											workSheet.Cells["D:D"].Style.Numberformat.Format = "#,###,###,##0";
											workSheet.Cells["L:Q"].Style.Numberformat.Format = "#,###,###,##0";
											workSheet.Cells["AD:AH"].Style.Numberformat.Format = "#,###,###,##0";
											workSheet.Cells["H:K"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["V:V"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["W:W"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["X:X"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["Y:Y"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["Z:Z"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["AA:AA"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["AB:AB"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["AC:AC"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["AI:AK"].Style.Numberformat.Format = "#,###,###,##0.00";
											
											workSheet.Cells["A2:AK2"].AutoFilter = true;
											workSheet.Cells.AutoFitColumns();
										},
										null,
										"A2");
			}

			//CFStats
			DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkSheetCFStats,
										dtCFStatsStack,
										workSheet =>
										{
											workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
											workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
											//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
											workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###,###,##0";

											//workSheet.Cells["H1"].AddComment("Change Numeric Format to Display Decimals", "Rich Andersen");
											workSheet.Cells["H1"].Value = workSheet.Cells["H1"].Text + "(Formatted)";
											workSheet.View.FreezePanes(2, 1);
											workSheet.Cells["A1:H1"].AutoFilter = true;
											workSheet.Cells.AutoFitColumns();
										},
										false,
										-1);

			//TPStats
			DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkSheetTPStats,
										dtTPStatsStack,
										workSheet =>
										{
											workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
											workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
											//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
											workSheet.Cells["D:I"].Style.Numberformat.Format = "#,###,###,##0";

											workSheet.View.FreezePanes(2, 1);
											workSheet.Cells["A1:I1"].AutoFilter = true;
											workSheet.Cells.AutoFitColumns();
										},
										false,
										-1);

			//Compacation History
			DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkSheetCompactionHist,
										dtCompHistStack,
										workSheet =>
										{
											workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
											workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
											//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
											workSheet.Cells["E:E"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";
											workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0";
											workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###,###,##0";
											workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###,###,##0.00";
											workSheet.Cells["J1"].AddComment("The notation means {tables:rows}. For example {1:3, 3:1} means 3 rows were taken from one sstable (1:3) and 1 row taken from 3 (3:1) sstables, all to make the one sstable in that compaction operation.", "Rich Andersen");

											workSheet.View.FreezePanes(2, 1);
											workSheet.Cells["A1:J1"].AutoFilter = true;
											workSheet.Cells.AutoFitColumns();
										},
										false,
										-1);
			//DDL Keyspace
			if (dtKeySpace.Rows.Count > 0)
			{
				DTLoadIntoExcelWorkBook(excelPkg,
											excelWorkSheetDDLKeyspaces,
											dtKeySpace,
											workSheet =>
											{
												workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
												workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
												//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
												workSheet.View.FreezePanes(2, 1);
												workSheet.Cells["A1:E1"].AutoFilter = true;
												workSheet.Cells.AutoFitColumns();
											});
			}

			//DDL CQL Table
			if (dtTable.Rows.Count > 0)
			{
				DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkSheetDDLTables,
										dtTable,
										workSheet =>
											{
												workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
												workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
												//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
												workSheet.View.FreezePanes(2, 1);
												workSheet.Cells["A1:F1"].AutoFilter = true;
												workSheet.Cells.AutoFitColumns();
											});
			}

			//Yaml
			runYamlListIntoDT.Wait();

			if (dtYaml.Rows.Count > 0)
			{
				DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkSheetYaml,
										dtYaml,
										workSheet =>
										{
											workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
											workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
											//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
											workSheet.View.FreezePanes(2, 1);
											workSheet.Cells["A1:E1"].AutoFilter = true;
											workSheet.Cells.AutoFitColumns();
										});
			}

			//Cassandra Log Summary
			//Need to wait until all logs are parsed...
			logParsingTasks.ForEach(task => task.Wait());
			DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkSheetSummaryLogCassandra,
										dtLogSummaryStack,
										workSheet =>
										{
											workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
											workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
											//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
											workSheet.Cells["A:A"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm";
											workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0";
											workSheet.Cells["B:B"].Style.Numberformat.Format = "d hh:mm";

											workSheet.View.FreezePanes(2, 1);
											workSheet.Cells["A1:H1"].AutoFilter = true;
											workSheet.Cells.AutoFitColumns();
										},
										false,
										-1);


			excelPkg.Save();
		} //Save non-log data
		Console.WriteLine("*** Excel WorkBooks saved to \"{0}\"", excelFile.FullName);
	}
	
	runLogToExcel.Wait();
	
	#endregion
}

#region Excel Related Functions

bool DetermineIPDCFromFileName(string pathItem, DataTable dtRingInfo, out string ipAddress, out string dcName)
{
	var possibleAddress = Common.StringFunctions.Split(pathItem,
														new char[] { ' ', '-', '_' },
														Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
														Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
	
	if (possibleAddress.Count() == 1)
	{
		if (!IPAddressStr(possibleAddress[0], out ipAddress))
		{
			dcName = null;
			return false;
		}
	}
	else
	{
		var lastPartName = possibleAddress.Last();
		
		if (Common.StringFunctions.CountOccurrences(lastPartName, '.') > 3)
		{
			var extPos = lastPartName.LastIndexOf('.');
			lastPartName = lastPartName.Substring(0,extPos);
		}
		
		//Ip Address is either the first part of the name or the last
		if (!IPAddressStr(possibleAddress[0], out ipAddress))
		{
			if (!IPAddressStr(lastPartName, out ipAddress))
			{
				dcName = null;
				return false;
			}
		}
	}
	
	var dcRow = dtRingInfo.Rows.Count == 0 ? null : dtRingInfo.Rows.Find(ipAddress);

	if (dcRow == null)
	{
		dcName = null;
	}
	else
	{
		dcName = dcRow[1] as string;
	}
	
	return true;
}

ExcelRangeBase DTLoadIntoExcelWorkBook(ExcelPackage excelPkg,
											string workSheetName,
											System.Data.DataTable dtExcel,
											Action<ExcelWorksheet> worksheetAction = null,
											string filterView = null,
											string startingWSCell = "A1")
{
	dtExcel.AcceptChanges();

	if (!string.IsNullOrEmpty(filterView))
	{
		var dataView = new DataView(dtExcel, filterView, null, DataViewRowState.CurrentRows);

		if (dataView != null)
		{
			dtExcel = dataView.ToTable(dtExcel.TableName);
		}
	}
	
	var dtErrors = dtExcel.GetErrors();
	if (dtErrors.Length > 0)
	{
		dtErrors.Dump(string.Format("Table \"{0}\" Has Error", dtExcel.TableName));
	}

	var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
	if (workSheet == null)
	{
		workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
	}
	else
	{
		workSheet.Cells.Clear();
	}

	if (string.IsNullOrEmpty(filterView))
	{
		Console.WriteLine("Loading DataTable \"{0}\" into Excel WorkSheet \"{1}\". Rows: {2:###,###,##0}", dtExcel.TableName, workSheet.Name, dtExcel.Rows.Count);
	}
	else
	{
		Console.WriteLine("Loading DataTable \"{0}\" into Excel WorkSheet \"{1}\" with Filter \"{2}\". Rows: {3:###,###,##0}", dtExcel.TableName, workSheet.Name, filterView, dtExcel.Rows.Count);
	}
	
	var loadRange = workSheet.Cells[startingWSCell].LoadFromDataTable(dtExcel, true);

	if (loadRange != null && worksheetAction != null)
	{
		worksheetAction(workSheet);
	}
	
	return loadRange;
}

ExcelRangeBase DTLoadIntoExcelWorkBook(ExcelPackage excelPkg,
											string workSheetName,
											Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable> dtExcels,
											Action<ExcelWorksheet> worksheetAction = null,
											bool enableMaxRowLimitPerWorkSheet = true,
											int maxRowInExcelWorkSheet = MaxRowInExcelWorkSheet,
											string filterView = null,
											string startingWSCell = "A1")
{
	var orginalWSName = workSheetName;
	var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
	if (workSheet == null)
	{
		workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
	}
	else
	{
		workSheet.Cells.Clear();
	}

	System.Data.DataTable dtExcel;
	ExcelRangeBase rangeLoadAt = workSheet.Cells[startingWSCell];
	ExcelRangeBase loadRange = null;
	bool printHdrs = true;
	DataRow[] dtErrors;
	int wsCount = 0;
	int totalRows = 0;
	
	while (dtExcels.Pop(out dtExcel))
	{
		dtExcel.AcceptChanges();

		if (!string.IsNullOrEmpty(filterView))
		{
			var dataView = new DataView(dtExcel, filterView, null, DataViewRowState.CurrentRows);

			if (dataView != null)
			{
				dtExcel = dataView.ToTable(dtExcel.TableName);
			}
		}
		
		dtErrors = dtExcel.GetErrors();
		if (dtErrors.Length > 0)
		{
			dtErrors.Dump(string.Format("Table \"{0}\" Has Error", dtExcel.TableName));
		}

		if (enableMaxRowLimitPerWorkSheet 
				&& maxRowInExcelWorkSheet > 0
				&& totalRows >= maxRowInExcelWorkSheet)
		{
			++wsCount;


			if (worksheetAction != null)
			{
				worksheetAction(workSheet);
			}

			workSheetName = string.Format("{0}-{1:000}", orginalWSName, wsCount);
			workSheet = excelPkg.Workbook.Worksheets[workSheetName];
			if (workSheet == null)
			{
				workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
			}
			else
			{
				workSheet.Cells.Clear();
			}

			printHdrs = true;
			rangeLoadAt = workSheet.Cells[startingWSCell];
			totalRows = 0;
		}

		loadRange = rangeLoadAt.LoadFromDataTable(dtExcel, printHdrs);

		if (string.IsNullOrEmpty(filterView))
		{
			Console.WriteLine("Loaded DataTable \"{0}\" into Excel WorkSheet \"{1}\" in Range {2}. Rows: {3:###,###,##0}",
								dtExcel.TableName,
								workSheet.Name,
								loadRange == null ? "<Empty>" : loadRange.Address,
								dtExcel.Rows.Count);
		}
		else
		{
			Console.WriteLine("Loaded DataTable \"{0}\" into Excel WorkSheet \"{1}\" with Filter \"{2}\" in Range {3}. Rows: {4:###,###,##0}",
								dtExcel.TableName,
								workSheet.Name,
								filterView,
								loadRange == null ? "<Empty>" : loadRange.Address,
								dtExcel.Rows.Count);
		}
									
		if (loadRange != null)
		{
			printHdrs = false;
			rangeLoadAt = workSheet.Cells[startingWSCell[0].ToString() + (loadRange.End.Row + 1)];
			totalRows += dtExcel.Rows.Count;
		}
	}

	if (worksheetAction != null && totalRows > 0)
	{
		worksheetAction(workSheet);
	}

	return loadRange;
}

int DTLoadIntoDifferentExcelWorkBook(string excelFilePath, 
											string workSheetName,
											Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable> dtExcels,
											Action<ExcelWorksheet> worksheetAction = null,
											int maxRowInExcelWorkBook = MaxRowInExcelWorkBook,
											int maxRowInExcelWorkSheet = MaxRowInExcelWorkSheet,
											string filterView = null,
											string startingWSCell = "A1")
{
	DataTable dtExcel;
	int wsCount = 0;
	int totalRows = 0;
	var excelTargetFile = Common.Path.PathUtils.BuildFilePath(excelFilePath);
	var stackGroups = new List<Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>>();
	var newStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	
	while (dtExcels.Pop(out dtExcel))
	{
		totalRows += dtExcel.Rows.Count;
		newStack.Push(dtExcel);

		if (maxRowInExcelWorkBook > 0
				&& totalRows >= maxRowInExcelWorkBook)
		{
			stackGroups.Add(newStack);
			totalRows = 0;
			newStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
		}
	}

	if (totalRows > 0)
	{
		stackGroups.Add(newStack);
	}

	excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}-{{1:000}}{1}",
													excelTargetFile.Name,
													excelTargetFile.FileExtension);

	Parallel.ForEach(stackGroups, stack =>
	//foreach (var stack in stackGroups)
	{
		var excelFile = excelTargetFile.ApplyFileNameFormat(new object[] { workSheetName, wsCount++}).FileInfo();
		
		using (var excelPkg = new ExcelPackage(excelFile))
		{
			DTLoadIntoExcelWorkBook(excelPkg, 
										workSheetName,
										stack,
										worksheetAction,
										maxRowInExcelWorkSheet > 0,
										maxRowInExcelWorkSheet,
										filterView,
										startingWSCell);
										
			excelPkg.Save();
			Console.WriteLine("*** Excel WorkBooks saved to \"{0}\"", excelFile.FullName);
		}
	});

	return wsCount;
}

#endregion

#region Reading/Parsing Files

void ReadRingFileParseIntoDataTables(IFilePath ringFilePath,
										System.Data.DataTable dtRingInfo,
										System.Data.DataTable dtTokenRange)
{
	if (dtRingInfo.Columns.Count == 0)
	{
		dtRingInfo.Columns.Add("Node IPAdress", typeof(string));
		dtRingInfo.Columns[0].Unique = true;
		dtRingInfo.PrimaryKey = new System.Data.DataColumn[] { dtRingInfo.Columns[0] };
		dtRingInfo.Columns.Add("DataCenter", typeof(string));
		dtRingInfo.Columns.Add("Rack", typeof(string));
		dtRingInfo.Columns.Add("Status", typeof(string));
		dtRingInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Storage Used (MB)", typeof(decimal)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Storage Utilization", typeof(decimal)).AllowDBNull = true;
		//dtRingInfo.Columns.Add("Number of Restarts", typeof(int)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Uptime", typeof(TimeSpan)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Heap Memory (MB)", typeof(string)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Off Heap Memory (MB)", typeof(decimal)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Nbr VNodes", typeof(int)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Nbr of Exceptions", typeof(int)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Gossip Enableed", typeof(bool)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Thrift Enabled", typeof(bool)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Native Transport Enable", typeof(bool)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Key Cache Information", typeof(string)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Row Cache Information", typeof(string)).AllowDBNull = true;
		dtRingInfo.Columns.Add("Counter Cache Information", typeof(string)).AllowDBNull = true;
    }

	if (dtTokenRange.Columns.Count == 0)
	{
		dtTokenRange.Columns.Add("DataCenter", typeof(string));
		dtTokenRange.Columns.Add("Node IPAdress", typeof(string));
		dtTokenRange.Columns.Add("Start Token (exclusive)", typeof(long));
		dtTokenRange.Columns.Add("End Token (inclusive)", typeof(long));
		dtTokenRange.Columns.Add("Slots", typeof(long));
		dtTokenRange.Columns.Add("Load(MB)", typeof(decimal));
	}
	
	var fileLines = ringFilePath.ReadAllLines();
	
	string currentDC = null;
	long? currentStartToken = null;
	long endToken;
	string line = null;
	string ipAddress;
	DataRow dataRow;
	List<string> parsedLine;
	bool newDC = true;
	bool rangeStart = false;
	
	foreach (var element in fileLines)
	{
		line = element.Trim();
		
		if (!string.IsNullOrEmpty(line))
		{
			if (line.StartsWith("Datacenter:"))
			{
				newDC = true;
				currentDC = line.Substring(12).Trim();
			}
			else if (newDC)
			{
				if (line[0] != '=' 
						&& !line.StartsWith("Address")
						&& !line.StartsWith("Note:")
						&& !line.StartsWith("Warning:"))
				{
					newDC = false;
					rangeStart = true;
					currentStartToken = long.Parse(line);
				}
			}
			else
			{
				
				//Address         Rack        Status State   Load Type            Owns                Token (end)
				parsedLine = Common.StringFunctions.Split(line, 
															' ',
															Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
															Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

				if (Char.IsDigit(parsedLine[0][0]) || parsedLine[0][0] == '-')
				{
					IPAddressStr(parsedLine[0], out ipAddress);
					
					dataRow = dtRingInfo.Rows.Find(ipAddress);

					if (dataRow == null)
					{
						dataRow = dtRingInfo.NewRow();

						dataRow["Node IPAdress"] = ipAddress;
						dataRow["DataCenter"] = currentDC;
						dataRow["Rack"] = parsedLine[1];
						dataRow["Status"] = parsedLine[2];

						dtRingInfo.Rows.Add(dataRow);
					}

					dataRow = dtTokenRange.NewRow();

					dataRow["DataCenter"] = currentDC;
					dataRow["Node IPAdress"] = ipAddress;
					dataRow["Start Token (exclusive)"] = currentStartToken;
					endToken = long.Parse(parsedLine[7]);
					dataRow["End Token (inclusive)"] = endToken;

					if (rangeStart)
					{
						rangeStart = false;
						dataRow["Slots"] =  (endToken - long.MinValue) 
												+ (long.MaxValue - currentStartToken.Value);
					}
					else
					{
						dataRow["Slots"] = Math.Abs(endToken - currentStartToken.Value);
					}
					
					dataRow["Load(MB)"] = ConvertInToMB(parsedLine[4], parsedLine[5]);
					
					currentStartToken = endToken;
					
					dtTokenRange.Rows.Add(dataRow);
				}
			}
		}
	}
	
	
}

void ReadCFStatsFileParseIntoDataTable(IFilePath cfstatsFilePath,
										string ipAddress,
										string dcName,
										System.Data.DataTable dtFSStats,
										IEnumerable<string> ignoreKeySpaces,
										IEnumerable<string> addToMBColumn)
{
	if (dtFSStats.Columns.Count == 0)
	{
		dtFSStats.Columns.Add("Data Center", typeof(string));
		dtFSStats.Columns[0].AllowDBNull = true;
		dtFSStats.Columns.Add("Node IPAdress", typeof(string));
		dtFSStats.Columns.Add("KeySpace", typeof(string));
		dtFSStats.Columns.Add("Table", typeof(string));
		dtFSStats.Columns[3].AllowDBNull = true;
		dtFSStats.Columns.Add("Attribute", typeof(string));
		dtFSStats.Columns.Add("Value", typeof(object));
		dtFSStats.Columns.Add("Unit of Measure", typeof(string));
		dtFSStats.Columns[6].AllowDBNull = true;

		dtFSStats.Columns.Add("Size in MB", typeof(decimal));
		dtFSStats.Columns[7].AllowDBNull = true;

		//dtFSStats.PrimaryKey = new System.Data.DataColumn[] { dtFSStats.Columns[0],  dtFSStats.Columns[1],  dtFSStats.Columns[2],  dtFSStats.Columns[3], dtFSStats.Columns[4] };
	}
	
	
	var fileLines = cfstatsFilePath.ReadAllLines();
	string line;
	DataRow dataRow;
	List<string> parsedLine;
	List<string> parsedValue;
	string currentKS = null;
	string currentTbl = null;
	object numericValue;

	foreach (var element in fileLines)
	{
		line = element.Trim();

		if (!string.IsNullOrEmpty(line) && line[0] != '-')
		{
			parsedLine = Common.StringFunctions.Split(line,
														':',
														Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
														Common.StringFunctions.SplitBehaviorOptions.Default);

			if (parsedLine[0] == "Keyspace")
			{
				if (ignoreKeySpaces != null && ignoreKeySpaces.Contains(parsedLine[1].ToLower()))
				{
					currentKS = null;
				}
				else
				{
					currentKS = parsedLine[1];
				}
				currentTbl = null;
			}
			else if (currentKS == null)
			{
				continue;
			}
			else if (parsedLine[0] == "Table")
			{
				currentTbl = parsedLine[1];
			}
			else
			{
				dataRow = dtFSStats.NewRow();

				dataRow[0] = dcName;
				dataRow[1] = ipAddress;
				dataRow[2] = currentKS;
				dataRow[3] = currentTbl;
				dataRow[4] = parsedLine[0];

				parsedValue = Common.StringFunctions.Split(parsedLine[1],
															' ',
															Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
															Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

				if (Common.StringFunctions.ParseIntoNumeric(parsedValue[0], out numericValue, true))
				{
					dataRow[5] = numericValue;

					if (parsedValue.Count() > 1)
					{
						dataRow[6] = parsedValue[1];
					}

					if (addToMBColumn != null)
					{
						var decNbr = decimal.Parse(numericValue.ToString());
						
						foreach (var item in addToMBColumn)
						{
							if (parsedLine[0].ToLower().Contains(item))
							{
								dataRow[7] = decNbr / BytesToMB;
								break;
							}
						}
					}
				}
				else
				{
					dataRow[6] = parsedLine[1];
				}

				dtFSStats.Rows.Add(dataRow);
			}
		}
	}
}

void ReadTPStatsFileParseIntoDataTable(IFilePath tpstatsFilePath,
										string ipAddress,
										string dcName,
										System.Data.DataTable dtTPStats)
{
	if (dtTPStats.Columns.Count == 0)
	{
		dtTPStats.Columns.Add("Data Center", typeof(string));
		dtTPStats.Columns[0].AllowDBNull = true;
		dtTPStats.Columns.Add("Node IPAdress", typeof(string));
		dtTPStats.Columns.Add("Attribute", typeof(string));
		
		dtTPStats.Columns.Add("Active", typeof(long));
		dtTPStats.Columns["Active"].AllowDBNull = true;
		dtTPStats.Columns.Add("Pending", typeof(long));
		dtTPStats.Columns["Pending"].AllowDBNull = true;
		dtTPStats.Columns.Add("Completed", typeof(long));
		dtTPStats.Columns["Completed"].AllowDBNull = true;
		dtTPStats.Columns.Add("Blocked", typeof(long));
		dtTPStats.Columns["Blocked"].AllowDBNull = true;
		dtTPStats.Columns.Add("All time blocked", typeof(long));
		dtTPStats.Columns["All time blocked"].AllowDBNull = true;
		dtTPStats.Columns.Add("Dropped", typeof(long));
		dtTPStats.Columns["Dropped"].AllowDBNull = true;
	}


	var fileLines = tpstatsFilePath.ReadAllLines();
	string line;
	DataRow dataRow;
	int parsingSection = 0; //0 -- Pool, 1 -- Message Type
	List<string> parsedValue;
	
	foreach (var element in fileLines)
	{
		line = element.Trim();

		if (string.IsNullOrEmpty(line))
		{
			continue;
		}
		if (line.StartsWith("Pool Name"))
        {
			parsingSection = 0;
			continue;
		}
		else if (line.StartsWith("Message type"))
		{
			parsingSection = 1;
			continue;
		}

		parsedValue = Common.StringFunctions.Split(line,
													' ',
													Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
													Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
		dataRow = dtTPStats.NewRow();

		dataRow[0] = dcName;
		dataRow[1] = ipAddress;
		dataRow["Attribute"] = parsedValue[0];
		
		if (parsingSection == 0)
		{
			//Pool Name                    Active   Pending      Completed   Blocked  All time blocked
			dataRow["Active"] = long.Parse(parsedValue[1]);
			dataRow["Pending"] = long.Parse(parsedValue[2]);
			dataRow["Completed"] = long.Parse(parsedValue[3]);
			dataRow["Blocked"] = long.Parse(parsedValue[4]);
			dataRow["All time blocked"] = long.Parse(parsedValue[5]);
		}
		else if (parsingSection == 1)
		{
			//Message type           Dropped
			dataRow["Dropped"] = long.Parse(parsedValue[1]);
		}

		dtTPStats.Rows.Add(dataRow);
	}
}

static Common.Patterns.Collections.ThreadSafe.List<DateTime> LogCassandraMaxTimestamps = new Common.Patterns.Collections.ThreadSafe.List<System.DateTime>();

void ReadCassandraLogParseIntoDataTable(IFilePath clogFilePath,
										string ipAddress,
										string dcName,
										DateTime onlyEntriesAfterThisTimeFrame,
										int maxRowWrite,
										System.Data.DataTable dtCLog,
										out DateTime maxTimestamp,
										int gcPausedFlagThresholdInMS = GCPausedFlagThresholdInMS)
{
	if (dtCLog.Columns.Count == 0)
	{
		dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
		dtCLog.Columns.Add("Node IPAdress", typeof(string));
		dtCLog.Columns.Add("Timestamp", typeof(DateTime));
		dtCLog.Columns.Add("Indicator", typeof(string));
		dtCLog.Columns.Add("Task", typeof(string));
		dtCLog.Columns.Add("Item", typeof(string));
		dtCLog.Columns.Add("Exception", typeof(string)).AllowDBNull = true;
		dtCLog.Columns.Add("Exception Description", typeof(string)).AllowDBNull = true;
		dtCLog.Columns.Add("Assocated Item", typeof(string)).AllowDBNull = true;
		dtCLog.Columns.Add("Assocated Value", typeof(object)).AllowDBNull = true;
		dtCLog.Columns.Add("Description", typeof(string));
		dtCLog.Columns.Add("Flagged", typeof(bool)).AllowDBNull = true;
	}

	var fileLines = clogFilePath.ReadAllLines();
	string line;
	List<string> parsedValues;
	DataRow dataRow;
	DataRow lastRow = null;
	DateTime lineDateTime;
	string lineIPAddress;
	int skipLines = -1;
	string tableItem = null;
	int tableItemPos = -1;
	int tableItemValuePos = -1;

	maxTimestamp = DateTime.MinValue;
	
	if (maxRowWrite <= 0)
	{
		maxRowWrite = int.MaxValue;
	}
	
	for(int nLine = 0; nLine < fileLines.Length; ++nLine)
	{
		line = fileLines[nLine].Trim();

		if (string.IsNullOrEmpty(line) 
			|| line.Substring(0, 3).ToLower() == "at "
			|| line.Substring(0, 4).ToLower() == "... ")
		{
			continue;
		}

		parsedValues = Common.StringFunctions.Split(line,
													' ',
													Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
													Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

		//INFO  [CompactionExecutor:9928] 2016-07-25 04:23:34,819  CompactionTask.java:274 - Compacted 4 sstables to [/data/system/peer_events-59dfeaea8db2334191ef109974d81484/system-peer_events-ka-77,].  35,935 bytes to 35,942 (~100% of original) in 40ms = 0.856924MB/s.  20 total partitions merged to 5.  Partition merge counts were {4:5, }
		//		INFO [SharedPool-Worker-2] 2016-07-25 04:25:35,919  Message.java:532 - Unexpected exception during request; channel = [id: 0x40c292ba, / 10.160.139.242:42705 :> / <1ocal node>:9042]
		//		java.io.IOException: Error while read(...): Connection reset by peer
		//    		at io.netty.channel.epoll.Native.readAddress(Native Method) ~[netty - all - 4.0.23.Final.jar:4.0.23.Final]
		//    		at io.netty.channel.epoll.EpollSocketChannel$EpollSocketUnsafe.doReadBytes(EpollSocketChannel.java:675) ~[netty - all - 4.0.23.Final.jar:4.0.23.Final]
		//    		at io.netty.channel.epoll.EpollSocketChannel$EpollSocketUnsafe.epollInReady(EpollSocketChannel.java:714) ~[netty - all - 4.0.23.Final.jar:4.0.23.Final]
		//		WARN  [HintedHandoffManager:1] 2016-07-25 04:26:10,445  HintedHandoffMetrics.java:79 - /10.170.110.191 has 1711 dropped hints, because node is down past configured hint window.
		//		ERROR[RMI TCP Connection(7348) - 127.0.0.1] 2016-07-29 23:24:54,576 SolrCore.java(line 2340) IO error while trying to get the size of the Directory
		//		java.io.FileNotFoundException: _i2v5.nvm
		//		at org.apache.lucene.store.FSDirectory.fileLength(FSDirectory.java:267)
		//	WARN [ReadStage:1325219] 2016-07-14 17:41:21,164 SliceQueryFilter.java (line 231) Read 11 live and 1411 tombstoned cells in cma.mls_records_property (see tombstone_warn_threshold). 5000 columns was requested, slices=[-]

		//INFO [CompactionExecutor:7414] 2016-07-26 23:11:50,335 CompactionController.java (line 191) Compacting large row billing/account_payables:20160726:FMCC (348583137 bytes) incrementally
		//INFO [ScheduledTasks:1] 2016-07-30 06:32:53,397 GCInspector.java (line 116) GC for ParNew: 394 ms for 1 collections, 13571498424 used; max is 25340346368
		//WARN [Native-Transport-Requests:30] 2016-08-01 22:58:11,080 BatchStatement.java (line 226) Batch of prepared statements for [clearcore.documents_case] is of size 71809, exceeding specified threshold of 65536 by 6273.
		//WARN [ReadStage:1907643] 2016-08-01 23:26:42,845 SliceQueryFilter.java (line 231) Read 14 live and 1344 tombstoned cells in cma.mls_records_property (see tombstone_warn_threshold). 5000 columns was requested, slices=[-]
		//INFO  [Service Thread] 2016-08-10 06:51:10,572  GCInspector.java:258 - G1 Young Generation GC in 264ms.  G1 Eden Space: 3470786560 -> 0; G1 Old Gen: 2689326672 -> 2934172000; G1 Survivor Space: 559939584 -> 35651584; 

		#region Exception Log Info Parsing
		if (parsedValues[0].ToLower().Contains("exception"))
		{
			if (lastRow != null)
			{
				lastRow.BeginEdit();
				
				lastRow["Exception"] = parsedValues[0][parsedValues[0].Length - 1] == ':'
										? parsedValues[0].Substring(0, parsedValues[0].Length - 1)
										: parsedValues[0];
				lastRow["Exception Description"] = line;

				if (lastRow["Assocated Item"] == DBNull.Value)
				{
					foreach (var element in parsedValues)
					{
						if (element[0] == '(')
                        {
							if (LookForIPAddress(element.Substring(1,element.Length - 2).Trim(), ipAddress, out lineIPAddress))
							{
								lastRow["Assocated Item"] = lineIPAddress;
								break;
							}
						}
						else if (element[0] == '/')
                        {
							if (LookForIPAddress(element, ipAddress, out lineIPAddress))
							{
								lastRow["Assocated Item"] = lineIPAddress;
								break;
							}
						}
					}
				}
				
				lastRow.EndEdit();
				lastRow.AcceptChanges();
			}
			continue;
		}
		else if (parsedValues[0].ToLower() == "caused")
		{
			if (lastRow != null)
			{
				lastRow.BeginEdit();
				
				lastRow["Exception"] = parsedValues[2][parsedValues[2].Length - 1] == ':'
										? parsedValues[2].Substring(0, parsedValues[2].Length - 1)
										: parsedValues[2];
				lastRow["Exception Description"] = line;

				if (lastRow["Assocated Item"] == DBNull.Value)
				{
					foreach (var element in parsedValues)
					{
						if (element[0] == '(')
                        {
							if (LookForIPAddress(element.Substring(1, element.Length - 2).Trim(), ipAddress, out lineIPAddress))
							{
								lastRow["Assocated Item"] = lineIPAddress;
								break;
							}
						}
						else if (element[0] == '/')
                        {
							if (LookForIPAddress(element, ipAddress, out lineIPAddress))
							{
								lastRow["Assocated Item"] = lineIPAddress;
								break;
							}
						}
					}
				}

				lastRow.EndEdit();
				lastRow.AcceptChanges();
			}
			continue;
		}
		#endregion
		
		if (parsedValues.Count < 6)
		{
			if (lastRow != null)
			{
				line.Dump(string.Format("Warning: Invalid Log Line File: {0}", clogFilePath.PathResolved));
			}
			continue;
		}

		#region Timestamp Parsing
		if (DateTime.TryParse(parsedValues[2] + ' ' + parsedValues[3].Replace(',', '.'), out lineDateTime))
		{
			if (lineDateTime < onlyEntriesAfterThisTimeFrame)
			{
				continue;
			}

			if (skipLines < 0)
			{
				if (maxRowWrite > 0)
				{
					skipLines = fileLines.Length - nLine - maxRowWrite;
				}
				else
				{
					skipLines = 1;
				}
			}

			if (--skipLines > 0)
			{
				continue;
			}
		}
		else
		{
			line.Dump(string.Format("Warning: Invalid Log Date/Time File: {0}", clogFilePath.PathResolved));
			continue;
		}
		#endregion

		#region Basic column Info
		
		dataRow = dtCLog.NewRow();
		
		dataRow[0] = dcName;
		dataRow[1] = ipAddress;
		dataRow["Timestamp"] = lineDateTime;
		
		maxTimestamp = maxTimestamp.Max(lineDateTime);
		
		dataRow["Indicator"] = parsedValues[0];
		
		if (parsedValues[1][0] == '[')
		{
			string strItem = parsedValues[1];
			int nPos = strItem.IndexOf(':');

			if (nPos > 2)
			{
				strItem = strItem.Substring(1, nPos-1);
			}
			else
			{
				strItem = strItem.Substring(1, strItem.Length - 2);
			}
			
			dataRow["Task"] = strItem;
		}
		else
		{
			dataRow["Task"] = parsedValues[1];
		}

		if (parsedValues[4][parsedValues[4].Length - 1] == ')')
		{
			var startPos = parsedValues[4].IndexOf('(');

			if (startPos >= 0)
			{
				parsedValues[4] = parsedValues[4].Substring(0, startPos);
			}
		}
		else if (parsedValues[4].Contains(":"))
		{
			var startPos = parsedValues[4].LastIndexOf(':');

			if (startPos >= 0)
			{
				parsedValues[4] = parsedValues[4].Substring(0, startPos);
			}
		}

		dataRow["Item"] = parsedValues[4];

		if (parsedValues[4] != tableItem)
		{
			tableItemPos = -1;
			tableItemValuePos = -1;
		}

		var logDesc = new StringBuilder();
		var startRange = parsedValues[5] == "-" ? 6 : 5;
		int itemPos = -1;
		int itemValuePos = -1;

		if (parsedValues[startRange][0] == '(')
		{
			++startRange;
		}
		
		#endregion

		#region Describe Info
		for (int nCell = startRange; nCell < parsedValues.Count; ++nCell)
		{
			if (LookForIPAddress(parsedValues[nCell], ipAddress, out lineIPAddress))
			{
				dataRow["Assocated Item"] = lineIPAddress;
			}
			else if (parsedValues[nCell].ToLower().Contains("exception"))
			{
				var exceptionLine = fileLines[nLine + 1].Trim();
				var exceptionEndPos = exceptionLine.IndexOf(' ');
				var exceptionClass = exceptionEndPos > 0 ? exceptionLine.Substring(0, exceptionEndPos) : null;

				if (!string.IsNullOrEmpty(exceptionClass) && exceptionClass[exceptionClass.Length - 1] == ':')
				{
					dataRow["Exception"] = exceptionClass.Substring(0, exceptionClass.Length - 1);
					dataRow["Exception Description"] = exceptionLine.Substring(exceptionEndPos + 1).TrimStart();
					++nLine;
				}
			}
			else if(parsedValues[4] == "CompactionController.java")
			{
				//Compacting large row billing/account_payables:20160726:FMCC (348583137 bytes)
				if (itemPos == nCell)
				{
					var ksTableName = parsedValues[nCell];
					var keyDelimatorPos = ksTableName.IndexOf(':');
										
					if (keyDelimatorPos > 0)
					{
						ksTableName = ksTableName.Substring(0,keyDelimatorPos).Replace('/','.');
						
						var splitItems = SplitTableName(ksTableName, null);
						
						ksTableName = splitItems.Item1 + '.' + splitItems.Item2;
					}
					
					dataRow["Assocated Item"] = ksTableName;
					
				}
				if (nCell >= itemPos && parsedValues[nCell][parsedValues[nCell].Length - 1] == ')')
				{
					var firstParan = parsedValues[nCell].IndexOf('(');

					if (firstParan >= 0)
					{
						dataRow["Assocated Value"] = ConvertInToMB(parsedValues[nCell].Substring(firstParan + 1, parsedValues[nCell].Length - firstParan - 2));
					}
				}

				if (parsedValues[nCell] == "large" && parsedValues.ElementAtOrDefault(nCell + 1) == "row")
				{
					itemPos = nCell + 2;
					dataRow["Flagged"] = true;
				}
			}
			else if (parsedValues[4] == "GCInspector.java")
			{
				//GCInspector.java (line 116) GC for ParNew: 394 ms for 1 collections, 13571498424 used; max is 25340346368
				//GCInspector.java (line 119) GC forConcurrentMarkSweep: 15132 ms for 2 collections, 4229845696 used; max is 25125584896
				//GCInspector.java:258 - G1 Young Generation GC in 264ms.  G1 Eden Space: 3470786560 -> 0; G1 Old Gen: 2689326672 -> 2934172000; G1 Survivor Space: 559939584 -> 35651584; 
				if (nCell == itemPos)
				{
					var time = DetermineTime(parsedValues[nCell]);

					if (time is int && (int)time >= gcPausedFlagThresholdInMS)
					{
						dataRow["Flagged"] = true;
						dataRow["Assocated Value"] = DetermineTime(parsedValues[nCell]);
					}
				}
				if (parsedValues[nCell] == "ParNew:"
						|| parsedValues[nCell] == "forConcurrentMarkSweep:"
						|| parsedValues[nCell] == "ConcurrentMarkSweep:")
				{
					itemPos = nCell + 1;
				}
				else if (parsedValues[nCell] == "Young")
				{
					itemPos = nCell + 4;
				}
			}
			else if (parsedValues[4] == "BatchStatement.java")
			{
				//BatchStatement.java (line 226) Batch of prepared statements for [clearcore.documents_case] is of size 71809, exceeding specified threshold of 65536 by 6273.
				if (nCell == itemPos)
				{
					var ksTableName = parsedValues[nCell];
					
					if (ksTableName[0] == '[')
					{
						ksTableName = ksTableName.Substring(1,ksTableName.Length - 2);

						var splitItems = SplitTableName(ksTableName, null);

						ksTableName = splitItems.Item1 + '.' + splitItems.Item2;
					}
					
					dataRow["Assocated Item"] = ksTableName;
				}
				if (nCell == itemValuePos)
				{
					object batchSize;

					if (StringFunctions.ParseIntoNumeric(parsedValues[nCell], out batchSize))
					{
						dataRow["Assocated Value"] = batchSize;
					}
				}
				if (parsedValues[nCell] == "Batch")
				{					
					itemPos = nCell + 5;
					itemValuePos = nCell + 9;
				}
			}
			else if (parsedValues[4] == "SliceQueryFilter.java")
			{
				//SliceQueryFilter.java (line 231) Read 14 live and 1344 tombstoned cells in cma.mls_records_property (see tombstone_warn_threshold). 5000 columns was requested, slices=[-]
				if (nCell == itemPos)
				{
					var splitItems = SplitTableName(parsedValues[nCell], null);

					dataRow["Assocated Item"] = splitItems.Item1 + '.' + splitItems.Item2;
				}
				if (nCell == itemValuePos)
				{
					object tbNum;

					if (StringFunctions.ParseIntoNumeric(parsedValues[nCell], out tbNum))
					{
						dataRow["Assocated Value"] = tbNum;
					}
				}
				if (parsedValues[nCell] == "Read")
				{
					itemPos = nCell + 8;
					itemValuePos = nCell + 4;
				}
			}
			else if (parsedValues[4] == "StatusLogger.java")
			{
				//StatusLogger.java:51 - Pool Name                    Active   Pending      Completed   Blocked  All Time Blocked
				//StatusLogger.java:66 - MutationStage                     4         0     2383662788         0                 0
				//StatusLogger.java:75 - CompactionManager                 2         3
				//StatusLogger.java:87 - MessagingService                n/a       0/1
				//
				//StatusLogger.java:97 - Cache Type                     Size                 Capacity               KeysToSave
				//StatusLogger.java:99 - KeyCache                   95002384                104857600                      all
				//
				//StatusLogger.java:112 - ColumnFamily                Memtable ops,data
				//StatusLogger.java:115 - dse_perf.node_slow_log           8150,3374559
								
				if (parsedValues[nCell] == "ColumnFamily")
				{
					tableItem = parsedValues[4];
					tableItemPos = nCell;
				}
				else if (parsedValues[nCell] == "Pool")
				{
					tableItem = null;
					tableItemPos = -1;
				}
				else if (parsedValues[nCell] == "Cache")
				{
					tableItem = null;
					tableItemPos = -1;
				}
				else if (nCell == tableItemPos)
				{
					var splitItems = SplitTableName(parsedValues[nCell], null);

					dataRow["Assocated Item"] = splitItems.Item1 + '.' + splitItems.Item2;
				}
			}

			logDesc.Append(' ');
			logDesc.Append(parsedValues[nCell]);
		}

		dataRow["Description"] = logDesc;
		
		#endregion
		
		dtCLog.Rows.Add(dataRow);
	
		lastRow = dataRow;
		
	}
	
	LogCassandraMaxTimestamps.Add(maxTimestamp);
}

void ReadCQLDDLParseIntoDataTable(IFilePath cqlDDLFilePath,
									string ipAdress,
									string dcName,
									System.Data.DataTable dtKeySpace,
									System.Data.DataTable dtTable,
									Dictionary<string, int> cqlHashCheck,
									IEnumerable<string> ignoreKeySpaces)
{

	if (dtKeySpace.Columns.Count == 0)
	{
		dtKeySpace.Columns.Add("Name", typeof(string));
		
		dtKeySpace.Columns.Add("Replication Strategy", typeof(string));
		dtKeySpace.Columns.Add("DataCenter", typeof(string));
		dtKeySpace.Columns.Add("Replication Factor", typeof(int));
		dtKeySpace.Columns.Add("DDL", typeof(string));

		dtKeySpace.PrimaryKey = new System.Data.DataColumn[] { dtKeySpace.Columns["Name"], dtKeySpace.Columns["DataCenter"] };
	}

	if (dtTable.Columns.Count == 0)
	{
		dtTable.Columns.Add("Keyspace Name", typeof(string));
		dtTable.Columns.Add("Name", typeof(string));
		dtTable.Columns.Add("Pritition Key", typeof(string));
		dtTable.Columns.Add("Cluster Key", typeof(string));
		dtTable.Columns["Cluster Key"].AllowDBNull = true;
		dtTable.Columns.Add("Compaction Strategy", typeof(string));
		dtTable.Columns.Add("DDL", typeof(string));
		
		dtTable.PrimaryKey = new System.Data.DataColumn[] { dtTable.Columns["Keyspace Name"], dtTable.Columns["Name"] };
	}
 

	var fileLines = cqlDDLFilePath.ReadAllLines();
	string line;
	var strCQL = new StringBuilder();
	List<string> parsedValues;
	List<string> parsedComponent;
	string currentKeySpace = null;
	DataRow dataRow;
	
	for (int nLine = 0; nLine < fileLines.Length; ++nLine)
	{
		line = fileLines[nLine].Trim();
		
		if (string.IsNullOrEmpty(line)
				|| line.Substring(0, 2) == "//"
				|| line.Substring(0, 2) == "--")
		{
			continue;
		}
		else if (line.Substring(0, 2) == "/*")
		{
			for (; nLine < fileLines.Length || line.EndsWith("*/"); ++nLine)
			{
				line = fileLines[nLine].Trim();
			}
		}
		
		strCQL.Append(" ");
		strCQL.Append(line);

		if (line[line.Length - 1] == ';')
		{
			string cqlStr = strCQL.ToString().TrimStart();
			strCQL.Clear();

			if (cqlStr.ToLower().StartsWith("use "))
			{
				parsedValues = Common.StringFunctions.Split(cqlStr,
																new char[] { ' ', ';' },
																Common.StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
																	| Common.StringFunctions.IgnoreWithinDelimiterFlag.Text
																	| Common.StringFunctions.IgnoreWithinDelimiterFlag.Bracket,
																Common.StringFunctions.SplitBehaviorOptions.Default
																	| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
																	
				currentKeySpace = RemoveQuotes(parsedValues.Last());
				continue;
			}

			parsedValues = Common.StringFunctions.Split(cqlStr,
														new char[] { ',', '{', '}'},
														Common.StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
															| Common.StringFunctions.IgnoreWithinDelimiterFlag.Text
															| Common.StringFunctions.IgnoreWithinDelimiterFlag.Bracket
															| Common.StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
														Common.StringFunctions.SplitBehaviorOptions.Default
															| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

			if (parsedValues[0].StartsWith("create", StringComparison.OrdinalIgnoreCase))
			{
				
				if (parsedValues[0].Substring(6,9).TrimStart().ToLower() == "keyspace")
				{
					parsedComponent = Common.StringFunctions.Split(parsedValues[0],
																	' ',
																	Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																	Common.StringFunctions.SplitBehaviorOptions.Default
																		| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
																		
					//CREATE KEYSPACE billing WITH replication =
					//'class': 'NetworkTopologyStrategy'
					//'us-west-2': '3'
					//;

					var ksName = RemoveQuotes(parsedComponent[parsedComponent.Count() - 4]);

					if (ignoreKeySpaces.Contains(ksName))
					{
						continue;
					}
					
					parsedComponent = Common.StringFunctions.Split(parsedValues[1],
																	':',
																	Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																	Common.StringFunctions.SplitBehaviorOptions.Default
																		| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
					
					var ksStratery = RemoveNamespace(parsedComponent.Last().Trim());

					for (int nIndex = 2; nIndex < parsedValues.Count - 1; ++nIndex)
					{
						dataRow = dtKeySpace.NewRow();
						dataRow["Name"] = ksName;
						dataRow["Replication Strategy"] = ksStratery;

						parsedComponent = Common.StringFunctions.Split(parsedValues[nIndex],
																		':',
																		Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																		Common.StringFunctions.SplitBehaviorOptions.Default
																			| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

						dataRow["DataCenter"] = RemoveQuotes(parsedComponent[0]);
						dataRow["Replication Factor"] = int.Parse(RemoveQuotes(parsedComponent[1]));
						dataRow["DDL"] = cqlStr;
						
						dtKeySpace.Rows.Add(dataRow);
					}

				}
				else if (parsedValues[0].Substring(6,6).TrimStart().ToLower() == "table")
				{
					//CREATE TABLE account_payables(date int, org_key text, product_type text, product_id bigint, product_update_id bigint, vendor_type text, parent_product_id bigint, parent_product_type text, parent_product_update_id bigint, user_id bigint, vendor_detail text, PRIMARY KEY((date, org_key), product_type, product_id, product_update_id, vendor_type)) WITH bloom_filter_fp_chance = 0.100000 AND caching = 'KEYS_ONLY' AND comment = '' AND dclocal_read_repair_chance = 0.100000 AND gc_grace_seconds = 864000 AND index_interval = 128 AND read_repair_chance = 0.000000 AND replicate_on_write = 'true' AND populate_io_cache_on_flush = 'false' AND default_time_to_live = 0 AND speculative_retry = '99.0PERCENTILE' AND memtable_flush_period_in_ms = 0 AND compaction =
					//		'class': 'LeveledCompactionStrategy'
					//AND compression =
					//'sstable_compression': 'LZ4Compressor'
					//;					
					var startParan = cqlStr.IndexOf('(');
					var endParan = cqlStr.LastIndexOf(')');
					var strFrtTbl = cqlStr.Substring(0,startParan);
					var strColsTbl = cqlStr.Substring(startParan + 1,endParan - startParan - 1);
					var strOtpsTbl = cqlStr.Substring(endParan + 1);
					
					//Split to Find Table Name
					parsedComponent = Common.StringFunctions.Split(strFrtTbl,
																	' ',
																	Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																	Common.StringFunctions.SplitBehaviorOptions.Default
																		| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

					var kstblName = SplitTableName(parsedComponent.Last(), currentKeySpace);

					if (ignoreKeySpaces.Contains(kstblName.Item1))
					{
						continue;
					}

					dataRow = dtTable.NewRow();
					dataRow["Keyspace Name"] = kstblName.Item1;
					dataRow["Name"] = kstblName.Item2;
					dataRow["DDL"] = cqlStr;
					
					//Find Columns
					var tblColumns = Common.StringFunctions.Split(strColsTbl,
																	',',
																	Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																	Common.StringFunctions.SplitBehaviorOptions.Default
																		| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

					
					if (tblColumns.Last().StartsWith("PRIMARY KEY", StringComparison.OrdinalIgnoreCase))
					{
						var pkClause = tblColumns.Last();
						startParan = pkClause.IndexOf('(');
						endParan = pkClause.LastIndexOf(')');

						var pckList = Common.StringFunctions.Split(pkClause.Substring(startParan + 1, endParan - startParan - 1),
																		',',
																		Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																		Common.StringFunctions.SplitBehaviorOptions.Default
																			| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries)
											.Select(sf => sf.Trim());

						var pkLocation = pckList.First();
						if (pkLocation[0] == '(')
						{
							startParan = pkLocation.IndexOf('(');
							endParan = pkLocation.LastIndexOf(')');

							var pkList = Common.StringFunctions.Split(pkLocation.Substring(startParan + 1, endParan - startParan - 1),
																				',',
																				Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																				Common.StringFunctions.SplitBehaviorOptions.Default
																					| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries)
											.Select(sf => sf.Trim());
							var pkdtList = new List<string>();
							
							foreach (var element in pkList)
							{
								pkdtList.Add(tblColumns.Find(c => c.StartsWith(element)));
							}
							dataRow["Pritition Key"] = string.Join(", ", pkdtList);
						}
						else
						{
							dataRow["Pritition Key"] = tblColumns.Find(c => c.StartsWith(pkLocation));
						}

						var cdtList = new List<string>();

						for (int nIndex = 1; nIndex < pckList.Count(); ++nIndex)
						{
							cdtList.Add(tblColumns.Find(c => c.StartsWith(pckList.ElementAt(nIndex))));
						}
                    	dataRow["Cluster Key"] = string.Join(", ", cdtList);
					}
					else
					{
						//look for keyworad Primary Key
						var pkVar = tblColumns.Find(c => c.EndsWith("primary key", StringComparison.OrdinalIgnoreCase));
						 
						dataRow["Pritition Key"] = pkVar.Substring(0, pkVar.Length - 11).TrimEnd();	
						dataRow["Cluster Key"] = null;
					}

					//parse options...
					parsedComponent = Common.StringFunctions.Split(strOtpsTbl.Substring(5).TrimStart(),
																	" and ",
																	StringComparison.OrdinalIgnoreCase,
																	Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																	Common.StringFunctions.SplitBehaviorOptions.Default
																		| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
					string optKeyword;
					
					for(int nIndex = 0; nIndex < parsedComponent.Count; ++nIndex)
					{
						optKeyword = parsedComponent[nIndex].TrimStart();
						
						if (optKeyword.StartsWith("compaction", StringComparison.OrdinalIgnoreCase))
						{
							var kwOptions = ParseKeyValuePair(optKeyword).Item2;
							var classPos = kwOptions.IndexOf("class");
							var classSplit = kwOptions.Substring(classPos).Split(new char[] { ':', ',', '}'});
							var strategy = classSplit[1].Trim();
							dataRow["Compaction Strategy"] = RemoveNamespace(strategy);
						}
							
					}
					
					dtTable.Rows.Add(dataRow);
				}
			}
		}
	}
}

void ReadCompactionHistFileParseIntoDataTable(IFilePath cmphistFilePath,
												string ipAddress,
												string dcName,
												System.Data.DataTable dtCmpHist,
												DataTable dtTable,
												IEnumerable<string> ignoreKeySpaces)
{
	if (dtCmpHist.Columns.Count == 0)
	{
		dtCmpHist.Columns.Add("Data Center", typeof(string));
		dtCmpHist.Columns[0].AllowDBNull = true;
		dtCmpHist.Columns.Add("Node IPAdress", typeof(string));
		dtCmpHist.Columns.Add("KeySpace", typeof(string));
		dtCmpHist.Columns.Add("Table", typeof(string));
		dtCmpHist.Columns.Add("Compaction Timestamp (UTC)", typeof(DateTime));
		dtCmpHist.Columns.Add("SSTable Size Before", typeof(long));
		dtCmpHist.Columns.Add("Before Size (MB)", typeof(decimal));
		dtCmpHist.Columns.Add("SSTable Size After", typeof(long));
		dtCmpHist.Columns.Add("After Size (MB)", typeof(decimal));
		dtCmpHist.Columns.Add("Compaction Strategy", typeof(string));
		dtCmpHist.Columns["Compaction Strategy"].AllowDBNull = true;
		dtCmpHist.Columns.Add("Partitions Merged (tables:rows)", typeof(string));
		
		//dtFSStats.PrimaryKey = new System.Data.DataColumn[] { dtFSStats.Columns[0],  dtFSStats.Columns[1],  dtFSStats.Columns[2],  dtFSStats.Columns[3], dtFSStats.Columns[4] };
	}


	var fileLines = cmphistFilePath.ReadAllLines();
	string line;
	DataRow dataRow;
	DataRow ksDataRow;
	List<string> parsedLine;
	string currentKeySpace;
	string currentTable;
	int offSet;

	foreach (var element in fileLines)
	{
		line = element.Trim();

		if (string.IsNullOrEmpty(line) 
				|| line.StartsWith("Compaction History", StringComparison.OrdinalIgnoreCase)
				|| line.StartsWith("id ", StringComparison.OrdinalIgnoreCase))
		{
			continue;
		}

		//Compaction History: 
		//id 									keyspace_name      	columnfamily_name 	compacted_at		bytes_in 	bytes_out      					rows_merged
		//cfde9db0-3d06-11e6-adbd-0fa082120add 	production_mqh_bi  	bi_newdata			1467101014795		247011505	247011472      					{ 1:354, 2:1}
		//																				timestamp			size SSTtable before and after compaction	the number of partitions merged. The notation means {tables:rows}. For example: {1:3, 3:1} means 3 rows were taken from one SSTable (1:3) and 1 row taken from 3 SSTables (3:1) to make the one SSTable in that compaction operation.
		//	0										1				2					3					4			5								6
		parsedLine = Common.StringFunctions.Split(line,
													' ',
													Common.StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace,
													Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

		if (parsedLine.Count > 6)
		{
			currentKeySpace = RemoveQuotes(parsedLine[1]);
			currentTable = RemoveQuotes(parsedLine[2]);
			offSet = 0;
		}
		else
		{
			if (parsedLine[1].Length > 20)
			{
				currentKeySpace = RemoveQuotes(parsedLine[1].Substring(0, 19));
				currentTable = RemoveQuotes(parsedLine[1].Substring(19));
				offSet = 1;
			}
			else
			{
				currentKeySpace = RemoveQuotes(parsedLine[1]);

				if (parsedLine[2].Length > 30)
				{
					currentTable = RemoveQuotes(parsedLine[2].Substring(0, 29));					
					parsedLine[2] = parsedLine[2].Substring(29);
					offSet = 1;
				}
				else
				{
					currentTable = RemoveQuotes(parsedLine[2]);
					offSet = 0;
				}
			}
		}

		if (ignoreKeySpaces.Contains(currentKeySpace))
		{
			continue;
		}
		
		dataRow = dtCmpHist.NewRow();

		dataRow["Data Center"] = dcName;
		dataRow["Node IPAdress"] = ipAddress;
		dataRow["KeySpace"] = currentKeySpace;
		dataRow["Table"] = currentTable;
		dataRow["Compaction Timestamp (UTC)"] = FromUnixTime(parsedLine[3 - offSet]);
		dataRow["SSTable Size Before"] = long.Parse(parsedLine[4 - offSet]);
		dataRow["Before Size (MB)"] = ConvertInToMB(parsedLine[4 - offSet], "MB");
		dataRow["SSTable Size After"] = long.Parse(parsedLine[5 - offSet]);
		dataRow["After Size (MB)"] = ConvertInToMB(parsedLine[5- offSet], "MB");
		dataRow["Partitions Merged (tables:rows)"] = parsedLine[6 - offSet];

		ksDataRow = dtTable.Rows.Find(new object[] {currentKeySpace, currentTable});

		if (ksDataRow != null)
		{
			dataRow["Compaction Strategy"] = ksDataRow["Compaction Strategy"];
		}
		
		dtCmpHist.Rows.Add(dataRow);
	}
}

void ReadInfoFileParseIntoDataTable(IFilePath infoFilePath,
									string ipAddress,
									string dcName,
									DataTable dtRingInfo)
{
	var fileLines = infoFilePath.ReadAllLines();
	string line;
	DataRow dataRow = dtRingInfo.Rows.Find(ipAddress);

	if (dataRow == null)
	{
		Console.WriteLine("Warning: IP Address {0} was not found in the \"nodetool ring\" file but was found within the \"nodetool info\" file.", ipAddress);
		return;
	}
	
	string lineCommand;
	string lineValue;
	int delimitorPos;

	dataRow.BeginEdit();
	
	foreach (var element in fileLines)
	{
		line = element.Trim();

		if (string.IsNullOrEmpty(line))
		{
			continue;
		}

		delimitorPos = line.IndexOf(':');

		if (delimitorPos <= 0)
		{
			continue;
		}
		
		lineCommand = line.Substring(0, delimitorPos).Trim().ToLower();
		lineValue = line.Substring(delimitorPos + 1).Trim();

		switch (lineCommand)
		{
			case "gossip active":
				dataRow["Gossip Enableed"] = bool.Parse(lineValue);
				break;
			case "thrift active":
				dataRow["Thrift Enabled"] = bool.Parse(lineValue);
				break;
			case "native transport active":
				dataRow["Native Transport Enable"] = bool.Parse(lineValue);
				break;
			case "load":
				dataRow["Storage Used (MB)"] = ConvertInToMB(lineValue);
				break;
			case "generation no":
				//dataRow["Number of Restarts"] = int.Parse(lineValue);
				break;
			case "uptime (seconds)":
				dataRow["Uptime"] = new TimeSpan(0,0,int.Parse(lineValue));
				break;
			case "heap memory (mb)":
				dataRow["Heap Memory (MB)"] = lineValue;
				break;
			case "off heap memory (mb)":
				dataRow["Off Heap Memory (MB)"] = decimal.Parse(lineValue);
				break;
			case "id":
			case "token":
			case "data center":
			case "rack":
				break;
			case "exceptions":
				dataRow["Nbr of Exceptions"] = int.Parse(lineValue);
				break;
			case "key cache":
				dataRow["Key Cache Information"] = lineValue;
				break;
			case "row cache":
				dataRow["Row Cache Information"] = lineValue;
				break;
			case "counter cache":
				dataRow["Counter Cache Information"] = lineValue;
				break;
			default:
				line.Dump("\"nodetool info\" Invalid line found.");
				break;
		}
	}
	
	dataRow.EndEdit();
	dataRow.AcceptChanges();
}

void ReadDSEToolRingFileParseIntoDataTable(IFilePath dseRingFilePath,
											DataTable dtRingInfo)
{
	var fileLines = dseRingFilePath.ReadAllLines();
	string line;
	List<string> parsedLine;
	string ipAddress;
	DataRow dataRow;

//Note: Ownership information does not include topology, please specify a keyspace.
//Address 			DC			Rack 	Workload	Status 	State	Load 		Owns	VNodes
//10.27.34.17 		DC1 		RAC1	Cassandra 	Up		Normal	48.36 GB	6.31 % 	256
//Warning: Node 10.27.34.54 is serving 1.20 times the token space of node 10.27.34.52, which means it will be using 1.20 times more disk space and network bandwidth.If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management
//Warning: Node 10.27.34.12 is serving 1.11 times the token space of node 10.27.34.21, which means it will be using 1.11 times more disk space and network bandwidth.If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management

	foreach (var element in fileLines)
	{
		line = element.Trim();

		if (string.IsNullOrEmpty(line)
			|| line.StartsWith("warning: ", StringComparison.OrdinalIgnoreCase)
			|| line.StartsWith("note: ", StringComparison.OrdinalIgnoreCase))
		{
			continue;
		}

		parsedLine = Common.StringFunctions.Split(line,
													' ',
													Common.StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace,
													Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

		if (IPAddressStr(parsedLine[0], out ipAddress))
		{
			dataRow = dtRingInfo.Rows.Find(ipAddress);


			if (dataRow == null)
			{
				Console.WriteLine("Warning: IP Address {0} was not found in the \"nodetool ring\" file but was found within the \"dsetool ring\" file. Ring information added.", ipAddress);
				
				dataRow = dtRingInfo.NewRow();

				dataRow["Node IPAdress"] = ipAddress;
				dataRow["DataCenter"] = parsedLine[1];
				dataRow["Rack"] = parsedLine[2];
				dataRow["Status"] = parsedLine[4];
				dataRow["Instance Type"] = parsedLine[3];
				dataRow["Storage Used (MB)"] = ConvertInToMB(parsedLine[6], parsedLine[7]);
				dataRow["Storage Utilization"] = decimal.Parse(parsedLine[8].LastIndexOf('%') >= 0 
																? parsedLine[8].Substring(0, parsedLine[8].Length - 1)
																:  parsedLine[8]) / 100m;
				dataRow["Nbr VNodes"] = int.Parse(parsedLine[9][0] == '%' ? parsedLine[10] : parsedLine[9]);
				
				dtRingInfo.Rows.Add(dataRow);
			}
			else
			{
				dataRow.BeginEdit();

				dataRow["Instance Type"] = parsedLine[3];
				dataRow["Storage Utilization"] = decimal.Parse(parsedLine[8].LastIndexOf('%') >= 0
																? parsedLine[8].Substring(0, parsedLine[8].Length - 1)
																: parsedLine[8]) / 100m;
				dataRow["Storage Used (MB)"] = ConvertInToMB(parsedLine[6], parsedLine[7]);
				dataRow["Nbr VNodes"] = int.Parse(parsedLine[9][0] == '%' ? parsedLine[10] : parsedLine[9]);
				
				dataRow.EndEdit();
				dataRow.AcceptChanges();
			}
		}
	}

	
}

class YamlInfo
{
	public string YamlType;
	public string IPAddress;
	public string DCName;
	public string Cmd;
	public string CmdParams;
	public IEnumerable<Tuple<string,string>> KeyValueParams;
	
	public string MakeKeyValue()
	{
		return this.DCName
					+ ": "
					+ this.Cmd
					+ ": "
					+ (this.KeyValueParams == null
							? this.CmdParams
							: string.Join(" ", this.KeyValueParams.Select(kvp => kvp.Item1 + ": " + kvp.Item2)));
    }

	public bool ComparerProperyOnly(YamlInfo compareItem)
	{
		return this.DCName == compareItem.DCName
				&& this.Cmd == compareItem.Cmd
				&& (this.KeyValueParams == null
					|| (this.KeyValueParams.Count() == compareItem.KeyValueParams.Count()
							&& this.KeyValueParams.All(item => compareItem.KeyValueParams.Where(kvp => kvp.Item1 == item.Item1).Count() > 0)));
	}
	
	public string ProperyName()
	{
		return this.Cmd + (this.KeyValueParams == null
								? string.Empty
								: "." + string.Join(".", this.KeyValueParams.Select(kvp => kvp.Item1)));
	}

	public string ProperyName(int inxProperty)
	{
		return this.Cmd + (this.KeyValueParams == null || inxProperty == 0
								? string.Empty
								: "." + this.KeyValueParams.ElementAt(inxProperty - 1).Item1);
	}

	public object ProperyValue(int inxProperty)
	{
		string strValue = this.KeyValueParams == null || inxProperty == 0
								? this.CmdParams
								: this.KeyValueParams.ElementAt(inxProperty - 1).Item2;
		object numValue;

		if (StringFunctions.ParseIntoNumeric(strValue, out numValue))
		{
			return numValue;
		}
		else if(strValue == "false")
		{
			return false;
		}
		else if (strValue == "true")
		{
			return true;
		}

		return strValue;
	}

	public bool AddValueToDR(DataTable dtYamal)
	{
		if (this.KeyValueParams == null)
		{
			var dataRow = dtYamal.NewRow();

			if (this.AddValueToDR(dataRow, 0))
			{
				dtYamal.Rows.Add(dataRow);
				return true;
			}
			
			return false;
		}
		
		for (int i = 1; i <= this.KeyValueParams.Count(); i++)
		{
			var dataRow = dtYamal.NewRow();

			if (this.AddValueToDR(dataRow, i))
			{
				dtYamal.Rows.Add(dataRow);
			}
		}
		
		return true;
	}

	public bool AddValueToDR(DataRow drYama, int inxProperty)
	{
		var maxIndex = this.KeyValueParams == null ? 0 : this.KeyValueParams.Count();

		if (inxProperty > maxIndex)
		{
			return false;
		}
		
		drYama["Yaml Type"] = this.YamlType;
		drYama["Data Center"] = this.DCName;
		drYama["Node IPAdress"] = this.IPAddress;
		drYama["Property"] = this.ProperyName(inxProperty);
		drYama["Value"] = this.ProperyValue(inxProperty);
		
		return true;
	}
}

void ReadYamlFileParseIntoList(IFilePath yamlFilePath,
										string ipAddress,
										string dcName,
										string yamlType,
										List<YamlInfo> yamlList)
{
	var fileLines = yamlFilePath.ReadAllLines();
	string line;
	int posCmdDel;
	string strCmd;
	string parsedValue;
	bool optionsCmdParamsFnd = false;
	bool optionsBrace = false;
	List<string> separateParams;

//seed_provider:
//# Addresses of hosts that are deemed contact points.
//# Cassandra nodes use this list of hosts to find each other and learn
//# the topology of the ring.  You must change this if you are running
//# multiple nodes!
//	-class_name: org.apache.cassandra.locator.SimpleSeedProvider
//	 parameters:
//          # seeds is actually a comma-delimited list of addresses.
//          # Ex: "<ip1>,<ip2>,<ip3>"
//          -seeds: "10.27.34.11,10.27.34.12"
//
//concurrent_reads: 32
//
//server_encryption_options:
//	internode_encryption: none
//	keystore: resources/dse/conf /.keystore
//	keystore_password:  cassandra
//	truststore: resources/dse/conf/.truststore
//    truststore_password: cassandra
//    # More advanced defaults below:
//    # protocol: TLS
//    # algorithm: SunX509
//    # store_type: JKS
//    # cipher_suites: [TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_CBC_SHA,TLS_DHE_RSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA]
//    # require_client_auth: false
//
// node_health_options: {enabled: false, refresh_rate_ms: 60000}
//
// cassandra_audit_writer_options: {mode: sync, batch_size: 50, flush_time: 500, num_writers: 10,
//		queue_size: 10000, write_consistency: QUORUM}

	for (int nIndex = 0; nIndex < fileLines.Length; ++nIndex)
	{
		line = fileLines[nIndex].Trim();

		if (string.IsNullOrEmpty(line)
			|| line[0] == '#'
			|| line.StartsWith("if ")
			|| line == "fi")
		{
			continue;
		}

		if (line[0] == '-')
		{
			parsedValue = RemoveCommentInLine(line.Substring(1).TrimStart().RemoveConsecutiveChar());
			yamlList.Last().CmdParams += ' ' + parsedValue;
			continue;
		}
		else if (optionsBrace)
		{
			parsedValue = RemoveCommentInLine(line.RemoveConsecutiveChar());
			yamlList.Last().CmdParams += ' ' + parsedValue;
			optionsBrace = !(parsedValue.Length > 0 && parsedValue[parsedValue.Length - 1] == '}');
			continue;
		}
		else if (line.StartsWith("parameters:")
					|| optionsCmdParamsFnd && fileLines[nIndex][0] == ' ')
		{
			parsedValue = RemoveCommentInLine(line.RemoveConsecutiveChar());
			yamlList.Last().CmdParams += ' ' + parsedValue;
			continue;
		}
		
		if (optionsCmdParamsFnd)
		{
			optionsCmdParamsFnd = false;
		}

		posCmdDel = line.IndexOf(':');

		if (posCmdDel < 0)
		{
			posCmdDel = line.IndexOf('=');

			if (posCmdDel < 0)
			{
				parsedValue = RemoveCommentInLine(line.RemoveConsecutiveChar());
				yamlList.Last().CmdParams += ' ' + parsedValue;
				continue;
			}
		}

		strCmd = line.Substring(0, posCmdDel);

		if (strCmd.EndsWith("_options"))
		{
			optionsCmdParamsFnd = true;
		}

		parsedValue = RemoveCommentInLine(line.Substring(posCmdDel + 1).Trim().RemoveConsecutiveChar());

		if (parsedValue.Length > 2 && parsedValue[0] == '{')
		{
			if (parsedValue[parsedValue.Length - 1] != '}')
			{
				optionsBrace = true;
			}
		}

		yamlList.Add(new YamlInfo()
							{
								YamlType = yamlType,
								Cmd = strCmd,
								DCName = dcName,
								IPAddress = ipAddress,
								CmdParams = parsedValue
							});
	}

	foreach (var element in yamlList)
	{
		separateParams = Common.StringFunctions.Split(element.CmdParams,
														new char[] { ',', ' ', '=' },
														Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
														Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

		if (separateParams.Count <= 1)
		{
			element.CmdParams = DetermineProperFormat(separateParams.FirstOrDefault());
		}
		else
		{
			var keyValues = new List<Tuple<string, string>>();
			string subCmd = string.Empty;
			bool optionsFnd = false;
			
			for (int nIndex = 0; nIndex < separateParams.Count; ++nIndex)
			{
				if (separateParams[nIndex][separateParams[nIndex].Length - 1] == ':')
				{
					separateParams[nIndex] = separateParams[nIndex].Substring(0,separateParams[nIndex].Length - 1);

					if(separateParams[nIndex + 1][separateParams[nIndex + 1].Length - 1] == ':')
					{
						subCmd = separateParams[nIndex] + '.';
						++nIndex;
						separateParams[nIndex] = separateParams[nIndex].Substring(0,separateParams[nIndex].Length - 1);
					}				
				}
				
				if (separateParams[nIndex].EndsWith("_options"))
				{
					optionsFnd = true;
					subCmd += separateParams[nIndex] + '.';
				}
				else if (optionsFnd)
				{
					keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false), DetermineProperFormat(separateParams[++nIndex])));
				}
				else if (separateParams[nIndex] != "parameters")
				{
					optionsFnd = false;
					
					if (separateParams[nIndex + 1].Length > 0 && separateParams[nIndex + 1][0] == '{')
                    {
						subCmd += separateParams[nIndex] + '.';
						++nIndex;
						separateParams[nIndex] = separateParams[nIndex].Substring(1);
					}
					
					keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false), DetermineProperFormat(separateParams[++nIndex])));
				}				
			}
			element.KeyValueParams = keyValues.OrderBy(v => v.Item1);
		}
		
	}
}

void ParseYamlListIntoDataTable(Common.Patterns.Collections.LockFree.Stack<List<YamlInfo>> yamlStackList,
									DataTable dtCYaml)
{
	List<YamlInfo> yamlList;
	List<YamlInfo> masterYamlList = new List<YamlInfo>();

	while (yamlStackList.Pop(out yamlList))
	{
		masterYamlList.AddRange(yamlList);
	}

	if (masterYamlList.Count == 0)
	{
		return;
	}
	
	var removeDups = masterYamlList.DuplicatesRemoved( item => item.MakeKeyValue());

	if (dtCYaml.Columns.Count == 0)
	{
		dtCYaml.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
		dtCYaml.Columns.Add("Node IPAdress", typeof(string));
		dtCYaml.Columns.Add("Yaml Type", typeof(string));
		dtCYaml.Columns.Add("Property", typeof(string));
		dtCYaml.Columns.Add("Value", typeof(object));
	}

	var yamlItems = removeDups.ToArray();
	
	foreach (var element in yamlItems)
	{
		if (yamlItems.Count(i => i.ComparerProperyOnly(element)) < 2)
		{
			element.IPAddress = "<Common>";
		}
		
		element.AddValueToDR(dtCYaml);
	}
}

class CLogSummaryInfo : IEqualityComparer<CLogSummaryInfo>, IEquatable<CLogSummaryInfo>
{
	public CLogSummaryInfo(DateTime period, TimeSpan periodSpan, string itemType, string itemValue, DataRow dataRow)
	{
//		dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
//		dtCLog.Columns.Add("Node IPAdress", typeof(string));
//		dtCLog.Columns.Add("Timestamp", typeof(DateTime));
//		dtCLog.Columns.Add("Indicator", typeof(string));
//		dtCLog.Columns.Add("Task", typeof(string));
//		dtCLog.Columns.Add("Item", typeof(string));
//		dtCLog.Columns.Add("Exception", typeof(string)).AllowDBNull = true;
//		dtCLog.Columns.Add("Exception Description", typeof(string)).AllowDBNull = true;
//		dtCLog.Columns.Add("Assocated Item", typeof(string)).AllowDBNull = true;
//		dtCLog.Columns.Add("Assocated Value", typeof(object)).AllowDBNull = true;
//		dtCLog.Columns.Add("Description", typeof(string));
//		dtCLog.Columns.Add("Flagged", typeof(bool)).AllowDBNull = true;

		this.DataCenter = dataRow == null ? null : dataRow["Data Center"] as string;
		this.IPAddress = dataRow == null ? null : (string) dataRow["Node IPAdress"];
		this.AssocatedItem = dataRow == null ? null : dataRow["Assocated Item"] as string;
		this.ItemType = itemType;
		this.ItemValue = itemValue;
		this.Period = period;
		this.PeriodSpan = periodSpan;
		this.AggregationCount = 0;
	}

	public CLogSummaryInfo(DateTime period, TimeSpan periodSpan, string itemType, string itemValue, string assocatedItem, string ipAddress, string dcName)
	{
		this.DataCenter = dcName;
		this.IPAddress = ipAddress;
		this.ItemType = itemType;
		this.ItemValue = itemValue;
		this.Period = period;
		this.PeriodSpan = periodSpan;
		this.AssocatedItem = assocatedItem;
		this.AggregationCount = 0;
	}

	public string DataCenter;
	public string IPAddress;
	public DateTime Period;
	public TimeSpan PeriodSpan;
	public string ItemType;
	public string ItemValue;
	public string AssocatedItem;
	public int AggregationCount;
	
	public bool Equals(CLogSummaryInfo x, CLogSummaryInfo y)
	{
		if (x == null && y == null)
			return true;
		else if (x == null | y == null)
			return false;
			
		return x.IPAddress == y.IPAddress
				&& x.DataCenter == y.DataCenter
				&& x.ItemType == y.ItemType
				&& x.ItemValue == y.ItemValue
				&& x.AssocatedItem == y.AssocatedItem
				&& x.Period == y.Period;
	}

	public bool Equals(CLogSummaryInfo y)
	{
		if (y == null)
			return false;

		return this.IPAddress == y.IPAddress
				&& this.DataCenter == y.DataCenter
				&& this.ItemType == y.ItemType
				&& this.ItemValue == y.ItemValue
				&& this.AssocatedItem == y.AssocatedItem
				&& this.Period == y.Period;
	}

	public bool Equals(DateTime period, string itemType, string itemValue, DataRow dataRow)
	{
		if(dataRow == null) return false; 
		
		return this.IPAddress == (string) dataRow["Node IPAdress"]
				&& this.DataCenter == dataRow["Data Center"] as string
				&& this.ItemType == itemType
				&& this.ItemValue == itemValue
				&& this.AssocatedItem == dataRow["Assocated Item"] as string
				&& this.Period == period;
	}

	public int GetHashCode(CLogSummaryInfo x)
	{
		if(x == null) return 0;

		return (x.IPAddress + x.DataCenter + x.AssocatedItem + x.ItemType + x.ItemValue + x.Period).GetHashCode();
	}
}

void ParseCassandraLogIntoSummaryDataTable(DataTable dtroCLog,
											DataTable dtCSummaryLog,
											string ipAddress,
											string dcName,
											string[] logAggregateIndicators,
											string[] logAggregateAdditionalTaskExceptionItems,
											string[] ignoreTaskExceptionItems,
											IEnumerable<Tuple<DateTime, TimeSpan>> bucketFromAggregatePeriods)
{
	if (dtCSummaryLog.Columns.Count == 0)
	{
		dtCSummaryLog.Columns.Add("Timestamp Period", typeof(DateTime));
		dtCSummaryLog.Columns.Add("Aggregation Period", typeof(TimeSpan));
		dtCSummaryLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
		dtCSummaryLog.Columns.Add("Assocated Item", typeof(string)).AllowDBNull = true;
		dtCSummaryLog.Columns.Add("Value", typeof(string)).AllowDBNull = true;
		dtCSummaryLog.Columns.Add("Occurrences", typeof(int));
		dtCSummaryLog.Columns.Add("Node IPAddress", typeof(string)).AllowDBNull = true;						
		dtCSummaryLog.Columns.Add("Type", typeof(string)).AllowDBNull = true;		
		
		dtCSummaryLog.DefaultView.Sort = "[Timestamp Period] DESC, [Data Center], [Assocated Item], [Value]";
	}

	if (dtroCLog.Rows.Count > 0)
	{
		var segments = new List<Tuple<DateTime, DateTime, TimeSpan, List<CLogSummaryInfo>>>();
		DataRow dataSummaryRow;
		
		for(int nIndex = 0; nIndex < bucketFromAggregatePeriods.Count(); ++nIndex)
		{
			segments.Add(new Tuple<DateTime, DateTime, TimeSpan, List<CLogSummaryInfo>>(bucketFromAggregatePeriods.ElementAt(nIndex).Item1,
																							bucketFromAggregatePeriods.ElementAtOrDefault(nIndex + 1) == null
																								? DateTime.MinValue
																								: bucketFromAggregatePeriods.ElementAt(nIndex + 1).Item1,
																							bucketFromAggregatePeriods.ElementAt(nIndex).Item2,
																							new List<CLogSummaryInfo>()));
		}
		
		Parallel.ForEach(segments, element =>
		//foreach (var element in segments)
		{
			var dataView = new DataView(dtroCLog,
										string.Format("#{0}# >= [Timestamp] and #{1}# < [Timestamp]", 
														element.Item1,
														element.Item2),
										"[Timestamp] DESC",
										DataViewRowState.CurrentRows);
			
			var startPeriod = element.Item1;
			var endPeriod = startPeriod - element.Item3;

//			dataView.RowFilter.Dump();
//			element.Dump();
//			if (dataView.ToTable().Rows.Count.Dump() > 0)
//			{				
//				dataView.ToTable().Rows[0].Dump();
//				dataView.ToTable().Rows[dataView.ToTable().Rows.Count - 1].Dump();
//			}
			
			foreach (DataRow dataRow in dataView.ToTable().Rows)
			{
				if ((DateTime)dataRow["Timestamp"] < endPeriod)
				{
					startPeriod = endPeriod;
					endPeriod = startPeriod - element.Item3;

					if ((DateTime)dataRow["Timestamp"] < endPeriod)
					{
						var newBeginPeriod = ((DateTime)dataRow["Timestamp"]).RoundUp(element.Item3);

						if (element.Item4.Count > 0)
						{ 
							while (newBeginPeriod < startPeriod)
							{
								element.Item4.Add(new CLogSummaryInfo(startPeriod, element.Item3, null, null, null, ipAddress, dcName));
								startPeriod = startPeriod - element.Item3;
							}
						}
						
						startPeriod = newBeginPeriod;
						endPeriod = startPeriod - element.Item3;						
					}
				}

				//		dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
				//		dtCLog.Columns.Add("Node IPAdress", typeof(string));
				//		dtCLog.Columns.Add("Timestamp", typeof(DateTime));
				//		dtCLog.Columns.Add("Indicator", typeof(string));
				//		dtCLog.Columns.Add("Task", typeof(string));
				//		dtCLog.Columns.Add("Item", typeof(string));
				//		dtCLog.Columns.Add("Assocated Item", typeof(string)).AllowDBNull = true;
				//		dtCLog.Columns.Add("Assocated Value", typeof(object)).AllowDBNull = true;
				//		dtCLog.Columns.Add("Description", typeof(string));
				//		dtCLog.Columns.Add("Flagged", typeof(bool)).AllowDBNull = true;

				var indicator = (string)dataRow["Indicator"];
				var taskItem = (string)dataRow["Task"];
				var item = (string)dataRow["Item"];
				var exception = dataRow["Exception"] as string;

				if (ignoreTaskExceptionItems.Contains(taskItem)
					|| ignoreTaskExceptionItems.Contains(item)
					|| ignoreTaskExceptionItems.Contains(exception))
				{
					continue;
				}

				if (logAggregateAdditionalTaskExceptionItems.Contains(taskItem))
				{
					var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Task", taskItem, dataRow));

					if (summaryInfo == null)
					{
						summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Task", taskItem, dataRow);
						element.Item4.Add(summaryInfo);
					}

					++summaryInfo.AggregationCount;
				}
				else if (logAggregateAdditionalTaskExceptionItems.Contains(item))
				{
					if (indicator == "INFO")
					{
						bool? flagged = dataRow["Flagged"] as bool?;

						if (!flagged.HasValue || !flagged.Value)
						{
							continue;
						}
					}
					
					var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Item", item, dataRow));

					if (summaryInfo == null)
					{
						summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Item", item, dataRow);
						element.Item4.Add(summaryInfo);
					}

					++summaryInfo.AggregationCount;
				}
				else if (logAggregateAdditionalTaskExceptionItems.Contains(exception))
				{
					var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Exception", exception, dataRow));

					if (summaryInfo == null)
					{
						summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Exception", exception, dataRow);
						element.Item4.Add(summaryInfo);
					}

					++summaryInfo.AggregationCount;
				}
				else if (!string.IsNullOrEmpty(exception))
				{
					var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Exception", exception, dataRow));

					if (summaryInfo == null)
					{
						summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Exception", exception, dataRow);
						element.Item4.Add(summaryInfo);
					}

					++summaryInfo.AggregationCount;
				}
				else if (logAggregateIndicators.Contains(indicator))
				{
					var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Indicator", indicator, dataRow));

					if (summaryInfo == null)
					{
						summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Indicator", indicator, dataRow);
						element.Item4.Add(summaryInfo);
					}

					++summaryInfo.AggregationCount;
				}

			}
			
		});//foreach

		foreach (var element in segments)
		{
			if (element.Item4.Count == 0)
			{
				dataSummaryRow = dtCSummaryLog.NewRow();

				dataSummaryRow["Data Center"] = dcName;
				dataSummaryRow["Node IPAddress"] = ipAddress;

				dataSummaryRow["Timestamp Period"] = element.Item1;
				dataSummaryRow["Aggregation Period"] = element.Item3;
				dataSummaryRow["Type"] = null;
				dataSummaryRow["Value"] = null;
				dataSummaryRow["Assocated Item"] = null;
				dataSummaryRow["Occurrences"] = 0;
				
				dtCSummaryLog.Rows.Add(dataSummaryRow);
			}
			else
			{
				foreach (var item in element.Item4)
				{
					dataSummaryRow = dtCSummaryLog.NewRow();

					dataSummaryRow["Data Center"] = item.DataCenter;
					dataSummaryRow["Node IPAddress"] = item.IPAddress;

					dataSummaryRow["Timestamp Period"] = item.Period;
					dataSummaryRow["Aggregation Period"] = item.PeriodSpan;
					dataSummaryRow["Type"] = item.ItemType;
					dataSummaryRow["Value"] = item.ItemValue;
					dataSummaryRow["Assocated Item"] = item.AssocatedItem;
					dataSummaryRow["Occurrences"] = item.AggregationCount;

					dtCSummaryLog.Rows.Add(dataSummaryRow);
				}
			}
		}
	}
}

void ParseOSMachineInfoDataTable(IDirectoryPath directoryPath,
									string[] osmachineFiles,									
									string ipAddress,
									string dcName,
									DataTable dtOSMachineInfo)
{
	if (dtOSMachineInfo.Columns.Count == 0)
	{
		dtOSMachineInfo.Columns.Add("Node IPAdress", typeof(string)).Unique = true;
		dtOSMachineInfo.PrimaryKey = new System.Data.DataColumn[] { dtOSMachineInfo.Columns["Node IPAdress"] };
		dtOSMachineInfo.Columns.Add("DataCenter", typeof(string)).AllowDBNull = true;
		
		dtOSMachineInfo.Columns.Add("CPU Architecture", typeof(string));
		dtOSMachineInfo.Columns.Add("Physical Memory (MB)", typeof(int));
		dtOSMachineInfo.Columns.Add("OS", typeof(string));
		dtOSMachineInfo.Columns.Add("OS Version", typeof(string));
		dtOSMachineInfo.Columns.Add("TimeZone", typeof(string));
		//CPU Load
		dtOSMachineInfo.Columns.Add("Average", typeof(decimal)); //h
		dtOSMachineInfo.Columns.Add("Idle", typeof(decimal));
		dtOSMachineInfo.Columns.Add("System", typeof(decimal));
		dtOSMachineInfo.Columns.Add("User", typeof(decimal)); //k
		//Memory
		dtOSMachineInfo.Columns.Add("Available", typeof(int)); //l
		dtOSMachineInfo.Columns.Add("Cache", typeof(int));
		dtOSMachineInfo.Columns.Add("Buffers", typeof(int));
		dtOSMachineInfo.Columns.Add("Shared", typeof(int));
		dtOSMachineInfo.Columns.Add("Free", typeof(int));
		dtOSMachineInfo.Columns.Add("Used", typeof(int)); //q
		//Java
		dtOSMachineInfo.Columns.Add("Vendor", typeof(string));//r
		dtOSMachineInfo.Columns.Add("Model", typeof(string));
		dtOSMachineInfo.Columns.Add("Runtime Name", typeof(string));
		dtOSMachineInfo.Columns.Add("Runtime Version", typeof(string));//u
		//Java NonHeapMemoryUsage
		dtOSMachineInfo.Columns.Add("Non-Heap Committed", typeof(decimal)); //v
		dtOSMachineInfo.Columns.Add("Non-Heap Init", typeof(decimal));
		dtOSMachineInfo.Columns.Add("Non-Heap Max", typeof(decimal));
		dtOSMachineInfo.Columns.Add("Non-Heap Used", typeof(decimal));//y
		//Javaa HeapMemoryUsage
		dtOSMachineInfo.Columns.Add("Heap Committed", typeof(decimal)); //z
		dtOSMachineInfo.Columns.Add("Heap Init", typeof(decimal)); //aa
		dtOSMachineInfo.Columns.Add("Heap Max", typeof(decimal)); //ab
		dtOSMachineInfo.Columns.Add("Heap Used", typeof(decimal)); //ac

		//NTP
		dtOSMachineInfo.Columns.Add("Correction (ms)", typeof(int)); //ad
		dtOSMachineInfo.Columns.Add("Polling (secs)", typeof(int));
		dtOSMachineInfo.Columns.Add("Maximum Error (us)", typeof(int));
		dtOSMachineInfo.Columns.Add("Estimated Error (us)", typeof(int));
		dtOSMachineInfo.Columns.Add("Time Constant", typeof(int)); //ah
		dtOSMachineInfo.Columns.Add("Precision (us)", typeof(decimal)); //ai
		dtOSMachineInfo.Columns.Add("Frequency (ppm)", typeof(decimal));
		dtOSMachineInfo.Columns.Add("Tolerance (ppm)", typeof(decimal)); //ak
	}

	var dataRow = dtOSMachineInfo.NewRow();

	dataRow["Node IPAdress"] = ipAddress;
	dataRow["DataCenter"] = dcName;
	
	foreach (var fileName in osmachineFiles)
	{
		IFilePath filePath;

		if (directoryPath.Clone().MakeFile(fileName, out filePath))
		{
			if (filePath.Exist())
			{
				if (fileName.Contains("machine-info"))
				{
					var infoObject = ParseJson(filePath.ReadAllText());
					
					dataRow["CPU Architecture"] = infoObject["arch"];
					dataRow["Physical Memory (MB)"] = infoObject["memory"];
				}
				else if (fileName.Contains("os-info"))
				{
					var infoObject = ParseJson(filePath.ReadAllText());
					
					dataRow["OS"] = infoObject["sub_os"];
					dataRow["OS Version"] = infoObject["os_version"];
				}
				else if (fileName.Contains("cpu"))
				{
					var infoObject = ParseJson(filePath.ReadAllText());
					
					dataRow["Idle"] = infoObject["%idle"];
					dataRow["System"] = infoObject["%system"];
					dataRow["User"] = infoObject["%user"];
				}
				else if (fileName.Contains("load_avg"))
				{
					dataRow["Average"] = decimal.Parse(filePath.ReadAllText());
				}
				else if (fileName.Contains("memory"))
				{
					var infoObject = ParseJson(filePath.ReadAllText());
					
					dataRow["Available"] = infoObject["available"];
					dataRow["Cache"] = infoObject["cache"];
					dataRow["Buffers"] = infoObject["buffers"];
					dataRow["Shared"] = infoObject["shared"];
					dataRow["Free"] = infoObject["free"];
					dataRow["Used"] = infoObject["used"];
				}
				else if (fileName.Contains("java_system_properties"))
				{
					var infoObject = ParseJson(filePath.ReadAllText());
					
					dataRow["Vendor"] = infoObject["java.vendor"];
					dataRow["Model"] = infoObject["sun.arch.data.model"];
					dataRow["Runtime Name"] = infoObject["java.runtime.name"];
					dataRow["Runtime Version"] = infoObject["java.runtime.version"];
					dataRow["TimeZone"] = ((string)infoObject["user.timezone"])
											.Replace((string) infoObject["file.separator"], "/");
				}
				else if (fileName.Contains("java_heap"))
				{
					var infoObject = ParseJson(filePath.ReadAllText());
					var nonHeapJson = ParseJson((string) infoObject["NonHeapMemoryUsage"]);
					var heapJson = ParseJson((string) infoObject["HeapMemoryUsage"]);
					
					//Java NonHeapMemoryUsage
					dataRow["Non-Heap Committed"] = ((dynamic) (nonHeapJson["committed"])) / BytesToMB;
					dataRow["Non-Heap Init"] = ((dynamic) (nonHeapJson["init"])) / BytesToMB;
					dataRow["Non-Heap Max"] = ((dynamic) (nonHeapJson["max"])) / BytesToMB;
					dataRow["Non-Heap Used"] = ((dynamic) (nonHeapJson["used"])) / BytesToMB;
					//Javaa HeapMemoryUsage
					dataRow["Heap Committed"] = ((dynamic) (heapJson["committed"])) / BytesToMB;
					dataRow["Heap Init"] = ((dynamic) (heapJson["init"])) / BytesToMB;
					dataRow["Heap Max"] = ((dynamic) (heapJson["max"])) / BytesToMB;
					dataRow["Heap Used"] = ((dynamic) (heapJson["used"])) / BytesToMB;
				}
				else if (fileName.Contains("ntpstat"))
				{
					var fileText = filePath.ReadAllText();
					var words = StringFunctions.Split(fileText,
														' ',
														StringFunctions.IgnoreWithinDelimiterFlag.Text,
														StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries | Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
					for(int nIndex = 0; nIndex < words.Count; ++nIndex)
					{
						var element = words[nIndex];
						
						if (element == "within")
						{
							dataRow["Correction (ms)"] = DetermineTime(words[++nIndex]);
						}
						else if (element == "every")
						{
							dataRow["Polling (secs)"] = DetermineTime(words[++nIndex]);
						}
					}
				}
				else if(fileName.Contains("ntptime"))
				{
					var fileText = filePath.ReadAllText();
					var words = StringFunctions.Split(fileText,
														' ',
														StringFunctions.IgnoreWithinDelimiterFlag.Text,
														StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries | Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
					for (int nIndex = 0; nIndex < words.Count; ++nIndex)
					{
						var element = words[nIndex];

						if (element == "maximum")
						{
							dataRow["Maximum Error (us)"] = DetermineTime(words[nIndex += 2]);
						}
						else if (element == "estimated")
						{
							dataRow["Estimated Error (us)"] = DetermineTime(words[nIndex += 2]);
						}
						else if (element == "constant")
						{
							dataRow["Time Constant"] = DetermineTime(words[++nIndex]);
						}
						else if (element == "precision")
						{
							dataRow["Precision (us)"] = DetermineTime(words[++nIndex]);
						}
						else if (element == "frequency")
						{
							dataRow["Frequency (ppm)"] = DetermineTime(words[++nIndex]);
						}
						else if (element == "tolerance")
						{
							dataRow["Tolerance (ppm)"] = DetermineTime(words[++nIndex]);
						}
					}
				}
			}
		}
	}

	lock (dtOSMachineInfo)
	{
		dtOSMachineInfo.Rows.Add(dataRow);
	}
}



#endregion

#region Helper Functions

bool LookForIPAddress(string value, string ignoreIPAddress, out string ipAddress)
{

	if (string.IsNullOrEmpty(value))
	{
		ipAddress = null;
		return false;
	}

	if (value[0] == '/')
	{
		string strIP;
		int nPortPoa = value.IndexOfAny(new char[] { ':', '\'' });

		if (nPortPoa > 7)
		{
			value = value.Substring(0,nPortPoa);
		}

		if (IPAddressStr(value.Substring(1), out strIP))
		{
			if (strIP != ignoreIPAddress)
			{
				ipAddress = strIP;
				return true;
			}
		}
	}
	else if (Char.IsDigit(value[0]))
	{
		string strIP;
		int nPortPoa = value.IndexOfAny(new char[] { ':', '\'' });

		if (nPortPoa > 6)
		{
			value = value.Substring(0, nPortPoa);
		}

		if (IPAddressStr(value, out strIP))
		{
			if (strIP != ignoreIPAddress)
			{
				ipAddress = strIP;
				return true;
			}
		}
	}
	else if (value[0] == '[')
	{
		var newValue = value.Substring(1);

		if (newValue[newValue.Length - 1] == ']')
		{
			newValue = newValue.Substring(0, newValue.Length - 1);
		}

		var items = newValue.Split(new char[] { ' ', ',', '>' });

		foreach (var element in items)
		{
			if (LookForIPAddress(element, ignoreIPAddress, out ipAddress))
			{
				return true;
			}
		}
	}
	
	ipAddress = null;
	return false;
}

bool IsIPv4(string value)
{
	var quads = value.Split('.');

	// if we do not have 4 quads, return false
	if (!(quads.Length == 4)) return false;

	// for each quad
	foreach (var quad in quads)
	{
		int q;
		// if parse fails 
		// or length of parsed int != length of quad string (i.e.; '1' vs '001')
		// or parsed int < 0
		// or parsed int > 255
		// return false
		if (!Int32.TryParse(quad, out q)
			|| !q.ToString().Length.Equals(quad.Length)
			|| q < 0
			|| q > 255)
		{ return false; }

	}

	return true;
}

bool IPAddressStr(string ipAddress, out string formattedAddress)
{
	if (IsIPv4(ipAddress))
	{
		System.Net.IPAddress objIP;

		if (System.Net.IPAddress.TryParse(ipAddress, out objIP))
		{
			formattedAddress = objIP.ToString();
			return true;
		}
	}
	
	formattedAddress = ipAddress;
	return false;
}

string RemoveQuotes(string item)
{
	if (item.Length > 2
			&& ((item[0] == '\'' && item[item.Length - 1] == '\'')
					|| (item[0] == '"' && item[item.Length - 1] == '"')))
	{
		return item.Substring(1, item.Length - 2);
	}
	
	return item;
}

Tuple<string,string> SplitTableName(string cqlTableName, string defaultKeySpaceName)
{
	var nameparts = Common.StringFunctions.Split(cqlTableName,
													'.',
													Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
													Common.StringFunctions.SplitBehaviorOptions.Default
														| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

	if (nameparts.Count == 1)
	{
		return new Tuple<string,string>(defaultKeySpaceName, RemoveQuotes(nameparts[0]));
	}
	
	return new Tuple<string,string>(RemoveQuotes(nameparts[0]), RemoveQuotes(nameparts[1]));
}

Tuple<string,string> ParseKeyValuePair(string pairKeyValue)
{
	var valueList = pairKeyValue.Split('=');

	if (valueList.Length == 1)
	{
		return new Tuple<string,string>(valueList[0].Trim(), null);
	}
	
	return new Tuple<string,string>(valueList[0].Trim(), valueList[1].Trim());
}

const decimal BytesToMB = 1048576m;

decimal ConvertInToMB(string strSize, string type)
{
	switch (type.ToLower())
	{
		case "bytes":
		case "byte":
			return decimal.Parse(strSize)/BytesToMB; 
		case "kb":
			return decimal.Parse(strSize)/1024m; 
		case "mb":
			return decimal.Parse(strSize);
		case "gb":
			return decimal.Parse(strSize) * 1024m;
		case "tb":
			return decimal.Parse(strSize) * 1048576m;
	}
	
	return -1;
}

decimal ConvertInToMB(string strSizeAndType)
{
	var spacePos = strSizeAndType.IndexOf(' ');

	if (spacePos <= 0)
	{
		return -1;
	}
	
	return ConvertInToMB(strSizeAndType.Substring(0, spacePos), strSizeAndType.Substring(spacePos + 1));
}

DateTime FromUnixTime(long unixTime)
{
	var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
	return epoch.AddMilliseconds(unixTime);
}

DateTime FromUnixTime(string unixTime)
{
	return FromUnixTime(long.Parse(unixTime));
}

string RemoveNamespace(string className)
{
	className = RemoveQuotes(className);

	if (!className.Contains('/'))
	{
		var lastPeriod = className.LastIndexOf('.');

		if (lastPeriod >= 0)
		{
			return className.Substring(lastPeriod + 1);
		}
	}
	
	return className;
}

string DetermineProperFormat(string strValue, bool ignoreBraces = false, bool removeNamespace = true)
{
	string strValueA;
	object item;

	if (string.IsNullOrEmpty(strValue))
	{
		return strValue;
	}

	strValue = strValue.Trim();

	if (strValue == string.Empty)
	{
		return strValue;
	}

	if (strValue[0] == '"')
	{
		var splitItems = strValue.Substring(1, strValue.Length - 2).Split(',');
		var fmtItems = splitItems.Select(i => DetermineProperFormat(i, true)).Sort();
		return string.Join(", ", fmtItems);
	}

	if (!ignoreBraces)
	{
		if (strValue[0] == '{')
		{
			strValue = strValue.Substring(1);
		}
		if (strValue[strValue.Length - 1] == '}')
		{
			strValue = strValue.Substring(0, strValue.Length - 1);
		}
	}

	strValue = RemoveQuotes(strValue);

	if (IPAddressStr(strValue, out strValueA))
	{
		return strValueA;
	}

	if (StringFunctions.ParseIntoNumeric(strValue, out item))
	{
		return item.ToString();
	}

	return removeNamespace ? RemoveNamespace(strValue) : strValue;
}

object DetermineProperObjectFormat(string strValue, bool ignoreBraces = false, bool removeNamespace = true)
{
	string strValueA;
	object item;

	if (string.IsNullOrEmpty(strValue))
	{
		return strValue;
	}

	strValue = strValue.Trim();

	if (strValue == string.Empty)
	{
		return strValue;
	}

	if (strValue[0] == '"')
	{
		var splitItems = strValue.Substring(1, strValue.Length - 2).Split(',');
		var fmtItems = splitItems.Select(i => DetermineProperFormat(i, true)).Sort();
		return string.Join(", ", fmtItems);
	}

	if (!ignoreBraces)
	{
		if (strValue[0] == '{')
		{
			strValue = strValue.Substring(1);
		}
		if (strValue[strValue.Length - 1] == '}')
		{
			strValue = strValue.Substring(0, strValue.Length - 1);
		}
	}

	strValue = RemoveQuotes(strValue);

	if (IPAddressStr(strValue, out strValueA))
	{
		return strValueA;
	}

	if (StringFunctions.ParseIntoNumeric(strValue, out item))
	{
		return item;
	}

	return removeNamespace ? RemoveNamespace(strValue) : strValue;
}

string RemoveCommentInLine(string line, char commentChar = '#')
{
	var commentPos = line.IndexOf(commentChar);

	if (commentPos >= 0)
	{
		return line.Substring(0, commentPos).TrimEnd();
	}
	
	return line;
}

object DetermineTime(string strTime)
{
	var timeAbbrPos = strTime.LastIndexOfAny(new char[] { 'm', 's', 'h' });
	object numTime;
	
	if (timeAbbrPos > 0)
	{
		strTime = strTime.Substring(0, timeAbbrPos);
	}

	if (StringFunctions.ParseIntoNumeric(strTime, out numTime))
	{
		return numTime;
	}
	
	return strTime;
}

Dictionary<string, object> ParseJson(string strJson)
{
	strJson = strJson.Trim();

	if (strJson[0] == '{')
	{
		strJson = strJson.Substring(1, strJson.Length - 2);
	}

	var keyValuePair = StringFunctions.Split(strJson,
												new char[] { ':', ','},
												StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace,
												StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries | Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
												
	var jsonDict = new Dictionary<string, object>();

	for (int nIndex = 0; nIndex < keyValuePair.Count; ++nIndex)
	{
		jsonDict.Add(RemoveQuotes(keyValuePair[nIndex].Trim()).Trim(),
						DetermineProperObjectFormat(RemoveQuotes(keyValuePair[++nIndex]), true, false));
	}
	
	return jsonDict;
}

#endregion