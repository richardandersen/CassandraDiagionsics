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


const decimal BytesToMB = 1048576m;
const int MaxRowInExcelWorkSheet = 50000; //-1 disabled
const int MaxRowInExcelWorkBook = 500000; //-1 disabled
static TimeSpan LogTimeSpanRange = new TimeSpan(6, 0, 0, 0); //Only import log entries for the past timespan (e.g., the last 5 days) based on LogCurrentDate.
static DateTime LogCurrentDate = new DateTime(2016, 07, 05); //DateTime.MinValue; //DateTime.Now.Date; //If DateTime.MinValue all log entries are parsed
static int LogMaxRowsPerNode = 2000; //-1 disabled

void Main()
{
	#region Configuration
	//Location where this application will write or update the Excel file.
	var excelFilePath = @"[DeskTop]\TestDiag.xlsx"; //<==== Should be updated
	
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
	//All diagnostic files are located directly under diagnosticPath folder. Each file should have the IP Adress either in the beginning or end of the file name.
	//	e.g., cfstats_10.192.40.7, system-10.192.40.7.log, 10.192.40.7_system.log, etc.
	var diagnosticPath = @"[MyDocuments]\LINQPad Queries\DataStax\TestData\production_group_v_1-diagnostics-2016_07_04_15_43_48_UTC"; //@"C:\Users\richard\Desktop\datastax"; //<==== Should be Updated 
	var diagnosticNoSubFolders = false; //<==== Should be Updated 
	var parseLogs = true;
	var parseNonLogs = true;
	
	//Excel Workbook names
	var excelWorkBookRingInfo = "Node Information";
	var excelWorkBookRingTokenRanges = "Ring Token Ranges";
	var excelWorkBookCFStats = "CFStats";
	var excelWorkBookTPStats = "TPStats";
	var excelWorkBookLogCassandra = "Cassandra Log";
	var excelWorkBookDDLKeyspaces = "DDL Keyspaces";
	var excelWorkBookDDLTables = "DDL Tables";
	var excelWorkBookCompactionHist = "Compaction History";
	var excelWorkBookYaml = "Settings-Yamls";

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
	
	#endregion
	
	#region Local Variables
	
	//Local Variables used for processing
	bool opsCtrDiag = false;	
	var dtRingInfo = new System.Data.DataTable(excelWorkBookRingInfo);
	var dtTokenRange = new System.Data.DataTable(excelWorkBookRingTokenRanges);
	var dtKeySpace = new System.Data.DataTable(excelWorkBookDDLKeyspaces);
	var dtTable = new System.Data.DataTable(excelWorkBookDDLTables);
	var cqlHashCheck = new Dictionary < string, int >();
	var dtCFStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var dtTPStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var dtLogsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var dtCompHistStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var listCYamlStack = new Common.Patterns.Collections.LockFree.Stack<List<YamlInfo>>();
	var dtYaml = new System.Data.DataTable(excelWorkBookYaml);

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

	if (diagnosticNoSubFolders)
	{
		#region Parse -- All Files in one Folder
		
		var diagChildren =  diagPath.Children();
		
		//Need to process nodetool ring files first
		var nodetoolRingChildFile = diagChildren.Where(c => c is IFilePath && c.Name.Contains(nodetoolRingFile)).FirstOrDefault();

		if (parseNonLogs && nodetoolRingChildFile != null)
		{
			Console.WriteLine("Processing File \"{0}\"", nodetoolRingChildFile.Path);
			ReadRingFileParseIntoDataTables((IFilePath)nodetoolRingChildFile, dtRingInfo, dtTokenRange);
		}

		nodetoolRingChildFile = diagChildren.Where(c => c is IFilePath && c.Name.Contains(dseToolDir + "_" + dsetoolRingFile)).FirstOrDefault();

		if (parseNonLogs && nodetoolRingChildFile != null)
		{
			Console.WriteLine("Processing File \"{0}\"", nodetoolRingChildFile.Path);
			ReadDSEToolRingFileParseIntoDataTable((IFilePath)nodetoolRingChildFile, dtRingInfo);
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

			}
		}


		Parallel.ForEach(diagChildren, (diagFile) =>
		//foreach (var diagFile in diagChildren)
		{
			if (diagFile is IFilePath)
			{
				string ipAddress;
				string dcName;
				
				if (DetermineIPDCFromFileName(((IFilePath)diagFile).FileName, dtRingInfo, out ipAddress, out dcName))
				{
					if (parseNonLogs && diagFile.Name.Contains(nodetoolCFStatsFile))
					{
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var dtCFStats = new System.Data.DataTable(excelWorkBookCFStats + "-" + ipAddress);
						dtCFStatsStack.Push(dtCFStats);
						ReadCFStatsFileParseIntoDataTable((IFilePath) diagFile, ipAddress, dcName, dtCFStats, ignoreKeySpaces, cfstatsCreateMBColumns);
					}
					else if (parseNonLogs && diagFile.Name.Contains(nodetoolTPStatsFile))
					{
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var dtTPStats = new System.Data.DataTable(excelWorkBookTPStats + "-" + ipAddress);
						dtTPStatsStack.Push(dtTPStats);
						ReadTPStatsFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtTPStats);
					}
					else if (parseNonLogs && diagFile.Name.Contains(nodetoolInfoFile))
					{
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						ReadInfoFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtRingInfo);
					}
					else if (parseNonLogs && diagFile.Name.Contains(nodetoolCompactionHistFile))
					{
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var dtCompHist = new System.Data.DataTable(excelWorkBookCompactionHist + "-" + ipAddress);
						dtCompHistStack.Push(dtCompHist);
						ReadCompactionHistFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtCompHist, dtTable, ignoreKeySpaces);
					}
					else if (parseLogs && diagFile.Name.Contains(logCassandraSystemLogFile))
					{
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var dtLog = new System.Data.DataTable(excelWorkBookLogCassandra + "-" + ipAddress);
						dtLogsStack.Push(dtLog);
						ReadCassandraLogParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, includeLogEntriesAfterThisTimeFrame, LogMaxRowsPerNode, dtLog);
					}
					else if (parseNonLogs && diagFile.Name.Contains(confCassandraYamlFileName))
					{
						Console.WriteLine("Processing File \"{0}\"", diagFile.Path);
						var yamlList = new List<YamlInfo>();
						listCYamlStack.Push(yamlList);
						ReadYamlFileParseIntoList((IFilePath)diagFile, ipAddress, dcName, confCassandraType, yamlList);
					}
					else if (parseNonLogs && diagFile.Name.Contains(confDSEFileName))
					{
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

		//Parallel.ForEach(nodeDirs, (element) =>
		foreach (var element in nodeDirs)
		{
			string ipAddress = null;
			string dcName = null;
			IFilePath diagFilePath = null;
			
			DetermineIPDCFromFileName(element.Name, dtRingInfo, out ipAddress, out dcName);

			if (parseNonLogs && element.Clone().AddChild(nodetoolDir).MakeFile(nodetoolCFStatsFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var dtCFStats = new System.Data.DataTable(excelWorkBookCFStats + "-" + ipAddress);
					dtCFStatsStack.Push(dtCFStats);
					ReadCFStatsFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtCFStats, ignoreKeySpaces, cfstatsCreateMBColumns);
				}
			}

			if (parseNonLogs && element.Clone().AddChild(nodetoolDir).MakeFile(nodetoolTPStatsFile, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var dtTPStats = new System.Data.DataTable(excelWorkBookTPStats + "-" + ipAddress);
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
					var dtHistComp = new System.Data.DataTable(excelWorkBookCompactionHist + "-" + ipAddress);
					dtCompHistStack.Push(dtHistComp);
					ReadCompactionHistFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtHistComp, dtTable, ignoreKeySpaces);
				}
			}

			if (parseLogs && element.Clone().AddChild(logsDir).MakeFile(logCassandraDirSystemLog, out diagFilePath))
			{
				if (diagFilePath.Exist())
				{
					Console.WriteLine("Processing File \"{0}\"", diagFilePath.Path);
					var dtLog = new System.Data.DataTable(excelWorkBookLogCassandra + "-" + ipAddress);
					dtLogsStack.Push(dtLog);
					ReadCassandraLogParseIntoDataTable(diagFilePath, ipAddress, dcName, includeLogEntriesAfterThisTimeFrame, LogMaxRowsPerNode, dtLog);
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

		}//);

		#endregion
	}

	var runYamlListIntoDT = Task.Run(() => ParseYamlListIntoDataTable(listCYamlStack, dtYaml));

	#endregion
	
	#region Excel Creation/Formatting
	
	//Cassandra Log (usually runs longer)
	var runLogParsing = Task.Run(() => DTLoadIntoDifferentExcelWorkBook(excelFilePath,
																		   excelWorkBookLogCassandra,
																		   dtLogsStack,
																		   workSheet =>
																			   {
																				   workSheet.Cells["C:C"].Style.Numberformat.Format = "m/d/yy h:mm:ss;@";
																				   workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
																				   workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
																					//workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
																				   workSheet.View.FreezePanes(2, 1);
																				   workSheet.Cells["A1:J1"].AutoFilter = true;
																				   workSheet.Cells.AutoFitColumns();
																			   }));

	//Non-Logs
	var excelFile = Common.Path.PathUtils.BuildFilePath(excelFilePath).FileInfo();
	using (var excelPkg = new ExcelPackage(excelFile))
	{
		//Ring
		if (dtRingInfo.Rows.Count > 0)
		{
			DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkBookRingInfo,
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
											workSheet.Cells["H:H"].Style.Numberformat.Format = "h:mm:ss;@";;
											
											workSheet.Cells.AutoFitColumns();
										});
		}

		//TokenRing
		if (dtTokenRange.Rows.Count > 0)
		{
			DTLoadIntoExcelWorkBook(excelPkg,
									excelWorkBookRingTokenRanges,
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

		//CFStats
		DTLoadIntoExcelWorkBook(excelPkg,
									excelWorkBookCFStats,
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
									});

		//TPStats
		DTLoadIntoExcelWorkBook(excelPkg, 
									excelWorkBookTPStats,
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
									});

		//Compacation History
		DTLoadIntoExcelWorkBook(excelPkg,
									excelWorkBookCompactionHist,
									dtCompHistStack,
									workSheet =>
									{
										workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
										workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
										//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
										workSheet.Cells["E:E"].Style.Numberformat.Format = "m/d/yy h:mm:ss;@";
										workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0";
										workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###,###,##0.00";
										workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###,###,##0";
										workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###,###,##0.00";
										workSheet.Cells["J1"].AddComment("The notation means {tables:rows}. For example {1:3, 3:1} means 3 rows were taken from one sstable (1:3) and 1 row taken from 3 (3:1) sstables, all to make the one sstable in that compaction operation.", "Rich Andersen");
										
										workSheet.View.FreezePanes(2, 1);
										workSheet.Cells["A1:J1"].AutoFilter = true;
										workSheet.Cells.AutoFitColumns();
									});

		//DDL Keyspace
		if (dtKeySpace.Rows.Count > 0)
		{
			DTLoadIntoExcelWorkBook(excelPkg,
										excelWorkBookDDLKeyspaces,
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
									excelWorkBookDDLTables,
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
									excelWorkBookYaml,
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

		excelPkg.Save();
	} //Save non-log data
	
	runLogParsing.Wait();
	
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
	
	var dcRow = dtRingInfo.Rows.Find(ipAddress);

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
											Action<ExcelWorksheet> worksheetAction = null)
{
	dtExcel.AcceptChanges();
	
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
	
	Console.WriteLine("Loading DataTable \"{0}\" into Excel WorkBook \"{1}\". Rows: {2:###,###,##0}", dtExcel.TableName, workSheet.Name, dtExcel.Rows.Count);
	
	var loadRange = workSheet.Cells["A1"].LoadFromDataTable(dtExcel, true);

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
											int maxRowInExcelWorkSheet = MaxRowInExcelWorkSheet)
{
	var orginalWSName = workSheetName;
	var workBook = excelPkg.Workbook.Worksheets[workSheetName];
	if (workBook == null)
	{
		workBook = excelPkg.Workbook.Worksheets.Add(workSheetName);
	}
	else
	{
		workBook.Cells.Clear();
	}

	System.Data.DataTable dtExcel;
	ExcelRangeBase rangeLoadAt = workBook.Cells["A1"];
	ExcelRangeBase loadRange = null;
	bool printHdrs = true;
	DataRow[] dtErrors;
	int wsCount = 0;
	int totalRows = 0;
	
	while (dtExcels.Pop(out dtExcel))
	{
		dtExcel.AcceptChanges();
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
				worksheetAction(workBook);
			}

			workSheetName = string.Format("{0}-{1:000}", orginalWSName, wsCount);
			workBook = excelPkg.Workbook.Worksheets[workSheetName];
			if (workBook == null)
			{
				workBook = excelPkg.Workbook.Worksheets.Add(workSheetName);
			}
			else
			{
				workBook.Cells.Clear();
			}

			printHdrs = true;
			rangeLoadAt = workBook.Cells["A1"];
			totalRows = 0;
		}

		loadRange = rangeLoadAt.LoadFromDataTable(dtExcel, printHdrs);
		Console.WriteLine("Loaded DataTable \"{0}\" into Excel WorkBook \"{1}\" in Range {2}. Rows: {3:###,###,##0}",
							dtExcel.TableName,
							workBook.Name,							
							loadRange == null ? "<Empty>" : loadRange.Address,
							dtExcel.Rows.Count);
							
		if (loadRange != null)
		{
			printHdrs = false;
			rangeLoadAt = workBook.Cells["A" + (loadRange.End.Row + 1)];
			totalRows += dtExcel.Rows.Count;
		}
	}

	if (worksheetAction != null && totalRows > 0)
	{
		worksheetAction(workBook);
	}

	return loadRange;
}

int DTLoadIntoDifferentExcelWorkBook(string excelFilePath, 
											string workSheetName,
											Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable> dtExcels,
											Action<ExcelWorksheet> worksheetAction = null,
											int maxRowInExcelWorkBook = MaxRowInExcelWorkBook,
											int maxRowInExcelWorkSheet = MaxRowInExcelWorkSheet)
{
	DataTable dtExcel;
	ExcelWorksheet workBook;
	int wsCount = 1;
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
		var excelFile = excelTargetFile.ApplyFileNameFormat(new object[] { workSheetName, wsCount}).FileInfo();
		
		using (var excelPkg = new ExcelPackage(excelFile))
		{
			DTLoadIntoExcelWorkBook(excelPkg, 
										workSheetName,
										stack,
										worksheetAction,
										maxRowInExcelWorkSheet > 0,
										maxRowInExcelWorkSheet);

			workBook = excelPkg.Workbook.Worksheets[workSheetName];
			
			excelPkg.Save();
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
		
		dtTPStats.Columns.Add("Active", typeof(int));
		dtTPStats.Columns["Active"].AllowDBNull = true;
		dtTPStats.Columns.Add("Pending", typeof(int));
		dtTPStats.Columns["Pending"].AllowDBNull = true;
		dtTPStats.Columns.Add("Completed", typeof(int));
		dtTPStats.Columns["Completed"].AllowDBNull = true;
		dtTPStats.Columns.Add("Blocked", typeof(int));
		dtTPStats.Columns["Blocked"].AllowDBNull = true;
		dtTPStats.Columns.Add("All time blocked", typeof(int));
		dtTPStats.Columns["All time blocked"].AllowDBNull = true;
		dtTPStats.Columns.Add("Dropped", typeof(int));
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
			dataRow["Active"] = int.Parse(parsedValue[1]);
			dataRow["Pending"] = int.Parse(parsedValue[2]);
			dataRow["Completed"] = int.Parse(parsedValue[3]);
			dataRow["Blocked"] = int.Parse(parsedValue[4]);
			dataRow["All time blocked"] = int.Parse(parsedValue[5]);
		}
		else if (parsingSection == 1)
		{
			//Message type           Dropped
			dataRow["Dropped"] = int.Parse(parsedValue[1]);
		}

		dtTPStats.Rows.Add(dataRow);
	}
}

void ReadCassandraLogParseIntoDataTable(IFilePath clogFilePath,
										string ipAddress,
										string dcName,
										DateTime onlyEntriesAfterThisTimeFrame,
										int maxRowWrite,
										System.Data.DataTable dtCLog)
{
	if (dtCLog.Columns.Count == 0)
	{
		dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
		dtCLog.Columns.Add("Node IPAdress", typeof(string));


		dtCLog.Columns.Add("Time", typeof(DateTime));
		dtCLog.Columns.Add("Indicator", typeof(string));
		dtCLog.Columns.Add("Task", typeof(string));
		dtCLog.Columns.Add("Item", typeof(string));
		dtCLog.Columns.Add("Exception", typeof(string)).AllowDBNull = true;
		dtCLog.Columns.Add("Exception Description", typeof(string)).AllowDBNull = true;
		dtCLog.Columns.Add("Assocated IP", typeof(string)).AllowDBNull = true;
		dtCLog.Columns.Add("Description", typeof(string));
	}

	var fileLines = clogFilePath.ReadAllLines();
	string line;
	List<string> parsedValues;
	string logDesc;
	DataRow dataRow;
	DataRow lastRow = null;
	DateTime lineDateTime;
	string lineIPAddress;
	int rowsAdded = 0;

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

		if (parsedValues[0].ToLower().Contains("exception"))
		{
			if (lastRow != null)
			{
				lastRow.BeginEdit();
				
				lastRow["Exception"] = parsedValues[0][parsedValues[0].Length - 1] == ':'
										? parsedValues[0].Substring(0, parsedValues[0].Length - 1)
										: parsedValues[0];
				lastRow["Exception Description"] = line;

				if (lastRow["Assocated IP"] == DBNull.Value)
				{
					foreach (var element in parsedValues)
					{
						if (element[0] == '(')
                        {
							if (LookForIPAddress(element.Substring(1,element.Length - 2).Trim(), ipAddress, out lineIPAddress))
							{
								lastRow["Assocated IP"] = lineIPAddress;
								break;
							}
						}
						else if (element[0] == '/')
                        {
							if (LookForIPAddress(element, ipAddress, out lineIPAddress))
							{
								lastRow["Assocated IP"] = lineIPAddress;
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

				if (lastRow["Assocated IP"] == DBNull.Value)
				{
					foreach (var element in parsedValues)
					{
						if (element[0] == '(')
                        {
							if (LookForIPAddress(element.Substring(1, element.Length - 2).Trim(), ipAddress, out lineIPAddress))
							{
								lastRow["Assocated IP"] = lineIPAddress;
								break;
							}
						}
						else if (element[0] == '/')
                        {
							if (LookForIPAddress(element, ipAddress, out lineIPAddress))
							{
								lastRow["Assocated IP"] = lineIPAddress;
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

		if (parsedValues.Count < 6)
		{
			if (lastRow != null)
			{
				line.Dump(string.Format("Warning: Invalid Log Line File: {0}", clogFilePath.PathResolved));
			}
			continue;
		}

		if (DateTime.TryParse(parsedValues[2] + ' ' + parsedValues[3].Replace(',', '.'), out lineDateTime))
		{
			if (lineDateTime < onlyEntriesAfterThisTimeFrame)
			{
				continue;
			}
		}
		else
		{
			line.Dump(string.Format("Warning: Invalid Log Date/Time File: {0}", clogFilePath.PathResolved));
			continue;
		}

		dataRow = dtCLog.NewRow();
		
		dataRow[0] = dcName;
		dataRow[1] = ipAddress;
		dataRow["Time"] = lineDateTime;
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
		
		dataRow["Item"] = parsedValues[4];
		
		logDesc = string.Empty;

		if (parsedValues[5][0] == '-') //check ERROR 
		{
			for (int nCell = 6; nCell < parsedValues.Count; ++nCell)
			{
				if (LookForIPAddress(parsedValues[nCell], ipAddress, out lineIPAddress))
				{
					dataRow["Assocated IP"] = lineIPAddress;
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
				
				logDesc += ' ' + parsedValues[nCell];
			}
			
			dataRow["Description"] = logDesc;
		}
		else
		{
			var startRange = parsedValues[5][0] == '(' ? 5 : 6;
			dataRow["Description"] = string.Join(" ", parsedValues.GetRange(startRange, parsedValues.Count - startRange));
		}

		dtCLog.Rows.Add(dataRow);
	
		if (rowsAdded++ > maxRowWrite)
		{
			break;
		}
		
		lastRow = dataRow;
		
	}
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
							
							var classSplit = ParseKeyValuePair(optKeyword).Item2.Split(':');
							var strategy = classSplit.Last().Trim();
							dataRow["Compaction Strategy"] = RemoveNamespace(strategy.Substring(0, strategy.Length - 1).TrimEnd());
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
			currentKeySpace = RemoveQuotes(parsedLine[1].Substring(0, 19));
			currentTable = RemoveQuotes(parsedLine[1].Substring(19));
			offSet = 1;
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
														new char[] { ',', ' ', ':', '=' },
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
			
			for (int nIndex = 0; nIndex < separateParams.Count; ++nIndex)
			{
				if (separateParams[nIndex] != "parameters")
				{
					if (separateParams[nIndex + 1].Length > 0 && separateParams[nIndex + 1][0] == '{')
                    {
						subCmd = separateParams[nIndex] + '.';
						++nIndex;
						separateParams[nIndex] = separateParams[nIndex].Substring(1);
					}
					
					keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex]), DetermineProperFormat(separateParams[++nIndex])));
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

decimal ConvertInToMB(string strSize, string type)
{
	switch (type.ToLower())
	{
		case "kb":
			return decimal.Parse(strSize)/1024m; 
		case "mb":
			return decimal.Parse(strSize);
		case "gb":
			return decimal.Parse(strSize) * 1024m;
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

string DetermineProperFormat(string strValue, bool ignoreBraces = false)
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
		var splitItems = strValue.Substring(1,strValue.Length - 2).Split(',');
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
	
	return RemoveNamespace(strValue);
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

#endregion