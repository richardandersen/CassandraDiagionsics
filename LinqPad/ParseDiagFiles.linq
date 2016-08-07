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

void Main()
{
	var excelFilePath = @"C:\Users\richa\Desktop\Cengage_CXP_PROD-diagnostic.xlsx";
	var diagnosticPath = @"C:\Users\richa\Desktop\Cengage_CXP_PROD-diagnostics-2016_07_25_16_52_53_UTC";
	
	var excelWorkSheetRingInfo = "RingInfo";
	var excelWorkSheetRingTokenRanges = "RingTokenRange";
	var excelWorkSheetRingCFStats = "CFStats";
	var excelWorkSheetLogCassandra = "Cassandra Log";

	List<string> ignoreKeySpaces = new List<string>() { "dse_system", "system_auth", "system_traces", "system", "dse_perf"  }; //MUST BE IN LOWER CASe
	List<string> mbColumns = new List<string>() { "memory used", "bytes", "space used", "data size"}; //MUST BE IN LOWER CASE
	
	
	bool opsCtrDiag = false;
	var diagNodeDir = "nodes";
	var nodetoolDir = "nodetool";
	var logsDir = "logs";
	var nodetoolRingFile = "ring";
	var nodetoolCFStatsFile = "cfstats";
	var logCassandraSystemLog = @".\cassandra\system.log";
	
	var dtRingInfo = new System.Data.DataTable(excelWorkSheetRingInfo);
	var dtTokenRange = new System.Data.DataTable(excelWorkSheetRingTokenRanges);
	var dtCFStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	var dtLogsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
	
	var diagPath = Common.Path.PathUtils.BuildDirectoryPath(diagnosticPath);
	var diagNodePath = diagPath.Clone().AddChild(diagNodeDir) as Common.IDirectoryPath;
	List<Common.IDirectoryPath> nodeDirs = null;
	
	if (diagNodePath != null && (opsCtrDiag = diagNodePath.Exist()))
	{
		nodeDirs = diagNodePath.Children().Cast<Common.IDirectoryPath>().ToList();
	}
	else
	{
		nodeDirs= diagPath.Children().Cast<Common.IDirectoryPath>().ToList();
	}

	IFilePath ringFilePath = null;

	if (((Common.IDirectoryPath)nodeDirs.First().Clone().AddChild(nodetoolDir)).MakeFile(nodetoolRingFile, out ringFilePath))
	{
		if (ringFilePath.Exist())
		{
			ReadRingFileParseIntoDataTables(ringFilePath, dtRingInfo, dtTokenRange);
		}
	}
	
	Parallel.ForEach(nodeDirs, (element) =>
	//foreach (var element in nodeDirs)
	{
		string ipAddress = null;
		string dcName = null;
		IFilePath cfstatsFilePath = null;
		IFilePath cassandraLogFilePath = null;
		
		Console.WriteLine("Processing File \"{0}\"", element.Path);
		
		var possibleAddress = Common.StringFunctions.Split(element.Name,
															new char[] { ' ', '-', '_'},
															Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
															Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
														
		if (possibleAddress.Count() == 1)
		{
			IPAddressStr(possibleAddress[0], out ipAddress);
		}
		else
		{				
			//Ip Address is either the first part of the name or the last
			if (!IPAddressStr(possibleAddress[0], out ipAddress))
			{
				IPAddressStr(possibleAddress.Last(), out ipAddress);
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

		if (((Common.IDirectoryPath)element.Clone().AddChild(nodetoolDir)).MakeFile(nodetoolCFStatsFile, out cfstatsFilePath))
		{
			if (cfstatsFilePath.Exist())
			{
				Console.WriteLine("Processing File \"{0}\"", cfstatsFilePath.Path);
				var dtCFStats = new System.Data.DataTable(excelWorkSheetRingCFStats + "-" + ipAddress);
				dtCFStatsStack.Push(dtCFStats);
				ReadCFStatsFileParseIntoDataTable(cfstatsFilePath, ipAddress, dcName, dtCFStats, ignoreKeySpaces, mbColumns);
			}
		}

		if (((Common.IDirectoryPath)element.Clone().AddChild(logsDir)).MakeFile(logCassandraSystemLog, out cassandraLogFilePath))
		{			
			if (cassandraLogFilePath.Exist())
			{
				Console.WriteLine("Processing File \"{0}\"", cassandraLogFilePath.Path);
				var dtLog = new System.Data.DataTable(excelWorkSheetLogCassandra + "-" + ipAddress);
				dtLogsStack.Push(dtLog);
				ReadCassandraLogParseIntoDataTable(cassandraLogFilePath, ipAddress, dcName, dtLog);
			}
		}
		
		
	});
	
		
	var excelFile = new FileInfo(excelFilePath);
	using (var excelPkg = new ExcelPackage(excelFile))
	{

		DTLoadIntoExcelWorkStation(excelPkg, excelWorkSheetRingInfo, dtRingInfo);
		
		var workBook = excelPkg.Workbook.Worksheets[excelWorkSheetRingInfo];
		if (workBook != null)
		{
			workBook.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
			workBook.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
			//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
			workBook.View.FreezePanes(2, 1);
			workBook.Cells["A1:D1"].AutoFilter = true;
			workBook.Cells.AutoFitColumns();
		}
		
		DTLoadIntoExcelWorkStation(excelPkg, excelWorkSheetRingTokenRanges, dtTokenRange);

		workBook = excelPkg.Workbook.Worksheets[excelWorkSheetRingTokenRanges];
		if (workBook != null)
		{
			workBook.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
			workBook.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
			//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
			workBook.View.FreezePanes(2, 1);
			workBook.Cells["A1:C1"].AutoFilter = true;
			workBook.Cells["A:B"].Style.Numberformat.Format = "0";
			workBook.Cells.AutoFitColumns();
		}

		DTLoadIntoExcelWorkStation(excelPkg, excelWorkSheetRingCFStats, dtCFStatsStack);

		workBook = excelPkg.Workbook.Worksheets[excelWorkSheetRingCFStats];
		if (workBook != null)
		{
			workBook.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
			workBook.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
			//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
			workBook.View.FreezePanes(2, 1);
			workBook.Cells["A1:H1"].AutoFilter = true;
			workBook.Cells.AutoFitColumns();
		}

		DTLoadIntoExcelWorkStation(excelPkg, excelWorkSheetLogCassandra, dtLogsStack);

		workBook = excelPkg.Workbook.Worksheets[excelWorkSheetLogCassandra];
		if (workBook != null)
		{
			workBook.Cells["C:C"].Style.Numberformat.Format = "m/d/yy h:mm:ss;@";
			workBook.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
			workBook.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
			//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
			workBook.View.FreezePanes(2, 1);
			workBook.Cells["A1:J1"].AutoFilter = true;			
			workBook.Cells.AutoFitColumns();
		}
		
		excelPkg.Save();
	}
}

ExcelRangeBase DTLoadIntoExcelWorkStation(ExcelPackage excelPkg,
											string workSheetName,
											System.Data.DataTable dtExcel)
{
	dtExcel.AcceptChanges();
	
	var dtErrors = dtExcel.GetErrors();
	if (dtErrors.Length > 0)
	{
		dtErrors.Dump(string.Format("Table \"{0}\" Has Error", dtExcel.TableName));
	}

	var workBook = excelPkg.Workbook.Worksheets[workSheetName];
	if (workBook == null)
	{
		workBook = excelPkg.Workbook.Worksheets.Add(workSheetName);
	}
	else
	{
		workBook.Cells.Clear();
	}
	
	Console.WriteLine("Loading DataTable \"{0}\" into Excel WorkBook \"{1}\". Rows: {2:###,###,##0}", dtExcel.TableName, workBook.Name, dtExcel.Rows.Count);
	
	return workBook.Cells["A1"].LoadFromDataTable(dtExcel, true);
}

ExcelRangeBase DTLoadIntoExcelWorkStation(ExcelPackage excelPkg,
											string workSheetName,
											Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable> dtExcels)
{
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
	
	while (dtExcels.Pop(out dtExcel))
	{
		dtExcel.AcceptChanges();
		dtErrors = dtExcel.GetErrors();
		if (dtErrors.Length > 0)
		{
			dtErrors.Dump(string.Format("Table \"{0}\" Has Error", dtExcel.TableName));
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
		}
	}

	return loadRange;
}

void ReadRingFileParseIntoDataTables(IFilePath ringFilePath,
										System.Data.DataTable dtRingInfo,
										System.Data.DataTable dtTokenRange)
{
	dtRingInfo.Columns.Add("Node IPAdress", typeof(string));
	dtRingInfo.Columns[0].Unique = true;
	dtRingInfo.PrimaryKey = new System.Data.DataColumn[] { dtRingInfo.Columns[0] };
	dtRingInfo.Columns.Add("DataCenter", typeof(string));
	dtRingInfo.Columns.Add("Rack", typeof(string));
	dtRingInfo.Columns.Add("Status", typeof(string));


	dtTokenRange.Columns.Add("Start Token (exclusive)", typeof(long));
	dtTokenRange.Columns.Add("End Token (inclusive)", typeof(long));
	dtTokenRange.Columns.Add("Node IPAdress", typeof(string));
	
	var fileLines = ringFilePath.ReadAllLines();
	
	string currentDC = null;
	long? currentStartToken = null;
	string line = null;
	string ipAddress;
	DataRow dataRow;
	List<string> parsedLine;
	bool newDC = true;
	
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
				if (line[0] != '=' && !line.StartsWith("Address"))
				{
					newDC = false;
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

						dataRow[0] = ipAddress;
						dataRow[1] = currentDC;
						dataRow[2] = parsedLine[1];
						dataRow[3] = parsedLine[2];

						dtRingInfo.Rows.Add(dataRow);
					}

					dataRow = dtTokenRange.NewRow();

					dataRow[2] = ipAddress;
					dataRow[0] = currentStartToken;
					currentStartToken = long.Parse(parsedLine[7]);
					dataRow[1] = currentStartToken;

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
								dataRow[7] = decNbr / 1000000m;
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

void ReadCassandraLogParseIntoDataTable(IFilePath clogFilePath,
										string ipAddress,
										string dcName,
										System.Data.DataTable dtCLog)
{
	if (dtCLog.Columns.Count == 0)
	{
		dtCLog.Columns.Add("Data Center", typeof(string));
		dtCLog.Columns[0].AllowDBNull = true;
		dtCLog.Columns.Add("Node IPAdress", typeof(string));


		dtCLog.Columns.Add("Time", typeof(DateTime));
		dtCLog.Columns.Add("Indicator", typeof(string));
		dtCLog.Columns.Add("Task", typeof(string));
		dtCLog.Columns.Add("Item", typeof(string));
		dtCLog.Columns.Add("Exception", typeof(string));
		dtCLog.Columns["Exception"].AllowDBNull = true;
		dtCLog.Columns.Add("Exception Description", typeof(string));
		dtCLog.Columns["Exception Description"].AllowDBNull = true;
		dtCLog.Columns.Add("Assocated IP", typeof(string));
		dtCLog.Columns["Assocated IP"].AllowDBNull = true;
		dtCLog.Columns.Add("Description", typeof(string));
	}


	var fileLines = clogFilePath.ReadAllLines();
	string line;
	List<string> parsedValues;
	string logDesc;
	DataRow dataRow;
	DateTime lineDateTime;
	string lineIPAddress;
	
	for(int nLine = 0; nLine < fileLines.Length; ++nLine)
	{
		line = fileLines[nLine].Trim();

		if (line.Substring(0, 3).ToLower() == "at ")
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
    
		dataRow = dtCLog.NewRow();
		
		dataRow[0] = dcName;
		dataRow[1] = ipAddress;

		if (DateTime.TryParse(parsedValues[2] + ' ' + parsedValues[3].Replace(',', '.'), out lineDateTime))
		{
			dataRow["Time"] = lineDateTime;
		}
		else
		{
			line.Dump("Invalid Date/Time");
		}
		
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

		if (parsedValues[5][0] == '-')
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
		
		dtCLog.Rows.Add(dataRow);
	}
}

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


// Define other methods and classes here