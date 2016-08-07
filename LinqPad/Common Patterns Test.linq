<Query Kind="Program">
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Cecil.FlowAnalysis.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\CommandLine.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Attributes.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.ClockTimer.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.ConfigurationSection.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Database.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Exceptions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Flags.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Path.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.AsyncDataTableOperation.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Collections.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.DataBinding.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.EventPublisher.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.EventPublisher.WCF.IService.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.EventPublisher.WCF.Server.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.FileWatcher.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.KeyHolder.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Lock.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.ObjectPool.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.QueueProcessor.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Shared.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Singleton.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.StatusPanel.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Threading.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.TimeZoneInfo.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.WCF.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.RationalNumbers.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.SequentialGuid.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Testing.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.TimeZones.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.TypedDataTable.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Db4objects.Db4o.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Db4objects.Db4o.Instrumentation.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Db4objects.Db4o.Linq.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Db4objects.Db4o.NativeQueries.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Microsoft.ExceptionMessageBox.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Mono.Cecil.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Mono.Reflection.dll</Reference>
  <Namespace>Common</Namespace>
  <Namespace>Common.Patterns</Namespace>
</Query>


void Main()
{
	string status = "None";
	int triggerValue = 1; //Default Set Value
	bool canceled = false;
	
	System.Threading.Tasks.Parallel.Invoke(() => status = Common.Patterns.Threading.LockFree.SpinWait(ref triggerValue,
																										out canceled,
																										(currentTriggerValue, nbrRetries) => "Update Function Retries: " + nbrRetries.ToString(),
																										(nbrRetries, maxSpinTimeRetryTimeOut) => { nbrRetries.Dump("Waiting"); return true;},
																										1,
																										0,
																										-1), //Set Spin Retries to 100
											() =>
												{
													status.Dump("Before Sleep");
							
													System.Threading.Thread.Sleep(10);
													
													status.Dump("After Sleep");
													
													triggerValue = 0; //Default UnSet Value
													
													status.Dump("After UnSet");
												}
											);
											
	status.Dump("Result");
}

// Define other methods and classes here