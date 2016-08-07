<Query Kind="Program">
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Console.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Functions.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Path.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.FileWatcher.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Shared.dll</Reference>
  <Reference>&lt;ProgramFilesX86&gt;\AndersenAssoc\Common Library Components\Common.Patterns.Singleton.dll</Reference>
</Query>

void Main()
{
	var keysGenerated = new HashSet<int>();
	var randomIndex = new Random();
	
	var file = new Common.File.FilePathAbsolute(@"D:\Projects\DataStax\Projects\3M\EPRS\Performance\RandomUpdateAccess.txt"); //@"D:\Projects\DataStax\Projects\3M\EPRS\Performance\RandomReadAccess.txt"
	
	using(var fileAccess = file.CreateText())
	{
		int nIndex = -1;
		
		do
		{
			nIndex = randomIndex.Next(0, 2512);
			
			if(!keysGenerated.Contains(nIndex))
			{
				fileAccess.Write(nIndex);
				fileAccess.Write(',');
				keysGenerated.Add(nIndex);
				
				Console.Write(nIndex.ToString() + " ");
				Console.WriteLine(keysGenerated.Count());
			}
			
		} while (keysGenerated.Count() < 2512);
	}
}

// Define other methods and classes here