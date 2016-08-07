<Query Kind="Program" />

void Main()
{
	var columnValue = new Dictionary<string,string>();
	
	columnValue.Add("CAC|00000|DateStamp", "DS0");
	columnValue.Add("CAC|00000|Status", "S0");
	columnValue.Add("CAC|00000|XmlRecord", "X0");
	columnValue.Add("CAC|Version","00000");
	columnValue.Add("GPCS|00000|DateStamp", "GDS0");
	columnValue.Add("GPCS|00000|Status", "GS0");
	columnValue.Add("GPCS|00000|XmlRecord", "GX0");
	columnValue.Add("GPCS|00001|DateStamp", "GDS1");
	columnValue.Add("GPCS|00001|Status", "GS1");
	columnValue.Add("GPCS|00001|XmlRecord", "GX1");
	columnValue.Add("GPCS|Version","00001");
	
	var fields = from item in columnValue
					where item.Key.Count(charItem => charItem == '|') == 2
					select item;
					
	fields.Dump();
	
	var items = from fieldItem in fields
					let fieldKeys = fieldItem.Key.Split('|')
					group new { Version=fieldKeys[1], Column=fieldKeys[2], Value=fieldItem.Value} by fieldKeys[0] into g1
					select new { ClaimSource = g1.Key,
									Elements = from g1Item in g1												
												group new {Column=g1Item.Column, g1Item.Value} by g1Item.Version into g2
												select new { Version = g2.Key, Values=g2.ToList()}};
												
	items.Dump();
	
	var items2 = from fieldItem in fields
					let fieldKeys = fieldItem.Key.Split('|')
					group new { Column=fieldKeys[2], Value=fieldItem.Value} by new { ClaimSource=fieldKeys[0], Version=fieldKeys[1] } into g1
					select new { ClaimSource = g1.Key,
									Elements = g1.ToDictionary(key => key.Column, value=> value.Value) };
												
	items2.Dump();
					
}

// Define other methods and classes here
