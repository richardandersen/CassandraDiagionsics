<Query Kind="Statements">
  <Reference>&lt;RuntimeDirectory&gt;\System.Web.Extensions.dll</Reference>
</Query>

IEnumerable<int> sequenceOfInts = new int[] { 1, 2, 3 };
var sequenceOfFoos = new object[] { new { Bar = "A" }, new { Bar = "B" } };

var serializer = new System.Web.Script.Serialization.JavaScriptSerializer();
serializer.Serialize(sequenceOfInts).Dump();
var jsonFoos = serializer.Serialize(sequenceOfFoos).Dump();

serializer.Deserialize<IEnumerable<object>>(jsonFoos).Dump();