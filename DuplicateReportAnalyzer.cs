using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ConsoleApp;

public static class DuplicateReportAnalyzer
{
    public static async Task GenerateSummaryReport(
        string keyNameFilter = null,
        bool? hasRootModule = null,
        bool? hasGenericModule = null,
        bool? allCultureValuesSame = null)
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("UILM_DB");
        var reportCollection = database.GetCollection<BsonDocument>("Duplicate_Key_Report");

        // 🔹 Build dynamic filter
        var filterBuilder = Builders<BsonDocument>.Filter;
        var filters = new List<FilterDefinition<BsonDocument>>();

        if (!string.IsNullOrEmpty(keyNameFilter))
            filters.Add(filterBuilder.Eq("KeyName", keyNameFilter));

        if (hasRootModule.HasValue)
            filters.Add(filterBuilder.Eq("HasRootModule", hasRootModule.Value));

        if (hasGenericModule.HasValue)
            filters.Add(filterBuilder.Eq("HasGenericModule", hasGenericModule.Value));

        if (allCultureValuesSame.HasValue)
            filters.Add(filterBuilder.Eq("AllCultureValuesSame", allCultureValuesSame.Value));

        var finalFilter = filters.Any() ? filterBuilder.And(filters) : filterBuilder.Empty;

        var records = await reportCollection.Find(finalFilter).ToListAsync();

        Console.WriteLine("🔹 Duplicate Key Analysis Summary 🔹\n");

        foreach (var doc in records)
        {
            string keyName = doc.GetValue("KeyName", "").AsString;
            bool hasRoot = doc.GetValue("HasRootModule", false).ToBoolean();
            bool hasGeneric = doc.GetValue("HasGenericModule", false).ToBoolean();
            bool allCultureSame = doc.GetValue("AllCultureValuesSame", false).ToBoolean();

            var modules = doc["Modules"].AsBsonArray.Select(m => m["Module"].AsString).ToList();

            // Find culture mismatches
            var cultureMismatch = new Dictionary<string, List<string>>(); // culture -> differing values

            var cultureValuesMap = new Dictionary<string, HashSet<string>>();

            foreach (var module in doc["Modules"].AsBsonArray)
            {
                var resources = module["Resources"].AsBsonArray;
                foreach (var res in resources)
                {
                    string culture = res["Culture"].AsString;
                    string value = res["Value"].AsString.Trim(); // trim leading/trailing spaces
                    if (culture == "id" || culture == "app id") continue; // ignore these

                    // Remove trailing punctuation (dot, comma, colon) for comparison
                    value = Regex.Replace(value, @"[.,:]$", "").Trim();

                    if (!cultureValuesMap.ContainsKey(culture))
                        cultureValuesMap[culture] = new HashSet<string>();

                    cultureValuesMap[culture].Add(value);
                }
            }

            foreach (var kvp in cultureValuesMap)
            {
                if (kvp.Value.Count > 1)
                {
                    cultureMismatch[kvp.Key] = kvp.Value.ToList();
                }
            }

            // Display summary
            Console.WriteLine($"KeyName: {keyName}");
            Console.WriteLine($"Modules: {string.Join(", ", modules)}");
            Console.WriteLine($"HasRootModule: {hasRoot}");
            Console.WriteLine($"HasGenericModule: {hasGeneric}");
            Console.WriteLine($"AllCultureValuesSame: {allCultureSame}");

            if (cultureMismatch.Any())
            {
                Console.WriteLine("Mismatch Cultures:");
                foreach (var cm in cultureMismatch)
                {
                    Console.WriteLine($"  {cm.Key}: {string.Join(" | ", cm.Value)}");
                }
            }

            Console.WriteLine(new string('-', 50));
        }

        Console.WriteLine($"\nTotal Records with AllCultureValuesSame = false: {records.Count}");
    }
}
