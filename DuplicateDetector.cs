using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp;

public static class DuplicateDetector
{
    public static async Task GenerateDuplicateReport()
{
    var client = new MongoClient("mongodb://localhost:27017");
    var database = client.GetDatabase("UILM_DB");

    var rawCollection = database.GetCollection<BsonDocument>("Raw_Keys");
    var reportCollection = database.GetCollection<BsonDocument>("Duplicate_Key_Report");

    var pipeline = new[]
    {
        new BsonDocument("$group",
            new BsonDocument
            {
                { "_id", "$KeyName" },
                { "TotalDuplicateCount", new BsonDocument("$sum", 1) },
                { "Modules", new BsonDocument("$push",
                    new BsonDocument
                    {
                        { "Module", "$Module" },
                        { "Id", "$_id" },
                        { "Resources", "$Resources" }
                    })
                }
            }),

        new BsonDocument("$match",
            new BsonDocument("TotalDuplicateCount",
                new BsonDocument("$gt", 1))),

        new BsonDocument("$addFields",
            new BsonDocument
            {
                { "HasRootModule",
                    new BsonDocument("$in", new BsonArray { "app-root", "$Modules.Module" })
                },
                { "HasGenericModule",
                    new BsonDocument("$in", new BsonArray { "generic-app", "$Modules.Module" })
                }
            }),

       new BsonDocument("$project",
    new BsonDocument
    {
        { "_id", 0 },
        { "KeyName", "$_id" },
        { "TotalDuplicateCount", 1 },
        { "Modules", 1 },
        { "HasRootModule", 1 },
        { "HasGenericModule", 1 },
        { "Processed", new BsonDocument("$literal", false) }
    })
    };

    var result = await rawCollection.Aggregate<BsonDocument>(pipeline).ToListAsync();
        // 🔹 C# Level Calculation
        // 🔹 C# Level Calculation
        foreach (var doc in result)
        {
            bool allCultureValuesSame = true;

            var modules = doc["Modules"].AsBsonArray;

            var cultureMap = new Dictionary<string, List<string>>();

            foreach (var module in modules)
            {
                var resources = module["Resources"].AsBsonArray;

                // 🔹 Remove id and app id cultures
                var filteredResources = new BsonArray();

                foreach (var res in resources)
                {
                    var culture = res["Culture"].AsString;

                    if (culture == "id" || culture == "app id")
                        continue;

                    filteredResources.Add(res);

                    var value = res["Value"].AsString;

                    if (!cultureMap.ContainsKey(culture))
                        cultureMap[culture] = new List<string>();

                    cultureMap[culture].Add(Normalize(value));
                }

                // replace cleaned resources
                module["Resources"] = filteredResources;
            }

            // 🔹 Check if all culture values are same
            foreach (var culture in cultureMap)
            {
                if (culture.Value.Distinct().Count() > 1)
                {
                    allCultureValuesSame = false;
                    break;
                }
            }

            doc["AllCultureValuesSame"] = allCultureValuesSame;
        }



        if (result.Any())
    {
        await reportCollection.DeleteManyAsync(Builders<BsonDocument>.Filter.Empty);
        await reportCollection.InsertManyAsync(result);

        Console.WriteLine($"✅ Duplicate Report Generated: {result.Count} records");
    }
    else
    {
        Console.WriteLine("No duplicates found.");
    }
}
    // 🔹 Helper Method: Normalize string for comparison
    private static string Normalize(string value)
    {
        if (string.IsNullOrWhiteSpace(value)) return string.Empty;

        value = value.Trim(); // remove leading/trailing spaces
        value = value.ToLowerInvariant(); // ignore case
        value = new string(value.Where(c => !char.IsPunctuation(c)).ToArray()); // remove punctuation
        return value;
    }
}


