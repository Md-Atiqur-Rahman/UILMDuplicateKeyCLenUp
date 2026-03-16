using MongoDB.Bson;
using MongoDB.Driver;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Previewer;
using System.Text.RegularExpressions;

namespace ConsoleApp;

public static class TestDuplicateReportAnalyzer
{
    private class SummaryStats
    {
        public int TotalDuplicateKeys { get; set; }
        public int TotalDuplicateModules { get; set; }
        public int DuplicateKeyForSameLanguageTrue { get; set; }
        public int DuplicateModuleForSameLanguageTrue { get; set; }
        public int DuplicateKeyForSameLanguageFalse { get; set; }
        public int DuplicateModuleForSameLanguageFalse { get; set; }
        public int DeletedModules { get; set; }
    }

    private class NormalizedModuleData
    {
        public string ModuleName { get; set; }
        public string RawEnUS { get; set; }
        public string RawDeDE { get; set; }
        public string RawFrFR { get; set; }
        public string RawItIT { get; set; }
        public string NormalizedEnUS { get; set; }
        public string NormalizedDeDE { get; set; }
        public string NormalizedFrFR { get; set; }
        public string NormalizedItIT { get; set; }
        public bool IsNormalized { get; set; }
        public bool AllCulturesNormalizedSame { get; set; }
        public bool AllCulturesRawSame { get; set; }
    }

    private class CultureMismatchDetail
    {
        public string Culture { get; set; }
        public Dictionary<string, List<string>> ModuleValues { get; set; } = new();
        public bool IsConsistent { get; set; }
        public int DistinctValueCount { get; set; }
    }

    public static async Task GenerateSummaryReport(
        string? keyNameFilter = null,
        bool? hasRootModule = null,
        bool? hasGenericModule = null,
        bool? isConsistent = null,
        bool isDeletePermission = false)
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("UILM_DB");
        var collection = database.GetCollection<BsonDocument>("Duplicate_Key_Report");
        var deletedModulesCollection = database.GetCollection<BsonDocument>("Deleted_Modules_Archive");

        var filterBuilder = Builders<BsonDocument>.Filter;
        var filter = filterBuilder.Empty;

        if (!string.IsNullOrEmpty(keyNameFilter))
            filter &= filterBuilder.Eq("KeyName", keyNameFilter);

        if (hasRootModule.HasValue)
            filter &= filterBuilder.Eq("HasRootModule", hasRootModule.Value);

        if (hasGenericModule.HasValue)
            filter &= filterBuilder.Eq("HasGenericModule", hasGenericModule.Value);

        if (isConsistent.HasValue)
            filter &= filterBuilder.Eq("IsConsistent", isConsistent.Value);

        var results = await collection.Find(filter).ToListAsync();

        if (!results.Any())
        {
            Console.WriteLine("No matching records found.");
            return;
        }

        // ✅ Execute delete operation if permission is granted
        if (isDeletePermission)
        {
            await ExecuteDeleteOperation(results, collection, deletedModulesCollection);
        }

        GeneratePdf(results, isDeletePermission);

        Console.WriteLine("✅ PDF Report Generated : DuplicateKeyReport.pdf");
    }

    private static async Task ExecuteDeleteOperation(
        List<BsonDocument> results,
        IMongoCollection<BsonDocument> collection,
        IMongoCollection<BsonDocument> deletedModulesCollection)
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("UILM_DB");
        var migrationResultCollection = database.GetCollection<BsonDocument>("MigrationResult");

        var deletedRecords = new List<BsonDocument>();
        
        // Track migration results by filter type
        var migrationResults = new Dictionary<string, MigrationFilterResult>
        {
            { "A", new MigrationFilterResult { FilterType = "A", HasRootModule = false, HasGenericModule = false, IsConsistent = true } },
            { "B", new MigrationFilterResult { FilterType = "B", HasRootModule = false, HasGenericModule = true, IsConsistent = true } },
            { "C", new MigrationFilterResult { FilterType = "C", HasRootModule = true, HasGenericModule = false, IsConsistent = true } },
            { "D", new MigrationFilterResult { FilterType = "D", HasRootModule = true, HasGenericModule = true, IsConsistent = true } }
        };

        foreach (var doc in results)
        {
            bool hasRoot = doc["HasRootModule"].AsBoolean;
            bool hasGeneric = doc["HasGenericModule"].AsBoolean;
            bool isConsistent = doc["IsConsistent"].AsBoolean;
            string keyName = doc["KeyName"].AsString;

            // Determine filter type
            string filterType = hasRoot switch
            {
                false when !hasGeneric => "A",
                false when hasGeneric => "B",
                true when !hasGeneric => "C",
                true when hasGeneric => "D",
                _ => ""
            };

            if (string.IsNullOrEmpty(filterType))
                continue;

            var modules = doc["Modules"].AsBsonArray;
            int previousModuleCount = modules.Count;
            int deletedModuleCount = 0;

            // ✅ FILTER A: HasRoot=False, HasGeneric=False, IsConsistent=True
            if (!hasRoot && !hasGeneric && isConsistent)
            {
                var modulesToDelete = new BsonArray(modules.Cast<BsonValue>());
                deletedModuleCount = modulesToDelete.Count;

                // Extract consistent data from first module
                var firstModule = modules[0].AsBsonDocument;
                var consistentResources = firstModule["Resources"].AsBsonArray;

                // Create new root module with consistent data
                var newRootModule = new BsonDocument
                {
                    { "Module", "app-root" },
                    { "Id", ObjectId.GenerateNewId().ToString() },
                    { "Resources", consistentResources }
                };

                var modulesToKeep = new BsonArray { newRootModule };

                // ✅ Archive deleted modules
                var archivedRecord = new BsonDocument
                {
                    { "_id", ObjectId.GenerateNewId() },
                    { "KeyName", keyName },
                    { "FilterType", "A" },
                    { "DeletedAt", DateTime.UtcNow },
                    { "HasRootModule", hasRoot },
                    { "HasGenericModule", hasGeneric },
                    { "IsConsistent", isConsistent },
                    { "DeletedModules", modulesToDelete },
                    { "NewRootModuleCreated", newRootModule },
                    { "ConsistentDataExtractedFrom", firstModule["Module"] }
                };

                deletedRecords.Add(archivedRecord);

                // ✅ Update original document
                var updateDefinition = Builders<BsonDocument>.Update
                    .Set("Modules", modulesToKeep)
                    .Set("UpdatedAt", DateTime.UtcNow)
                    .Set("ModulesDeletionTriggered", true)
                    .Set("FilterType", "A");

                await collection.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]),
                    updateDefinition);

                Console.WriteLine($"✅ [FILTER A] Deleted {deletedModuleCount} modules for KeyName: {keyName}");
                Console.WriteLine($"   └─ Created new 'app-root' module with consistent data");
            }

            // ✅ FILTER B: HasRoot=False, HasGeneric=True, IsConsistent=True
            else if (!hasRoot && hasGeneric && isConsistent)
            {
                var modulesToKeep = new BsonArray();
                var modulesToDelete = new BsonArray();
                var genericAppKept = false;  // Track if we've already kept one generic-app

                foreach (var module in modules)
                {
                    var moduleName = module["Module"].AsString;

                    // Keep only ONE generic-app module
                    if (moduleName == "generic-app" && !genericAppKept)
                    {
                        modulesToKeep.Add(module);
                        genericAppKept = true;  // Mark that we've kept one
                    }
                    else
                    {
                        // Delete: all other modules OR duplicate generic-app
                        modulesToDelete.Add(module);
                    }
                }

                deletedModuleCount = modulesToDelete.Count;

                if (modulesToDelete.Count > 0)
                {
                    var archivedRecord = new BsonDocument
                    {
                        { "_id", ObjectId.GenerateNewId() },
                        { "KeyName", keyName },
                        { "FilterType", "B" },
                        { "DeletedAt", DateTime.UtcNow },
                        { "HasRootModule", hasRoot },
                        { "HasGenericModule", hasGeneric },
                        { "IsConsistent", isConsistent },
                        { "DeletedModules", modulesToDelete },
                        { "GenericModuleKept", modulesToKeep.Count > 0 ? modulesToKeep[0] : null },
                        { "DuplicateGenericAppRemoved", modulesToDelete.Cast<BsonDocument>()
                            .Count(m => m["Module"].AsString == "generic-app") }  // ✅ Track duplicates
                    };

                    deletedRecords.Add(archivedRecord);
                }

                var updateDefinition = Builders<BsonDocument>.Update
                    .Set("Modules", modulesToKeep)  // ✅ Only ONE generic-app remains
                    .Set("UpdatedAt", DateTime.UtcNow)
                    .Set("ModulesDeletionTriggered", true)
                    .Set("FilterType", "B");

                await collection.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]),
                    updateDefinition);

                Console.WriteLine($"✅ [FILTER B] Deleted {deletedModuleCount} modules for KeyName: {keyName}");
                Console.WriteLine($"   └─ Kept ONE 'generic-app' module only");
                if (deletedModuleCount - (modulesToDelete.Cast<BsonDocument>().Count(m => m["Module"].AsString == "generic-app") - 1) > 0)
                    Console.WriteLine($"   └─ Removed {modulesToDelete.Cast<BsonDocument>().Count(m => m["Module"].AsString == "generic-app")} duplicate generic-app(s)");
            }

            // ✅ FILTER C: HasRoot=True, HasGeneric=False, IsConsistent=True
            else if (hasRoot && !hasGeneric && isConsistent)
            {
                var modulesToKeep = new BsonArray();
                var modulesToDelete = new BsonArray();

                foreach (var module in modules)
                {
                    var moduleName = module["Module"].AsString;

                    if (moduleName == "app-root")
                    {
                        modulesToKeep.Add(module);
                    }
                    else
                    {
                        modulesToDelete.Add(module);
                    }
                }

                deletedModuleCount = modulesToDelete.Count;

                if (modulesToDelete.Count > 0)
                {
                    var archivedRecord = new BsonDocument
                    {
                        { "_id", ObjectId.GenerateNewId() },
                        { "KeyName", keyName },
                        { "FilterType", "C" },
                        { "DeletedAt", DateTime.UtcNow },
                        { "HasRootModule", hasRoot },
                        { "HasGenericModule", hasGeneric },
                        { "IsConsistent", isConsistent },
                        { "DeletedModules", modulesToDelete },
                        { "RootModuleKept", modulesToKeep.Count > 0 ? modulesToKeep[0] : null }
                    };

                    deletedRecords.Add(archivedRecord);
                }

                var updateDefinition = Builders<BsonDocument>.Update
                    .Set("Modules", modulesToKeep)
                    .Set("UpdatedAt", DateTime.UtcNow)
                    .Set("ModulesDeletionTriggered", true)
                    .Set("FilterType", "C");

                await collection.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]),
                    updateDefinition);

                Console.WriteLine($"✅ [FILTER C] Deleted {deletedModuleCount} modules for KeyName: {keyName}");
                Console.WriteLine($"   └─ Kept 'app-root' module only");
            }

            // ✅ FILTER D: HasRoot=True, HasGeneric=True, IsConsistent=True
            else if (hasRoot && hasGeneric && isConsistent)
            {
                var modulesToKeep = new BsonArray();
                var modulesToDelete = new BsonArray();

                foreach (var module in modules)
                {
                    var moduleName = module["Module"].AsString;

                    if (moduleName == "app-root")
                    {
                        modulesToKeep.Add(module);
                    }
                    else
                    {
                        modulesToDelete.Add(module);
                    }
                }

                deletedModuleCount = modulesToDelete.Count;

                if (modulesToDelete.Count > 0)
                {
                    var archivedRecord = new BsonDocument
                    {
                        { "_id", ObjectId.GenerateNewId() },
                        { "KeyName", keyName },
                        { "FilterType", "D" },
                        { "DeletedAt", DateTime.UtcNow },
                        { "HasRootModule", hasRoot },
                        { "HasGenericModule", hasGeneric },
                        { "IsConsistent", isConsistent },
                        { "DeletedModules", modulesToDelete },
                        { "RootModuleKept", modulesToKeep[0] }
                    };

                    deletedRecords.Add(archivedRecord);
                }

                var updateDefinition = Builders<BsonDocument>.Update
                    .Set("Modules", modulesToKeep)
                    .Set("UpdatedAt", DateTime.UtcNow)
                    .Set("ModulesDeletionTriggered", true)
                    .Set("FilterType", "D");

                await collection.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]),
                    updateDefinition);

                Console.WriteLine($"✅ [FILTER D] Deleted {deletedModuleCount} modules for KeyName: {keyName}");
            }

            // Track migration result
            if (!string.IsNullOrEmpty(filterType) && migrationResults.ContainsKey(filterType))
            {
                migrationResults[filterType].Keys.Add(new KeyMigrationDetail
                {
                    Name = keyName,
                    PreviousModules = previousModuleCount,
                    DeletedModules = deletedModuleCount
                });
            }
        }

        // ✅ Insert archived records
        if (deletedRecords.Any())
        {
            await deletedModulesCollection.InsertManyAsync(deletedRecords);
            Console.WriteLine($"\n✅ Archived {deletedRecords.Count} deletion records to 'Deleted_Modules_Archive'");
            
            // Summary
            var filterACount = deletedRecords.Count(r => r["FilterType"].AsString == "A");
            var filterBCount = deletedRecords.Count(r => r["FilterType"].AsString == "B");
            var filterCCount = deletedRecords.Count(r => r["FilterType"].AsString == "C");
            var filterDCount = deletedRecords.Count(r => r["FilterType"].AsString == "D");
            
            Console.WriteLine("\n📊 Deletion Summary:");
            if (filterACount > 0)
                Console.WriteLine($"   • Filter A: {filterACount} records (Deleted all + Created new 'app-root')");
            if (filterBCount > 0)
                Console.WriteLine($"   • Filter B: {filterBCount} records (Kept 'generic-app' only)");
            if (filterCCount > 0)
                Console.WriteLine($"   • Filter C: {filterCCount} records (Kept 'app-root' only)");
            if (filterDCount > 0)
                Console.WriteLine($"   • Filter D: {filterDCount} records (Kept 'app-root' only)");
        }

        // ✅ Store migration results
        await StoreMigrationResults(migrationResultCollection, migrationResults);
    }

    private static async Task StoreMigrationResults(
        IMongoCollection<BsonDocument> migrationResultCollection,
        Dictionary<string, MigrationFilterResult> migrationResults)
    {
        var resultsToStore = new List<BsonDocument>();

        foreach (var (filterType, filterResult) in migrationResults)
        {
            if (filterResult.Keys.Count == 0)
                continue;

            var keysArray = new BsonArray(filterResult.Keys.Select(k =>
                new BsonDocument
                {
                    { "name", k.Name },
                    { "PreviousModules", k.PreviousModules },
                    { "DeletedModules", k.DeletedModules }
                }
            ));

            var migrationRecord = new BsonDocument
            {
                { "_id", ObjectId.GenerateNewId() },
                { "FilterType", filterType },
                { "HasRootModule", filterResult.HasRootModule },
                { "HasGenericModule", filterResult.HasGenericModule },
                { "IsConsistent", filterResult.IsConsistent },
                { "Keys", keysArray },
                { "TotalKeys", filterResult.Keys.Count },
                { "MigratedAt", DateTime.UtcNow }
            };

            resultsToStore.Add(migrationRecord);
        }

        if (resultsToStore.Any())
        {
            // Clear previous migration results (optional - update instead of insert)
            await migrationResultCollection.DeleteManyAsync(Builders<BsonDocument>.Filter.Empty);
            
            // Insert new migration results
            await migrationResultCollection.InsertManyAsync(resultsToStore);
            
            Console.WriteLine($"\n✅ Stored {resultsToStore.Count} migration results to 'MigrationResult'");
            
            foreach (var record in resultsToStore)
            {
                var filterType = record["FilterType"].AsString;
                var totalKeys = record["TotalKeys"].AsInt32;
                Console.WriteLine($"   • Filter {filterType}: {totalKeys} keys migrated");
            }
        }
    }

    private class MigrationFilterResult
    {
        public string FilterType { get; set; }
        public bool HasRootModule { get; set; }
        public bool HasGenericModule { get; set; }
        public bool IsConsistent { get; set; }
        public List<KeyMigrationDetail> Keys { get; set; } = new();
    }

    private class KeyMigrationDetail
    {
        public string Name { get; set; }
        public int PreviousModules { get; set; }
        public int DeletedModules { get; set; }
    }

    private static string NormalizeValue(string value)
    {
        if (string.IsNullOrEmpty(value))
            return "";

        value = value.Trim();
        value = value.ToLower();
        value = Regex.Replace(value, @"[^\w\s]", "");
        value = Regex.Replace(value, @"\s+", " ").Trim();

        return value;
    }

    private static bool AreValuesNormalized(string value1, string value2)
    {
        if (string.IsNullOrEmpty(value1) && string.IsNullOrEmpty(value2))
            return true;

        var normalized1 = NormalizeValue(value1);
        var normalized2 = NormalizeValue(value2);

        return normalized1 == normalized2;
    }

    private static Dictionary<string, SummaryStats> GetSummaryStatistics(List<BsonDocument> results)
    {
        var summaryData = new Dictionary<string, SummaryStats>
        {
            { "A", new SummaryStats() },
            { "B", new SummaryStats() },
            { "C", new SummaryStats() },
            { "D", new SummaryStats() }
        };

        foreach (var doc in results)
        {
            bool root = doc["HasRootModule"].AsBoolean;
            bool generic = doc["HasGenericModule"].AsBoolean;
            bool consistent = doc["IsConsistent"].AsBoolean;

            string key = root switch
            {
                false when !generic => "A",
                false when generic => "B",
                true when !generic => "C",
                true when generic => "D",
                _ => ""
            };

            if (!string.IsNullOrEmpty(key))
            {
                int moduleCount = doc["Modules"].AsBsonArray.Count;

                summaryData[key].TotalDuplicateKeys++;
                summaryData[key].TotalDuplicateModules += moduleCount;

                if (consistent)
                {
                    summaryData[key].DuplicateKeyForSameLanguageTrue++;
                    summaryData[key].DuplicateModuleForSameLanguageTrue += moduleCount;
                }
                else
                {
                    summaryData[key].DuplicateKeyForSameLanguageFalse++;
                    summaryData[key].DuplicateModuleForSameLanguageFalse += moduleCount;
                }

                // Count deleted modules for all filters when IsConsistent=True
                if (consistent)
                {
                    switch (key)
                    {
                        case "A":
                            // Filter A: Delete all modules, then create new root
                            summaryData[key].DeletedModules += moduleCount;
                            break;
                        case "B":
                            // Filter B: Delete all except generic-app
                            int deletedCountB = moduleCount - 1;
                            summaryData[key].DeletedModules += Math.Max(0, deletedCountB);
                            break;
                        case "C":
                            // Filter C: Delete all except app-root
                            int deletedCountC = moduleCount - 1;
                            summaryData[key].DeletedModules += Math.Max(0, deletedCountC);
                            break;
                        case "D":
                            // Filter D: Delete all except app-root
                            int deletedCountD = moduleCount - 1;
                            summaryData[key].DeletedModules += Math.Max(0, deletedCountD);
                            break;
                    }
                }
            }
        }

        return summaryData;
    }

    private static List<NormalizedModuleData> GetNormalizedModules(BsonDocument doc)
    {
        var normalizedModules = new List<NormalizedModuleData>();
        var modules = doc["Modules"].AsBsonArray;

        foreach (var module in modules)
        {
            var moduleName = module["Module"].AsString;
            string en = "", de = "", fr = "", it = "";

            foreach (var res in module["Resources"].AsBsonArray)
            {
                var culture = res["Culture"].AsString;
                var value = res["Value"].AsString;

                if (culture == "en-US") en = value;
                if (culture == "de-DE") de = value;
                if (culture == "fr-FR") fr = value;
                if (culture == "it-IT") it = value;
            }

            var normEN = NormalizeValue(en);
            var normDE = NormalizeValue(de);
            var normFR = NormalizeValue(fr);
            var normIT = NormalizeValue(it);

            var allRawSame = en == de && de == fr && fr == it && !string.IsNullOrEmpty(en);
            var allNormSame = normEN == normDE && normDE == normFR && normFR == normIT && !string.IsNullOrEmpty(normEN);

            var normalizedData = new NormalizedModuleData
            {
                ModuleName = moduleName,
                RawEnUS = en,
                RawDeDE = de,
                RawFrFR = fr,
                RawItIT = it,
                NormalizedEnUS = normEN,
                NormalizedDeDE = normDE,
                NormalizedFrFR = normFR,
                NormalizedItIT = normIT,
                IsNormalized = normEN != en || normDE != de || normFR != fr || normIT != it,
                AllCulturesRawSame = allRawSame,
                AllCulturesNormalizedSame = allNormSame
            };

            normalizedModules.Add(normalizedData);
        }

        return normalizedModules;
    }

    private static bool AreCultureValuesConsistent(List<NormalizedModuleData> modules)
    {
        var enValues = modules.Select(m => NormalizeValue(m.RawEnUS)).Distinct().Count();
        var deValues = modules.Select(m => NormalizeValue(m.RawDeDE)).Distinct().Count();
        var frValues = modules.Select(m => NormalizeValue(m.RawFrFR)).Distinct().Count();
        var itValues = modules.Select(m => NormalizeValue(m.RawItIT)).Distinct().Count();

        // All cultures should have exactly 1 distinct value across all modules
        return enValues == 1 && deValues == 1 && frValues == 1 && itValues == 1;
    }

    private static List<CultureMismatchDetail> GetCultureMismatchDetails(List<NormalizedModuleData> modules)
    {
        var mismatchDetails = new List<CultureMismatchDetail>();
        var cultures = new[] { "en-US", "de-DE", "fr-FR", "it-IT" };

        foreach (var culture in cultures)
        {
            var mismatch = new CultureMismatchDetail { Culture = culture };
            var valueModuleMap = new Dictionary<string, List<string>>();

            // Group modules by normalized culture value
            foreach (var module in modules)
            {
                string rawValue = culture switch
                {
                    "en-US" => module.RawEnUS,
                    "de-DE" => module.RawDeDE,
                    "fr-FR" => module.RawFrFR,
                    "it-IT" => module.RawItIT,
                    _ => ""
                };

                string normalizedValue = NormalizeValue(rawValue);

                if (!valueModuleMap.ContainsKey(normalizedValue))
                    valueModuleMap[normalizedValue] = new List<string>();

                valueModuleMap[normalizedValue].Add($"{module.ModuleName} ({rawValue})");
            }

            mismatch.ModuleValues = valueModuleMap;
            mismatch.DistinctValueCount = valueModuleMap.Count;
            mismatch.IsConsistent = mismatch.DistinctValueCount == 1;

            mismatchDetails.Add(mismatch);
        }

        return mismatchDetails;
    }

    private static void GeneratePdf(List<BsonDocument> results, bool isDeletePermission)
    {
        var summaryStats = GetSummaryStatistics(results);

        var document = Document.Create(container =>
        {
            container.Page(page =>
            {
                page.Size(PageSizes.A4);
                page.Margin(20);
                page.Content().Column(col =>
                {
                    col.Item().Text("Duplicate Key Analysis Report (With Normalization)")
                        .FontSize(20)
                        .Bold();

                    col.Item().Text($"Generated: {DateTime.Now}")
                        .FontSize(10);

                    if (isDeletePermission)
                    {
                        col.Item().PaddingTop(5).Text("⚠️ DELETE OPERATION EXECUTED")
                            .FontSize(10).Bold().FontColor("#FF0000");
                        
                        col.Item().Text("Filter A: All modules deleted + new 'app-root' created with consistent data")
                            .FontSize(8).FontColor("#FF6B6B");
                        
                        col.Item().Text("Filter B: All modules except 'generic-app' deleted")
                            .FontSize(8).FontColor("#FF6B6B");
                        
                        col.Item().Text("Filter C: All modules except 'app-root' deleted")
                            .FontSize(8).FontColor("#FF6B6B");
                        
                        col.Item().Text("Filter D: All modules except 'app-root' deleted")
                            .FontSize(8).FontColor("#FF6B6B");
                    }

                    col.Item().PaddingTop(20).Text("Summary Statistics")
                        .FontSize(14)
                        .Bold();

                    // Summary Table
                    col.Item().PaddingTop(10).Table(table =>
                    {
                        table.ColumnsDefinition(columns =>
                        {
                            columns.RelativeColumn(0.8f);
                            columns.RelativeColumn(1.2f);
                            columns.RelativeColumn(1.2f);
                            columns.RelativeColumn(1.2f);
                            columns.RelativeColumn(1.2f);
                            columns.RelativeColumn(1.8f);
                            columns.RelativeColumn(1.8f);
                            columns.RelativeColumn(1.8f);
                            columns.RelativeColumn(1.8f);
                            columns.RelativeColumn(1.2f);
                        });

                        table.Header(header =>
                        {
                            header.Cell().Background("#4472C4").Text("Filter").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Root").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Generic").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Total Dup Keys").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Total Dup Modules").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Consistent Keys").FontSize(6).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Consistent Modules").FontSize(6).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Inconsistent Keys").FontSize(6).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Inconsistent Modules").FontSize(6).Bold().FontColor(Colors.White);
                            header.Cell().Background("#FF6B6B").Text("Deleted").FontSize(7).Bold().FontColor(Colors.White);
                        });

                        var rows = new[] { ("A", false, false), ("B", false, true), ("C", true, false), ("D", true, true) };
                        int grandTotalKeys = 0, grandTotalModules = 0, grandKeysTrue = 0, grandModulesTrue = 0, grandKeysFalse = 0, grandModulesFalse = 0, grandDeleted = 0;

                        foreach (var (letter, hasRoot, hasGeneric) in rows)
                        {
                            var stats = summaryStats[letter];
                            grandTotalKeys += stats.TotalDuplicateKeys;
                            grandTotalModules += stats.TotalDuplicateModules;
                            grandKeysTrue += stats.DuplicateKeyForSameLanguageTrue;
                            grandModulesTrue += stats.DuplicateModuleForSameLanguageTrue;
                            grandKeysFalse += stats.DuplicateKeyForSameLanguageFalse;
                            grandModulesFalse += stats.DuplicateModuleForSameLanguageFalse;
                            grandDeleted += stats.DeletedModules;

                            table.Cell().Background("#E7E6E6").Text(letter).FontSize(7).Bold();
                            table.Cell().Text(hasRoot.ToString()).FontSize(7);
                            table.Cell().Text(hasGeneric.ToString()).FontSize(7);
                            table.Cell().Text(stats.TotalDuplicateKeys.ToString()).FontSize(7).Bold();
                            table.Cell().Text(stats.TotalDuplicateModules.ToString()).FontSize(7).Bold();
                            table.Cell().Text(stats.DuplicateKeyForSameLanguageTrue.ToString()).FontSize(7);
                            table.Cell().Text(stats.DuplicateModuleForSameLanguageTrue.ToString()).FontSize(7);
                            table.Cell().Text(stats.DuplicateKeyForSameLanguageFalse.ToString()).FontSize(7);
                            table.Cell().Text(stats.DuplicateModuleForSameLanguageFalse.ToString()).FontSize(7);
                            table.Cell().Background("#FFE0E0").Text(stats.DeletedModules.ToString()).FontSize(7).Bold();
                        }

                        table.Cell().Background("#D3D3D3").Text("Total").FontSize(7).Bold();
                        table.Cell().Background("#D3D3D3").Text("").FontSize(7);
                        table.Cell().Background("#D3D3D3").Text("").FontSize(7);
                        table.Cell().Background("#D3D3D3").Text(grandTotalKeys.ToString()).FontSize(7).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandTotalModules.ToString()).FontSize(7).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandKeysTrue.ToString()).FontSize(7).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandModulesTrue.ToString()).FontSize(7).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandKeysFalse.ToString()).FontSize(7).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandModulesFalse.ToString()).FontSize(7).Bold();
                        table.Cell().Background("#FFD3D3").Text(grandDeleted.ToString()).FontSize(7).Bold();
                    });

                    col.Item().PaddingTop(30).Text("Detailed Key Analysis")
                        .FontSize(14)
                        .Bold();

                    foreach (var doc in results)
                    {
                        string key = doc["KeyName"].AsString;
                        bool root = doc["HasRootModule"].AsBoolean;
                        bool generic = doc["HasGenericModule"].AsBoolean;
                        bool consistent = doc["IsConsistent"].AsBoolean;

                        col.Item().PaddingTop(15).Text($"KeyName: {key}")
                            .FontSize(12)
                            .Bold();

                        col.Item().Text($"HasRootModule: {root} | HasGenericModule: {generic} | IsConsistent: {consistent}")
                            .FontSize(9);

                        var normalizedModules = GetNormalizedModules(doc);
                        bool cultureConsistent = AreCultureValuesConsistent(normalizedModules);

                        if (cultureConsistent)
                        {
                            col.Item().PaddingTop(5).Text("✓ All modules use consistent translations")
                                .FontSize(9).Bold().FontColor("#008000");
                        }
                        else
                        {
                            col.Item().PaddingTop(5).Text("✗ Inconsistent translations detected")
                                .FontSize(9).Bold().FontColor("#FF0000");

                            var mismatchDetails = GetCultureMismatchDetails(normalizedModules);

                            col.Item().PaddingTop(8).Text("Issues by Culture:")
                                .FontSize(8).Bold();

                            foreach (var mismatch in mismatchDetails)
                            {
                                if (!mismatch.IsConsistent)
                                {
                                    col.Item().PaddingTop(5).Text($"{mismatch.Culture}: {mismatch.DistinctValueCount} different values")
                                        .FontSize(7).FontColor("#FF0000").Bold();

                                    foreach (var kvp in mismatch.ModuleValues)
                                    {
                                        col.Item().Text($"  Value: \"{kvp.Key}\"")
                                            .FontSize(7);

                                        foreach (var moduleInfo in kvp.Value)
                                        {
                                            col.Item().Text($"    • {moduleInfo}")
                                                .FontSize(6);
                                        }
                                    }
                                }
                            }
                        }

                        col.Item().PaddingTop(8).Table(table =>
                        {
                            table.ColumnsDefinition(columns =>
                            {
                                columns.RelativeColumn(1.5f);
                                columns.RelativeColumn(1.5f);
                                columns.RelativeColumn(1.5f);
                                columns.RelativeColumn(1.5f);
                                columns.RelativeColumn(1.5f);
                                columns.RelativeColumn(0.8f);
                            });

                            table.Header(header =>
                            {
                                header.Cell().Background("#70AD47").Text("Module").FontSize(8).Bold().FontColor(Colors.White);
                                header.Cell().Background("#70AD47").Text("en-US").FontSize(8).Bold().FontColor(Colors.White);
                                header.Cell().Background("#70AD47").Text("de-DE").FontSize(8).Bold().FontColor(Colors.White);
                                header.Cell().Background("#70AD47").Text("fr-FR").FontSize(8).Bold().FontColor(Colors.White);
                                header.Cell().Background("#70AD47").Text("it-IT").FontSize(8).Bold().FontColor(Colors.White);
                                header.Cell().Background("#70AD47").Text("Match").FontSize(8).Bold().FontColor(Colors.White);
                            });

                            foreach (var moduleData in normalizedModules)
                            {
                                var sameStatus = moduleData.AllCulturesNormalizedSame ? "✓" : "✗";
                                var statusColor = moduleData.AllCulturesNormalizedSame ? "#C6EFCE" : "#FFC7CE";

                                table.Cell().Background(statusColor).Text(moduleData.ModuleName).FontSize(8);
                                table.Cell().Background(statusColor).Text(moduleData.NormalizedEnUS).FontSize(8);
                                table.Cell().Background(statusColor).Text(moduleData.NormalizedDeDE).FontSize(8);
                                table.Cell().Background(statusColor).Text(moduleData.NormalizedFrFR).FontSize(8);
                                table.Cell().Background(statusColor).Text(moduleData.NormalizedItIT).FontSize(8);
                                table.Cell().Background(statusColor).Text(sameStatus).FontSize(8).Bold();
                            }
                        });
                    }
                });
            });
        });

        document.GeneratePdf("DuplicateKeyReport.pdf");
    }
}