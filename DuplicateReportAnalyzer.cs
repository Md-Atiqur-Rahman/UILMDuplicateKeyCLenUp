using MongoDB.Bson;
using MongoDB.Driver;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Previewer;
using System.Text.RegularExpressions;

namespace ConsoleApp;

public static class DuplicateReportAnalyzer
{
    // ✅ Add these helper classes here
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

    // ✅ Public methods start here
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

        //GeneratePdf(results, isDeletePermission);

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
        var uilmDeleteItemIds = new List<string>();  // ✅ Track items to delete from UILM
        var uilmSaveItems = new List<UilmBulkOperationService.UilmKeyForBulkSave>();  // ✅ Track items to save to UILM
        
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
                var firstModuleId = firstModule["Id"].AsString;

                // Create new root module with consistent data
                var newRootModule = new BsonDocument
                {
                    { "Module", "app-root" },
                    { "Id", ObjectId.GenerateNewId().ToString() },
                    { "Resources", consistentResources }
                };

                var modulesToKeep = new BsonArray { newRootModule };

                // ✅ Track ALL modules for deletion from UILM (Filter A also deletes from UILM)
                foreach (var module in modules)
                {
                    if (module["Id"] != null)
                    {
                        uilmDeleteItemIds.Add(module["Id"].AsString);
                    }
                }

                // ✅ Prepare for UILM save (new root module)
                var uilmNewKey = ConvertToUilmBulkSaveFormat(
                    doc["KeyName"].AsString,
                    newRootModule,
                    doc
                );
                //uilmSaveItems.Add(uilmNewKey);

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
                    { "ConsistentDataExtractedFrom", firstModule["Module"] },
                    { "UilmDeleteAttempted", false },
                    { "UilmSaveAttempted", false }
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
                Console.WriteLine($"   └─ Deleted from UILM: {deletedModuleCount} old modules");
                Console.WriteLine($"   └─ Created in UILM: 1 new 'app-root' module");
            }

            // ✅ FILTER B: HasRoot=False, HasGeneric=True, IsConsistent=True
            else if (!hasRoot && hasGeneric && isConsistent)
            {
                var modulesToKeep = new BsonArray();
                var modulesToDelete = new BsonArray();
                var genericAppKept = false;
                BsonDocument keptGenericModule = null;

                foreach (var module in modules)
                {
                    var moduleName = module["Module"].AsString;

                    // Keep only ONE generic-app module
                    if (moduleName == "generic-app" && !genericAppKept)
                    {
                        modulesToKeep.Add(module);
                        genericAppKept = true;
                        keptGenericModule = module.AsBsonDocument;
                    }
                    else
                    {
                        modulesToDelete.Add(module);
                        // ✅ Track deleted item IDs for UILM deletion
                        if (module["Id"] != null)
                        {
                            uilmDeleteItemIds.Add(module["Id"].AsString);
                        }
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
                            .Count(m => m["Module"].AsString == "generic-app") },
                        { "UilmDeleteAttempted", false }
                    };

                    deletedRecords.Add(archivedRecord);
                }

                var updateDefinition = Builders<BsonDocument>.Update
                    .Set("Modules", modulesToKeep)
                    .Set("UpdatedAt", DateTime.UtcNow)
                    .Set("ModulesDeletionTriggered", true)
                    .Set("FilterType", "B");

                await collection.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]),
                    updateDefinition);

                Console.WriteLine($"✅ [FILTER B] Deleted {deletedModuleCount} modules for KeyName: {keyName}");
                Console.WriteLine($"   └─ Kept ONE 'generic-app' module only");
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
                        // ✅ Track deleted item IDs for UILM deletion
                        if (module["Id"] != null)
                        {
                            uilmDeleteItemIds.Add(module["Id"].AsString);
                        }
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
                        { "RootModuleKept", modulesToKeep.Count > 0 ? modulesToKeep[0] : null },
                        { "UilmDeleteAttempted", false }
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
                        // ✅ Track deleted item IDs for UILM deletion
                        if (module["Id"] != null)
                        {
                            uilmDeleteItemIds.Add(module["Id"].AsString);
                        }
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
                        { "RootModuleKept", modulesToKeep[0] },
                        { "UilmDeleteAttempted", false }
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

        // ✅ Execute UILM Operations
        await ExecuteUilmOperations(uilmDeleteItemIds, uilmSaveItems, deletedRecords, deletedModulesCollection);

        // ✅ Store migration results
        await StoreMigrationResults(migrationResultCollection, migrationResults);
    }

    private static async Task ExecuteUilmOperations(
        List<string> deleteItemIds,
        List<UilmBulkOperationService.UilmKeyForBulkSave> saveItems,
        List<BsonDocument> deletedRecords,
        IMongoCollection<BsonDocument> deletedModulesCollection)
    {
        Console.WriteLine("\n🚀 Starting UILM Operations...\n");

        // ✅ Process Delete Items
        if (deleteItemIds.Any())
        {
            Console.WriteLine($"📋 Checking {deleteItemIds.Count} items for existence in UILM...");
            var existingItems = new List<string>();

            foreach (var itemId in deleteItemIds)
            {
                var exists = await UilmBulkOperationService.ItemExistsAsync(itemId);
                if (exists)
                {
                    existingItems.Add(itemId);
                    Console.WriteLine($"   ✓ Item exists: {itemId}");
                }
                else
                {
                    Console.WriteLine($"   ✗ Item not found: {itemId}");
                }
            }

            if (existingItems.Any())
            {
                Console.WriteLine($"\n🗑️  Bulk deleting {existingItems.Count} items from UILM...");
                var (deleteSuccess, deleteMessage) = await UilmBulkOperationService.BulkDeleteAsync(existingItems);

                // Update archive records with delete status
                foreach (var deletedRecord in deletedRecords.Where(r => 
                    existingItems.Contains(r["DeletedModules"].AsBsonArray[0]["Id"].AsString)))
                {
                    var updateDef = Builders<BsonDocument>.Update
                        .Set("UilmDeleteAttempted", true)
                        .Set("UilmDeleteSuccess", deleteSuccess)
                        .Set("UilmDeleteMessage", deleteMessage);

                    await deletedModulesCollection.UpdateOneAsync(
                        Builders<BsonDocument>.Filter.Eq("_id", deletedRecord["_id"]),
                        updateDef);
                }
            }
        }

        // ✅ Process Save Items
        if (saveItems.Any())
        {
            Console.WriteLine($"\n💾 Bulk saving {saveItems.Count} new items to UILM...");
            var (saveSuccess, saveMessage) = await UilmBulkOperationService.BulkSaveAsync(saveItems);

            // Update archive records with save status
            foreach (var deletedRecord in deletedRecords.Where(r => r["FilterType"].AsString == "A"))
            {
                var updateDef = Builders<BsonDocument>.Update
                    .Set("UilmSaveAttempted", true)
                    .Set("UilmSaveSuccess", saveSuccess)
                    .Set("UilmSaveMessage", saveMessage);

                await deletedModulesCollection.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.Eq("_id", deletedRecord["_id"]),
                    updateDef);
            }
        }

        Console.WriteLine("\n✅ UILM Operations Completed\n");
    }

    private static UilmBulkOperationService.UilmKeyForBulkSave ConvertToUilmBulkSaveFormat(
        string keyName,
        BsonDocument module,
        BsonDocument sourceDoc)
    {
        const string AppRootModuleId = "5aa9386f-78cf-4084-bc20-76a38b627ea4";

        var resources = module["Resources"].AsBsonArray.Select(r =>
            new UilmBulkOperationService.ResourceItemForSave
            {
                Value = r["Value"].AsString ?? "",
                Culture = r["Culture"].AsString ?? "",
                CharacterLength = (r["Value"].AsString ?? "").Length
            }
        ).ToArray();

        return new UilmBulkOperationService.UilmKeyForBulkSave
        {
            ItemId = Guid.NewGuid().ToString(),
            KeyName = keyName ?? "UNKNOWN",
            ModuleId = AppRootModuleId,
            Resources = resources ?? Array.Empty<UilmBulkOperationService.ResourceItemForSave>(),
            Routes = null,
            IsPartiallyTranslated = resources?.All(r => !string.IsNullOrEmpty(r.Value)) ?? false,
            IsNewKey = true,
            LastUpdateDate = DateTime.UtcNow,
            CreateDate = DateTime.UtcNow,
            Context = "Auto-generated from duplicate consolidation",
            ShouldPublish = true,
            ProjectKey = "5350C966B6894A61B0913EB9FD5DC928"
        };
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
            //await migrationResultCollection.DeleteManyAsync(Builders<BsonDocument>.Filter.Empty);
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
}