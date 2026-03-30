using MongoDB.Bson;
using MongoDB.Driver;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Previewer;
using System.Text.RegularExpressions;

namespace ConsoleApp;

public static class InconsistentDuplicateCleaner
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

        // Local helper: partition modules into matched / unmatched sets using reference-module comparison
        static (List<BsonDocument> Matched, List<BsonDocument> Unmatched) GroupModulesByReference(BsonArray modules, string filterType)
        {
            var expectedCultures = new[] { "en-US", "de-DE", "fr-FR", "it-IT" };
            var moduleList = new List<(BsonDocument Doc, string ModuleName, int Occurrence, Dictionary<string, string> CultureValues)>();

            foreach (var module in modules)
            {
                var mDoc = module.AsBsonDocument;
                var moduleName = mDoc.Contains("Module") ? mDoc["Module"].AsString : "";
                var resources = mDoc.Contains("Resources") ? mDoc["Resources"].AsBsonArray : new BsonArray();
                var cultureValues = new Dictionary<string, string>();

                foreach (var res in resources)
                {
                    var r = res.AsBsonDocument;
                    var culture = r.Contains("Culture") ? r["Culture"].AsString : "";
                    var value = r.Contains("Value") ? r["Value"].AsString : "";
                    cultureValues[culture] = value;
                }

                var occurrence = moduleList.Count(x => x.ModuleName == moduleName) + 1;
                moduleList.Add((mDoc, moduleName, occurrence, cultureValues));
            }

            if (!moduleList.Any())
                return (new List<BsonDocument>(), new List<BsonDocument>());

            // ✅ FILTER-TYPE AWARE REFERENCE SELECTION
            string referenceModuleName = "";

            if (filterType == "B" || filterType == "D")
            {
                // ✅ FILTER B & D: Use generic-app as reference (because HasGenericModule = true)
                if (moduleList.Any(m => m.ModuleName == "generic-app"))
                {
                    referenceModuleName = "generic-app";
                }
                else
                {
                    var nonGenericModules = moduleList.Where(m => m.ModuleName != "generic-app").ToList();
                    referenceModuleName = nonGenericModules.Any() ? nonGenericModules.First().ModuleName : moduleList.First().ModuleName;
                }
            }
            else if (filterType == "C")
            {
                // ✅ FILTER C: Use app-root as reference (because HasRootModule = true, HasGenericModule = false)
                if (moduleList.Any(m => m.ModuleName == "app-root"))
                {
                    referenceModuleName = "app-root";
                }
                else
                {
                    var nonRootModules = moduleList.Where(m => m.ModuleName != "app-root").ToList();
                    referenceModuleName = nonRootModules.Any() ? nonRootModules.First().ModuleName : moduleList.First().ModuleName;
                }
            }
            else if (filterType == "A")
            {
                // ✅ FILTER A: No root, no generic - find most frequently occurring module as reference
                var groupedByName = moduleList.GroupBy(m => m.ModuleName)
                    .OrderByDescending(g => g.Count())
                    .FirstOrDefault();

                referenceModuleName = groupedByName?.Key ?? moduleList.First().ModuleName;
            }

            Console.WriteLine($"   📌 Reference module for Filter {filterType}: {referenceModuleName}");

            var reference = moduleList.First(m => m.ModuleName == referenceModuleName);
            var matched = new List<BsonDocument> { reference.Doc };
            var unmatched = new List<BsonDocument>();

            foreach (var m in moduleList.OrderBy(m => m.ModuleName).ThenBy(m => m.Occurrence))
            {
                if (m.ModuleName == reference.ModuleName && m.Occurrence == reference.Occurrence)
                    continue;

                bool isMatch = true;
                foreach (var culture in expectedCultures)
                {
                    var refVal = reference.CultureValues.ContainsKey(culture) ? NormalizeValue(reference.CultureValues[culture]) : "";
                    var modVal = m.CultureValues.ContainsKey(culture) ? NormalizeValue(m.CultureValues[culture]) : "";
                    if (refVal != modVal) { isMatch = false; break; }
                }

                if (isMatch) 
                    matched.Add(m.Doc);  // ✅ যদি match করে তাহলে matched এ যাবে
                else 
                    unmatched.Add(m.Doc);  // ✅ যদি match না করে তাহলে unmatched এ যাবে
            }

            return (matched, unmatched);
        }

        foreach (var doc in results)
        {
            bool hasRoot = doc.Contains("HasRootModule") && doc["HasRootModule"].AsBoolean;
            bool hasGeneric = doc.Contains("HasGenericModule") && doc["HasGenericModule"].AsBoolean;
            bool isConsistent = doc.Contains("IsConsistent") && doc["IsConsistent"].AsBoolean;
            string keyName = doc.Contains("KeyName") ? doc["KeyName"].AsString : "";

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

            var modules = doc.Contains("Modules") ? doc["Modules"].AsBsonArray : new BsonArray();
            int previousModuleCount = modules.Count;
            int deletedModuleCount = 0;

            // ✅ Skip consistent keys - only process inconsistent ones
            if (isConsistent)
            {
                continue;
            }

            // ----- isConsistent == false (ONLY PROCESS THIS) -----
            var (matchedGroup, unmatchedGroup) = GroupModulesByReference(modules, filterType);

            var finalModules = new BsonArray();
            var groupArchives = new List<BsonDocument>();
            var uilmDeleteIds = new List<string>();
            var uilmDeleteItemKeyMap = new Dictionary<string, string>();
            var uilmInsertItems = new List<UilmBulkOperationService.UilmKeyForBulkSave>();

            string refModuleName = (filterType == "B" || filterType == "D") ? "generic-app" 
                                 : (filterType == "C") ? "app-root" 
                                 : "";  // Filter A: will be created

            // ✅ Helper to process matched group
            void ProcessMatchedGroup(List<BsonDocument> groupModules)
            {
                if (groupModules == null || groupModules.Count == 0)
                    return;

                if (filterType == "C")
                {

                    int matchedCount = groupModules.Count;
                    int unmatchedCount = unmatchedGroup.Count;
                    

                    if (unmatchedCount > matchedCount)
                    {

                        Console.WriteLine($"   ℹ️  [FILTER C] Unmatched count ({unmatchedCount}) > Matched count ({matchedCount}). Processing unmatched group...");
                        

                        var unmatchedValueGroups = GetValueGroupsFromModules(unmatchedGroup);
                        var mostCommonUnmatchedValue = unmatchedValueGroups
                            .OrderByDescending(g => g.Count)
                            .FirstOrDefault();
                        
                        var otherUnmatchedValues = unmatchedValueGroups
                            .OrderByDescending(g => g.Count)
                            .Skip(1)
                            .SelectMany(g => g)
                            .ToList();

                        int mostCommonUnmatchedCount = mostCommonUnmatchedValue?.Count ?? 0;
                        
                        if (mostCommonUnmatchedCount > matchedCount)
                        {
                            // ✅ CASE A: mostCommonUnmatchedCount > matchedCount
                            Console.WriteLine($"   ℹ️  [FILTER C] Most common unmatched ({mostCommonUnmatchedCount}) > Matched ({matchedCount}). Deleting most common + keeping matched + other unmatched...");
                            
                            DeleteModulesFromGroup(mostCommonUnmatchedValue, "Unmatched (most common)");
                            
                            foreach (var m in otherUnmatchedValues)
                                finalModules.Add(m);
                            
                            foreach (var m in groupModules)
                                finalModules.Add(m);
                            
                            CreateAndKeepGenericApp(mostCommonUnmatchedValue, "Unmatched (most common)");
                        }
                        else if (mostCommonUnmatchedCount == matchedCount)
                        {
                            // ✅ CASE B: mostCommonUnmatchedCount == matchedCount
                            Console.WriteLine($"   ℹ️  [FILTER C] Most common unmatched ({mostCommonUnmatchedCount}) == Matched ({matchedCount}). Deleting most common + keeping matched + other unmatched...");
                            
                            DeleteModulesFromGroup(mostCommonUnmatchedValue, "Unmatched (most common)");
                            
                            foreach (var m in otherUnmatchedValues)
                                finalModules.Add(m);
                            
                            foreach (var m in groupModules)
                                finalModules.Add(m);
                            
                            CreateAndKeepGenericApp(mostCommonUnmatchedValue, "Unmatched (most common)");
                        }
                        else
                        {
                            // ✅ CASE C: mostCommonUnmatchedCount < matchedCount
                            Console.WriteLine($"   ℹ️  [FILTER C] Most common unmatched ({mostCommonUnmatchedCount}) < Matched ({matchedCount}). Deleting matched + keeping all unmatched...");
                            
                            DeleteModulesFromGroup(groupModules, "Matched");
                            
                            foreach (var m in unmatchedGroup)
                                finalModules.Add(m);
                        
                            CreateAndKeepGenericApp(groupModules, "Matched");
                        }
                    }
                    else
                    {
                        if (matchedCount == 1)
                            return;
                        // ✅ Matched count >= Unmatched count
                        Console.WriteLine($"   ℹ️  [FILTER C] Matched count ({matchedCount}) >= Unmatched count ({unmatchedCount}). Deleting matched + keeping all unmatched...");
                        

                        // ✅ Delete matched group (NOT unmatchedGroup)
                        DeleteModulesFromGroup(groupModules, "Matched");
                        
                        // ✅ Keep all unmatched
                        foreach (var m in unmatchedGroup)
                            finalModules.Add(m);
                    
                        // ✅ Create generic-app from matched group
                        CreateAndKeepGenericApp(groupModules, "Matched");
                    }
                }
                else
                {
                    // ✅ FOR FILTER B & D: Original logic
                    var existingReference = groupModules.FirstOrDefault(m => 
                        m.Contains("Module") && m["Module"].AsString == refModuleName);

                    BsonDocument referenceToKeep;
                    string referenceIdForUilm = null;

                    if (existingReference != null)
                    {
                        referenceToKeep = existingReference;
                        referenceIdForUilm = existingReference.Contains("Id") ? existingReference["Id"].AsString : null;
                        
                        // ✅ FOR B, D: Delete duplicate reference modules
                        foreach (var m in groupModules)
                        {
                            if (m.Contains("Module") && m["Module"].AsString == refModuleName)
                            {
                                if (ReferenceEquals(m, existingReference))
                                    continue;

                                if (m.Contains("Id"))
                                {
                                    string deleteId = m["Id"].AsString;
                                    uilmDeleteIds.Add(deleteId);
                                    uilmDeleteItemKeyMap[deleteId] = keyName;
                                    Console.WriteLine($"      🗑️  Queued duplicate {refModuleName} delete: {deleteId}");
                                }
                            }
                        }
                    }
                    else
                    {
                        // ✅ FOR FILTER A: Create new generic-app
                        if (filterType == "A")
                        {
                            var firstMod = groupModules[0];
                            var resources = firstMod.Contains("Resources") ? firstMod["Resources"].AsBsonArray : new BsonArray();
                            referenceIdForUilm = ObjectId.GenerateNewId().ToString();

                            referenceToKeep = new BsonDocument
                            {
                                { "Module", "generic-app" },
                                { "Id", referenceIdForUilm },
                                { "Resources", resources }
                            };

                            var uilmItem = new UilmBulkOperationService.UilmKeyForBulkSave
                            {
                                ItemId = referenceIdForUilm,
                                KeyName = keyName,
                                ModuleId = "f43680b9-df15-4c01-998d-eac8fac79d25",
                                Resources = ConvertResourcesToUilmFormat(resources),
                                Routes = null,
                                IsPartiallyTranslated = false,
                                IsNewKey = true,
                                LastUpdateDate = DateTime.UtcNow,
                                CreateDate = DateTime.UtcNow,
                                Context = $"Auto-generated from inconsistent cleanup (Filter A)",
                                ShouldPublish = true,
                                ProjectKey = "5350C966B6894A61B0913EB9FD5DC928"
                            };

                            uilmInsertItems.Add(uilmItem);
                            Console.WriteLine($"   📝 [FILTER A] Prepared UILM insert: {keyName} (Module: generic-app)");
                        }
                        else
                        {
                            Console.WriteLine($"   ⚠️  [FILTER {filterType}] No {refModuleName} found in matched group!");
                            return;
                        }
                    }

                    // ✅ Delete all non-reference modules
                    foreach (var m in groupModules)
                    {
                        if (m.Contains("Module") && m["Module"].AsString == refModuleName && ReferenceEquals(m, existingReference))
                            continue;

                        if (m.Contains("Id") && referenceToKeep.Contains("Id") && m["Id"].AsString == referenceToKeep["Id"].AsString)
                            continue;

                        if (m.Contains("Id"))
                        {
                            string deleteId = m["Id"].AsString;
                            if (!uilmDeleteIds.Contains(deleteId))
                            {
                                uilmDeleteIds.Add(deleteId);
                                uilmDeleteItemKeyMap[deleteId] = keyName;
                            }
                        }

                        deletedModuleCount++;
                    }

                    finalModules.Add(referenceToKeep);
                }
            }

            // ✅ Helper: Check if all modules in group have same normalized values
            bool CheckIfAllValuesSame(List<BsonDocument> groupModules)
            {
                var expectedCultures = new[] { "en-US", "de-DE", "fr-FR", "it-IT" };
                if (groupModules.Count < 2) return true;

                var firstModValues = ExtractCultureValues(groupModules[0]);
                
                foreach (var m in groupModules.Skip(1))
                {
                    var moduleValues = ExtractCultureValues(m);
                    foreach (var culture in expectedCultures)
                    {
                        var firstNorm = NormalizeValue(firstModValues.ContainsKey(culture) ? firstModValues[culture] : "");
                        var moduleNorm = NormalizeValue(moduleValues.ContainsKey(culture) ? moduleValues[culture] : "");
                        if (firstNorm != moduleNorm)
                            return false;
                    }
                }

                return true;
            }

            // ✅ Helper: Get value groups from modules
            List<List<BsonDocument>> GetValueGroupsFromModules(List<BsonDocument> groupModules)
            {
                var expectedCultures = new[] { "en-US", "de-DE", "fr-FR", "it-IT" };
                var valueGroups = new Dictionary<string, List<BsonDocument>>();

                foreach (var m in groupModules)
                {
                    var moduleValues = ExtractCultureValues(m);
                    string valueKey = string.Join("|", expectedCultures.Select(c =>
                        NormalizeValue(moduleValues.ContainsKey(c) ? moduleValues[c] : "")));

                    if (!valueGroups.ContainsKey(valueKey))
                        valueGroups[valueKey] = new List<BsonDocument>();

                    valueGroups[valueKey].Add(m);
                }

                return valueGroups.Values.ToList();
            }

            // ✅ Helper: Extract culture values from module
            Dictionary<string, string> ExtractCultureValues(BsonDocument module)
            {
                var cultureValues = new Dictionary<string, string>();
                var resources = module.Contains("Resources") ? module["Resources"].AsBsonArray : new BsonArray();

                foreach (var res in resources)
                {
                    var r = res.AsBsonDocument;
                    var culture = r.Contains("Culture") ? r["Culture"].AsString : "";
                    var value = r.Contains("Value") ? r["Value"].AsString : "";
                    cultureValues[culture] = value;
                }

                return cultureValues;
            }

            // ✅ Helper: Delete modules from group
            void DeleteModulesFromGroup(List<BsonDocument> groupModules, string groupLabel)
            {
                foreach (var m in groupModules)
                {
                    if (m.Contains("Id"))
                    {
                        string deleteId = m["Id"].AsString;
                        if (!uilmDeleteIds.Contains(deleteId))
                        {
                            uilmDeleteIds.Add(deleteId);
                            uilmDeleteItemKeyMap[deleteId] = keyName;
                            string moduleName = m.Contains("Module") ? m["Module"].AsString : "Unknown";
                            Console.WriteLine($"      🗑️  Queued delete ({groupLabel}): {deleteId} ({moduleName})");
                        }
                    }
                    deletedModuleCount++;
                }
            }

            // ✅ Helper: Create and keep generic-app from best group
            void CreateAndKeepGenericApp(List<BsonDocument> sourceGroup, string sourceLabel)
            {
                if (sourceGroup == null || sourceGroup.Count == 0) return;

                var firstMod = sourceGroup[0];
                var resources = firstMod.Contains("Resources") ? firstMod["Resources"].AsBsonArray : new BsonArray();
                string genericIdForUilm = ObjectId.GenerateNewId().ToString();

                var genericModule = new BsonDocument
                {
                    { "Module", "generic-app" },
                    { "Id", genericIdForUilm },
                    { "Resources", resources }
                };

                var uilmItem = new UilmBulkOperationService.UilmKeyForBulkSave
                {
                    ItemId = genericIdForUilm,
                    KeyName = keyName,
                    ModuleId = "f43680b9-df15-4c01-998d-eac8fac79d25",
                    Resources = ConvertResourcesToUilmFormat(resources),
                    Routes = null,
                    IsPartiallyTranslated = false,
                    IsNewKey = true,
                    LastUpdateDate = DateTime.UtcNow,
                    CreateDate = DateTime.UtcNow,
                    Context = $"Auto-generated from inconsistent cleanup (Filter C - {sourceLabel})",
                    ShouldPublish = true,
                    ProjectKey = "5350C966B6894A61B0913EB9FD5DC928"
                };

                uilmInsertItems.Add(uilmItem);
                finalModules.Add(genericModule);
                Console.WriteLine($"   📝 [FILTER C] Created generic-app from {sourceLabel} group");
            }

            // ✅ Helper to process unmatched group (Keep everything, no delete/insert)
            void ProcessUnmatchedGroup(List<BsonDocument> groupModules)
            {
                if (groupModules == null || groupModules.Count == 0)
                    return;

                // ✅ For unmatched group: Keep ALL modules, delete nothing, insert nothing
                foreach (var m in groupModules)
                {
                    finalModules.Add(m);
                }

                // Archive unmatched group (no changes)
                var archived = new BsonDocument
                {
                    { "_id", ObjectId.GenerateNewId() },
                    { "KeyName", keyName },
                    { "FilterType", filterType },
                    { "Group", "Unmatched" },
                    { "DeletedAt", DateTime.UtcNow },
                    { "HasRootModule", hasRoot },
                    { "HasGenericModule", hasGeneric },
                    { "IsConsistent", isConsistent },
                    { "DeletedModuleCount", 0 },
                    { "ModulesKeptIntact", groupModules.Count },
                    { "Note", "Unmatched modules kept as-is. No deletion or insertion." },
                    { "UilmDeleteAttempted", false },
                    { "UilmInsertAttempted", false }
                };

                groupArchives.Add(archived);
                
                Console.WriteLine($"   ℹ️  [FILTER {filterType}] Unmatched group: Keeping all {groupModules.Count} modules as-is.");
            }

            // ✅ Process groups based on filter type
            if (filterType == "A")
            {
                await FilterAProcessor.ProcessFilterA(
                    doc,
                    collection,
                    deletedModulesCollection,
                    uilmDeleteIds,
                    uilmDeleteItemKeyMap,
                    uilmInsertItems,
                    keyName,
                    previousModuleCount,
                    deletedModuleCount);
            }
            else if (filterType == "B" || filterType == "D")
            {
                ProcessMatchedGroup(matchedGroup);
            }
            else if (filterType == "C")
            {
                ProcessMatchedGroup(matchedGroup);
            }
            
            // ✅ Save archives for both groups
            foreach (var ar in groupArchives)
                deletedRecords.Add(ar);

            // ✅ Execute UILM operations for inconsistent keys
            Console.WriteLine($"\n🔄 Executing UILM operations for Filter {filterType} - KeyName '{keyName}'...");

            // Step 1: Delete old modules from UILM (B, C, D: duplicates + non-refs | A: all from matched)
            if (uilmDeleteIds.Any())
            {
                Console.WriteLine($"🗑️  Deleting {uilmDeleteIds.Count} modules from UILM...");
                var (deleteSuccess, deleteMsg) = await UilmBulkOperationService.BulkDeleteAsync(uilmDeleteIds, uilmDeleteItemKeyMap);
                Console.WriteLine($"   {(deleteSuccess ? "✅" : "❌")} UILM Delete: {deleteMsg}");

                // ✅ Remove deleted items from Duplicate_Key_Report
                if (deleteSuccess)
                {
                    var deleteIdsHashSet = new HashSet<string>(uilmDeleteIds);
                    var removeFromReportDef = Builders<BsonDocument>.Update
                        .PullFilter("Modules", 
                            Builders<BsonDocument>.Filter.In("Id", deleteIdsHashSet));
                    
                    await collection.UpdateOneAsync(
                        Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]),
                        removeFromReportDef);
                    
                    Console.WriteLine($"   ✅ Removed {uilmDeleteIds.Count} modules from Duplicate_Key_Report");
                }

                // Update archives with delete status
                foreach (var ar in groupArchives)
                {
                    var updateArchive = Builders<BsonDocument>.Update
                        .Set("UilmDeleteAttempted", true)
                        .Set("UilmDeleteSuccess", deleteSuccess)
                        .Set("UilmDeleteMessage", deleteMsg);

                    await deletedModulesCollection.UpdateOneAsync(
                        Builders<BsonDocument>.Filter.Eq("_id", ar["_id"]),
                        updateArchive);
                }
            }

            // Step 2: Insert new modules to UILM (ONLY for Filter A, B, C, D with generic-app creation)
            if (uilmInsertItems.Any())
            {
                Console.WriteLine($"📝 Inserting {uilmInsertItems.Count} new generic-app module(s) to UILM...");
                var (insertSuccess, insertMsg) = await UilmBulkOperationService.BulkSaveAsync(uilmInsertItems);
                Console.WriteLine($"   {(insertSuccess ? "✅" : "❌")} UILM Insert: {insertMsg}");

                // ✅ Add new modules to Duplicate_Key_Report (ONLY if insert successful)
                if (insertSuccess)
                {
                    var newModulesToAdd = new List<BsonDocument>();
                    foreach (var uilmItem in uilmInsertItems)
                    {
                        var newModule = new BsonDocument
                        {
                            { "Module", "generic-app" },
                            { "Id", uilmItem.ItemId },
                            { "Resources", new BsonArray(
                                uilmItem.Resources.Select(r => new BsonDocument
                                {
                                    { "Culture", r.Culture },
                                    { "Value", r.Value },
                                    { "CharacterLength", r.CharacterLength }
                                })
                            )}
                        };
                        newModulesToAdd.Add(newModule);
                    }

                    var addToReportDef = Builders<BsonDocument>.Update
                        .PushEach("Modules", newModulesToAdd);
                    
                    await collection.UpdateOneAsync(
                        Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]),
                        addToReportDef);
                    
                    Console.WriteLine($"   ✅ Added {uilmInsertItems.Count} new module(s) to Duplicate_Key_Report");
                }

                // Update archives with insert status
                foreach (var ar in groupArchives)
                {
                    var updateArchive = Builders<BsonDocument>.Update
                        .Set("UilmInsertAttempted", true)
                        .Set("UilmInsertSuccess", insertSuccess)
                        .Set("UilmInsertMessage", insertMsg);

                    await deletedModulesCollection.UpdateOneAsync(
                        Builders<BsonDocument>.Filter.Eq("_id", ar["_id"]),
                        updateArchive);
                }
            }

            Console.WriteLine($"✅ [FILTER {filterType}] KeyName '{keyName}' - Processed: Deleted {deletedModuleCount}, Final modules: {finalModules.Count}");

            // ✅ Track migration results
            if (!string.IsNullOrEmpty(filterType) && migrationResults.ContainsKey(filterType))
            {
                migrationResults[filterType].Keys.Add(new KeyMigrationDetail
                {
                    Name = keyName,
                    PreviousModules = previousModuleCount,
                    DeletedModules = deletedModuleCount
                });
            }

            ProcessUnmatchedGroup(unmatchedGroup);
        }

        // Insert archived records
        if (deletedRecords.Any())
        {
            await deletedModulesCollection.InsertManyAsync(deletedRecords);
            Console.WriteLine($"\n✅ Archived {deletedRecords.Count} deletion records to 'Deleted_Modules_Archive'");

            var filterACount = deletedRecords.Count(r => r.Contains("FilterType") && r["FilterType"].AsString == "A");
            var filterBCount = deletedRecords.Count(r => r.Contains("FilterType") && r["FilterType"].AsString == "B");
            var filterCCount = deletedRecords.Count(r => r.Contains("FilterType") && r["FilterType"].AsString == "C");
            var filterDCount = deletedRecords.Count(r => r.Contains("FilterType") && r["FilterType"].AsString == "D");

            Console.WriteLine("\n📊 Deletion Summary:");
            if (filterACount > 0) Console.WriteLine($"   • Filter A: {filterACount} records");
            if (filterBCount > 0) Console.WriteLine($"   • Filter B: {filterBCount} records");
            if (filterCCount > 0) Console.WriteLine($"   • Filter C: {filterCCount} records");
            if (filterDCount > 0) Console.WriteLine($"   • Filter D: {filterDCount} records");
        }

        // Store migration results
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

    // New helper: compare modules to a reference module (prefer app-root, then generic-app, then first)
    private static (int Matching, int NonMatching, List<string> MatchedModules, List<string> UnmatchedModules) GetReferenceMatchCounts(BsonDocument doc)
    {
        var expectedCultures = new[] { "en-US", "de-DE", "fr-FR", "it-IT" };
        var moduleList = new List<(string ModuleName, int Occurrence, Dictionary<string, string> CultureValues)>();
        var modules = doc["Modules"].AsBsonArray;

        foreach (var module in modules)
        {
            var mDoc = module.AsBsonDocument;
            var moduleName = mDoc.GetValue("Module").AsString;
            var resources = mDoc.GetValue("Resources").AsBsonArray;
            var cultureValues = new Dictionary<string, string>();

            foreach (var res in resources)
            {
                var r = res.AsBsonDocument;
                cultureValues[r.GetValue("Culture").AsString] = r.GetValue("Value").AsString;
            }

            var occurrence = moduleList.Count(x => x.ModuleName == moduleName) + 1;
            moduleList.Add((moduleName, occurrence, cultureValues));
        }

        if (!moduleList.Any())
            return (0, 0, new List<string>(), new List<string>());

        // Prefer app-root first, then generic-app, then any other module
        string referenceModuleName = moduleList.Any(m => m.ModuleName == "app-root")
            ? "app-root"
            : moduleList.Any(m => m.ModuleName == "generic-app")
                ? "generic-app"
                : moduleList.First().ModuleName;

        var reference = moduleList.First(m => m.ModuleName == referenceModuleName);
        var matched = new List<string> { $"{reference.ModuleName} (Occurrence {reference.Occurrence})" };
        var unmatched = new List<string>();

        foreach (var m in moduleList.OrderBy(m => m.ModuleName).ThenBy(m => m.Occurrence))
        {
            if (m.ModuleName == reference.ModuleName && m.Occurrence == reference.Occurrence)
                continue;

            bool isMatch = true;
            foreach (var culture in expectedCultures)
            {
                var refVal = reference.CultureValues.ContainsKey(culture) ? NormalizeValue(reference.CultureValues[culture]) : "";
                var modVal = m.CultureValues.ContainsKey(culture) ? NormalizeValue(m.CultureValues[culture]) : "";
                if (refVal != modVal) { isMatch = false; break; }
            }

            var displayName = $"{m.ModuleName} (Occurrence {m.Occurrence})";
            if (isMatch) matched.Add(displayName); else unmatched.Add(displayName);
        }

        return (matched.Count, unmatched.Count, matched, unmatched);
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
                            columns.RelativeColumn(1.2f);
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
                            header.Cell().Background("#FF6B6B").Text("Matching").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#FF6B6B").Text("Non-Matching").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#FF6B6B").Text("Deleted").FontSize(7).Bold().FontColor(Colors.White);
                        });

                        var rows = new[] { ("A", false, false), ("B", false, true), ("C", true, false), ("D", true, true) };
                        int grandTotalKeys = 0, grandTotalModules = 0, grandKeysTrue = 0, grandModulesTrue = 0, grandKeysFalse = 0, grandModulesFalse = 0, grandDeleted = 0;
                        int grandMatching = 0, grandNonMatching = 0;

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

                            // Calculate matching/non-matching for this filter
                            int filterMatching = 0, filterNonMatching = 0;
                            foreach (var doc in results)
                            {
                                bool root = doc["HasRootModule"].AsBoolean;
                                bool generic = doc["HasGenericModule"].AsBoolean;
                                string docFilterKey = root switch
                                {
                                    false when !generic => "A",
                                    false when generic => "B",
                                    true when !hasGeneric => "C",
                                    true when hasGeneric => "D",
                                    _ => ""
                                };

                                if (docFilterKey == letter)
                                {
                                    var (matching, nonMatching, _, _) = GetReferenceMatchCounts(doc);
                                    filterMatching += matching;
                                    filterNonMatching += nonMatching;
                                }
                            }

                            grandMatching += filterMatching;
                            grandNonMatching += filterNonMatching;

                            table.Cell().Background("#E7E6E6").Text(letter).FontSize(7).Bold();
                            table.Cell().Background("#E7E6E6").Text(hasRoot.ToString()).FontSize(7);
                            table.Cell().Background("#E7E6E6").Text(hasGeneric.ToString()).FontSize(7);
                            table.Cell().Background("#E7E6E6").Text(stats.TotalDuplicateKeys.ToString()).FontSize(7).Bold();
                            table.Cell().Background("#E7E6E6").Text(stats.TotalDuplicateModules.ToString()).FontSize(7).Bold();
                            table.Cell().Background("#E7E6E6").Text(stats.DuplicateKeyForSameLanguageTrue.ToString()).FontSize(7);
                            table.Cell().Background("#E7E6E6").Text(stats.DuplicateModuleForSameLanguageTrue.ToString()).FontSize(7);
                            table.Cell().Background("#E7E6E6").Text(stats.DuplicateKeyForSameLanguageFalse.ToString()).FontSize(7);
                            table.Cell().Background("#E7E6E6").Text(stats.DuplicateModuleForSameLanguageFalse.ToString()).FontSize(7);
                            table.Cell().Background("#FFE0E0").Text(filterMatching.ToString()).FontSize(7).Bold();
                            table.Cell().Background("#FFE0E0").Text(filterNonMatching.ToString()).FontSize(7).Bold();
                            table.Cell().Background("#FFD3D3").Text(stats.DeletedModules.ToString()).FontSize(7).Bold();
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
                        table.Cell().Background("#FFD3D3").Text(grandMatching.ToString()).FontSize(7).Bold();
                        table.Cell().Background("#FFD3D3").Text(grandNonMatching.ToString()).FontSize(7).Bold();
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
                            var (matchingCount, nonMatchingCount, matchedList, unmatchedList) = GetReferenceMatchCounts(doc);

                            col.Item().PaddingTop(5).Text("✗ Inconsistent translations detected")
                                .FontSize(9).Bold().FontColor("#FF0000");

                            col.Item().PaddingTop(6).Text($"Inconsistent Modules — Matching: {matchingCount} | Non-Matching: {nonMatchingCount}")
                                .FontSize(9).Bold();

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

                            col.Item().PaddingTop(8).Text($"Matched Modules: {string.Join(", ", matchedList)}")
                                .FontSize(8);
                            col.Item().Text($"Unmatched Modules: {string.Join(", ", unmatchedList)}")
                                .FontSize(8);
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

    private static UilmBulkOperationService.ResourceItemForSave[] ConvertResourcesToUilmFormat(BsonArray resources)
    {
        var result = new List<UilmBulkOperationService.ResourceItemForSave>();

        foreach (var res in resources)
        {
            var r = res.AsBsonDocument;
            result.Add(new UilmBulkOperationService.ResourceItemForSave
            {
                Value = r.Contains("Value") ? r["Value"].AsString ?? "" : "",
                Culture = r.Contains("Culture") ? r["Culture"].AsString ?? "" : "",
                CharacterLength = r.Contains("Value") ? (r["Value"].AsString ?? "").Length : 0
            });
        }

        return result.ToArray();
    }
}