using MongoDB.Bson;
using MongoDB.Driver;
using System.Text.RegularExpressions;

namespace ConsoleApp;

public static class FilterAProcessor
{
    /// <summary>
    /// Filter A Processing Logic
    /// HasRootModule: false
    /// HasGenericModule: false
    /// IsConsistent: false (inconsistent keys only)
    /// 
    /// Strategy:
    /// 1. Use FIRST module as reference (since no app-root or generic-app)
    /// 2. Group modules: Matched (same as first) vs Unmatched (different)
    /// 3. Compare counts and decide deletion strategy
    /// 4. Create new app-root from best group
    /// </summary>
    public static async Task ProcessFilterA(
        BsonDocument doc,
        IMongoCollection<BsonDocument> collection,
        IMongoCollection<BsonDocument> deletedModulesCollection,
        List<string> uilmDeleteIds,
        Dictionary<string, string> uilmDeleteItemKeyMap,
        List<UilmBulkOperationService.UilmKeyForBulkSave> uilmInsertItems,
        string keyName,
        int previousModuleCount,
        int deletedModuleCount)
    {
        var modules = doc.Contains("Modules") ? doc["Modules"].AsBsonArray : new BsonArray();
        
        if (modules.Count == 0)
        {
            Console.WriteLine($"   ⚠️  [FILTER A] No modules found for key '{keyName}'");
            return;
        }

        // Step 1: Use first module as reference
        var (matchedGroup, unmatchedGroup) = GroupModulesByFirstReference(modules);

        Console.WriteLine($"   📌 [FILTER A] Reference module (First): {GetModuleName(matchedGroup[0])}");
        Console.WriteLine($"   ℹ️  [FILTER A] Matched: {matchedGroup.Count}, Unmatched: {unmatchedGroup.Count}");

        // Step 2: Compare counts and decide strategy
        int matchedCount = matchedGroup.Count;
        int unmatchedCount = unmatchedGroup.Count;

        var finalModules = new BsonArray();

        if (unmatchedCount > matchedCount)
        {
            // ✅ Branch A: Unmatched count is greater
            Console.WriteLine($"   ℹ️  [FILTER A] Unmatched count ({unmatchedCount}) > Matched count ({matchedCount}). Processing unmatched group...");

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
                // ✅ CASE A.1: mostCommon > matched
                Console.WriteLine($"   ℹ️  [FILTER A] Most common unmatched ({mostCommonUnmatchedCount}) > Matched ({matchedCount}). Deleting most common + keeping matched + other unmatched...");

                DeleteModulesFromGroup(mostCommonUnmatchedValue, "Unmatched (most common)", uilmDeleteIds, uilmDeleteItemKeyMap, keyName, ref deletedModuleCount);

                foreach (var m in otherUnmatchedValues)
                    finalModules.Add(m);

                foreach (var m in matchedGroup)
                    finalModules.Add(m);

                CreateAndKeepGenericApp(mostCommonUnmatchedValue, "Unmatched (most common)", keyName, uilmInsertItems, finalModules);
            }
            else if (mostCommonUnmatchedCount == matchedCount)
            {
                if (mostCommonUnmatchedCount == 1)
                {
                    Console.WriteLine($"   ⏭️  [FILTER A] Only reference module exists (matched count = 1). Skipping processing...");
                    return;
                }
                // ✅ CASE A.2: mostCommon == matched
                Console.WriteLine($"   ℹ️  [FILTER A] Most common unmatched ({mostCommonUnmatchedCount}) == Matched ({matchedCount}). Deleting most common + keeping matched + other unmatched...");

                DeleteModulesFromGroup(mostCommonUnmatchedValue, "Unmatched (most common)", uilmDeleteIds, uilmDeleteItemKeyMap, keyName, ref deletedModuleCount);

                foreach (var m in otherUnmatchedValues)
                    finalModules.Add(m);

                foreach (var m in matchedGroup)
                    finalModules.Add(m);

                CreateAndKeepGenericApp(mostCommonUnmatchedValue, "Unmatched (most common)", keyName, uilmInsertItems, finalModules);
            }
            else
            {
                // ✅ CASE A.3: mostCommon < matched
                Console.WriteLine($"   ℹ️  [FILTER A] Most common unmatched ({mostCommonUnmatchedCount}) < Matched ({matchedCount}). Deleting matched + keeping all unmatched...");

                DeleteModulesFromGroup(matchedGroup, "Matched", uilmDeleteIds, uilmDeleteItemKeyMap, keyName, ref deletedModuleCount);

                foreach (var m in unmatchedGroup)
                    finalModules.Add(m);

                CreateAndKeepGenericApp(matchedGroup, "Matched", keyName, uilmInsertItems, finalModules);
            }
        }
        else
        {
            // ✅ Branch B: Matched count >= Unmatched count
            if (matchedCount == 1)
            {
                Console.WriteLine($"   ⏭️  [FILTER A] Only reference module exists (matched count = 1). Skipping processing...");
                return;
            }

            Console.WriteLine($"   ℹ️  [FILTER A] Matched count ({matchedCount}) >= Unmatched count ({unmatchedCount}). Deleting matched + keeping all unmatched...");

            DeleteModulesFromGroup(matchedGroup, "Matched", uilmDeleteIds, uilmDeleteItemKeyMap, keyName, ref deletedModuleCount);

            foreach (var m in unmatchedGroup)
                finalModules.Add(m);

            CreateAndKeepGenericApp(matchedGroup, "Matched", keyName, uilmInsertItems, finalModules);
        }

    }

    /// <summary>
    /// Group modules by first module as reference
    /// </summary>
    private static (List<BsonDocument> Matched, List<BsonDocument> Unmatched) GroupModulesByFirstReference(BsonArray modules)
    {
        var expectedCultures = new[] { "en-US", "de-DE", "fr-FR", "it-IT" };
        var matched = new List<BsonDocument>();
        var unmatched = new List<BsonDocument>();

        if (modules.Count == 0)
            return (matched, unmatched);

        // Use first module as reference
        var firstModuleDoc = modules[0].AsBsonDocument;
        var firstModuleName = GetModuleName(firstModuleDoc);
        var firstModuleValues = ExtractCultureValues(firstModuleDoc);

        matched.Add(firstModuleDoc);

        // Compare rest with first module
        for (int i = 1; i < modules.Count; i++)
        {
            var moduleDoc = modules[i].AsBsonDocument;
            var moduleValues = ExtractCultureValues(moduleDoc);

            bool isMatch = true;
            foreach (var culture in expectedCultures)
            {
                var firstVal = firstModuleValues.ContainsKey(culture) ? NormalizeValue(firstModuleValues[culture]) : "";
                var modVal = moduleValues.ContainsKey(culture) ? NormalizeValue(moduleValues[culture]) : "";
                if (firstVal != modVal)
                {
                    isMatch = false;
                    break;
                }
            }

            if (isMatch)
                matched.Add(moduleDoc);
            else
                unmatched.Add(moduleDoc);
        }

        return (matched, unmatched);
    }

    /// <summary>
    /// Get value groups from modules
    /// </summary>
    private static List<List<BsonDocument>> GetValueGroupsFromModules(List<BsonDocument> groupModules)
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

    /// <summary>
    /// Extract culture values from module
    /// </summary>
    private static Dictionary<string, string> ExtractCultureValues(BsonDocument module)
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

    /// <summary>
    /// Get module name from BsonDocument
    /// </summary>
    private static string GetModuleName(BsonDocument module)
    {
        return module.Contains("Module") ? module["Module"].AsString : "Unknown";
    }

    /// <summary>
    /// Delete modules from group
    /// </summary>
    private static void DeleteModulesFromGroup(
        List<BsonDocument> groupModules,
        string groupLabel,
        List<string> uilmDeleteIds,
        Dictionary<string, string> uilmDeleteItemKeyMap,
        string keyName,
        ref int deletedModuleCount)
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
                    string moduleName = GetModuleName(m);
                    Console.WriteLine($"      🗑️  Queued delete ({groupLabel}): {deleteId} ({moduleName})");
                }
            }
            deletedModuleCount++;
        }
    }

    /// <summary>
    /// Create new app-root module and add to final modules
    /// </summary>
    private static void CreateAndKeepGenericApp(
        List<BsonDocument> sourceGroup,
        string sourceLabel,
        string keyName,
        List<UilmBulkOperationService.UilmKeyForBulkSave> uilmInsertItems,
        BsonArray finalModules)
    {
        if (sourceGroup == null || sourceGroup.Count == 0)
            return;

        var firstMod = sourceGroup[0];
        var resources = firstMod.Contains("Resources") ? firstMod["Resources"].AsBsonArray : new BsonArray();
        string genericAppIdForUilm = ObjectId.GenerateNewId().ToString();

        // ✅ Create generic-app (NOT app-root)
        var genericAppModule = new BsonDocument
        {
            { "Module", "generic-app" },
            { "Id", genericAppIdForUilm },
            { "Resources", resources }
        };

        var uilmItem = new UilmBulkOperationService.UilmKeyForBulkSave
        {
            ItemId = genericAppIdForUilm,
            KeyName = keyName,
            ModuleId = "f43680b9-df15-4c01-998d-eac8fac79d25",
            Resources = ConvertResourcesToUilmFormat(resources),
            Routes = null,
            IsPartiallyTranslated = false,
            IsNewKey = true,
            LastUpdateDate = DateTime.UtcNow,
            CreateDate = DateTime.UtcNow,
            Context = $"Auto-generated from inconsistent cleanup (Filter A - {sourceLabel})",
            ShouldPublish = true,
            ProjectKey = "5350C966B6894A61B0913EB9FD5DC928"
        };

        uilmInsertItems.Add(uilmItem);
        finalModules.Add(genericAppModule);
        Console.WriteLine($"   📝 [FILTER A] Created generic-app from {sourceLabel} group");
    }

    /// <summary>
    /// Normalize value for comparison
    /// </summary>
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

    /// <summary>
    /// Convert resources to UILM format
    /// </summary>
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