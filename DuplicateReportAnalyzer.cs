using MongoDB.Bson;
using MongoDB.Driver;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Previewer;
using System.Text.RegularExpressions;

namespace ConsoleApp;

public static class DuplicateReportAnalyzer
{
    private class SummaryStats
    {
        public int TotalDuplicateKeys { get; set; }
        public int TotalDuplicateModules { get; set; }
        public int DuplicateKeyForSameLanguageTrue { get; set; }
        public int DuplicateModuleForSameLanguageTrue { get; set; }
        public int DuplicateKeyForSameLanguageFalse { get; set; }
        public int DuplicateModuleForSameLanguageFalse { get; set; }
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
        bool? isconsistent = null)
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("UILM_DB");
        var collection = database.GetCollection<BsonDocument>("Duplicate_Key_Report");

        var filterBuilder = Builders<BsonDocument>.Filter;
        var filter = filterBuilder.Empty;

        if (!string.IsNullOrEmpty(keyNameFilter))
            filter &= filterBuilder.Eq("KeyName", keyNameFilter);

        if (hasRootModule.HasValue)
            filter &= filterBuilder.Eq("HasRootModule", hasRootModule.Value);

        if (hasGenericModule.HasValue)
            filter &= filterBuilder.Eq("HasGenericModule", hasGenericModule.Value);

        if (isconsistent.HasValue)
            filter &= filterBuilder.Eq("Isconsistent", isconsistent.Value);

        var results = await collection.Find(filter).ToListAsync();

        if (!results.Any())
        {
            Console.WriteLine("No matching records found.");
            return;
        }

        GeneratePdf(results);

        Console.WriteLine("✅ PDF Report Generated : DuplicateKeyReport.pdf");
    }

    private static string NormalizeValue(string value)
    {
        if (string.IsNullOrEmpty(value))
            return "";

        // Trim spaces
        value = value.Trim();
        
        // Convert to lowercase
        value = value.ToLower();
        
        // Remove punctuation and special characters (keep only alphanumeric and spaces)
        value = Regex.Replace(value, @"[^\w\s]", "");
        
        // Remove extra spaces
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
            bool same = doc["Isconsistent"].AsBoolean;

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

                if (same)
                {
                    summaryData[key].DuplicateKeyForSameLanguageTrue++;
                    summaryData[key].DuplicateModuleForSameLanguageTrue += moduleCount;
                }
                else
                {
                    summaryData[key].DuplicateKeyForSameLanguageFalse++;
                    summaryData[key].DuplicateModuleForSameLanguageFalse += moduleCount;
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

            // Check if all RAW values are the same
            var allRawSame = en == de && de == fr && fr == it && !string.IsNullOrEmpty(en);

            // Check if all NORMALIZED values are the same
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

    // Check if each culture value is consistent across all modules
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

    private static void GeneratePdf(List<BsonDocument> results)
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
                        });

                        table.Header(header =>
                        {
                            header.Cell().Background("#4472C4").Text("Filter").FontSize(8).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Root").FontSize(8).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Generic").FontSize(8).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Total Duplicate Keys").FontSize(8).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Total Duplicate Modules").FontSize(8).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Consistent Keys (Same=True)").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Consistent Modules (Same=True)").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Inconsistent Keys (Same=False)").FontSize(7).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Inconsistent Modules (Same=False)").FontSize(7).Bold().FontColor(Colors.White);
                        });

                        var rows = new[] { ("A", false, false), ("B", false, true), ("C", true, false), ("D", true, true) };
                        int grandTotalKeys = 0, grandTotalModules = 0, grandKeysTrue = 0, grandModulesTrue = 0, grandKeysFalse = 0, grandModulesFalse = 0;

                        foreach (var (letter, hasRoot, hasGeneric) in rows)
                        {
                            var stats = summaryStats[letter];
                            grandTotalKeys += stats.TotalDuplicateKeys;
                            grandTotalModules += stats.TotalDuplicateModules;
                            grandKeysTrue += stats.DuplicateKeyForSameLanguageTrue;
                            grandModulesTrue += stats.DuplicateModuleForSameLanguageTrue;
                            grandKeysFalse += stats.DuplicateKeyForSameLanguageFalse;
                            grandModulesFalse += stats.DuplicateModuleForSameLanguageFalse;

                            table.Cell().Background("#E7E6E6").Text(letter).FontSize(8).Bold();
                            table.Cell().Text(hasRoot.ToString()).FontSize(8);
                            table.Cell().Text(hasGeneric.ToString()).FontSize(8);
                            table.Cell().Text(stats.TotalDuplicateKeys.ToString()).FontSize(8).Bold();
                            table.Cell().Text(stats.TotalDuplicateModules.ToString()).FontSize(8).Bold();
                            table.Cell().Text(stats.DuplicateKeyForSameLanguageTrue.ToString()).FontSize(8);
                            table.Cell().Text(stats.DuplicateModuleForSameLanguageTrue.ToString()).FontSize(8);
                            table.Cell().Text(stats.DuplicateKeyForSameLanguageFalse.ToString()).FontSize(8);
                            table.Cell().Text(stats.DuplicateModuleForSameLanguageFalse.ToString()).FontSize(8);
                        }

                        table.Cell().Background("#D3D3D3").Text("Total").FontSize(8).Bold();
                        table.Cell().Background("#D3D3D3").Text("").FontSize(8);
                        table.Cell().Background("#D3D3D3").Text("").FontSize(8);
                        table.Cell().Background("#D3D3D3").Text(grandTotalKeys.ToString()).FontSize(8).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandTotalModules.ToString()).FontSize(8).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandKeysTrue.ToString()).FontSize(8).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandModulesTrue.ToString()).FontSize(8).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandKeysFalse.ToString()).FontSize(8).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandModulesFalse.ToString()).FontSize(8).Bold();
                    });

                    col.Item().PaddingTop(30).Text("Detailed Key Analysis")
                        .FontSize(14)
                        .Bold();

                    // Detailed records
                    foreach (var doc in results)
                    {
                        string key = doc["KeyName"].AsString;
                        bool root = doc["HasRootModule"].AsBoolean;
                        bool generic = doc["HasGenericModule"].AsBoolean;
                        bool consistent = doc["Isconsistent"].AsBoolean;

                        col.Item().PaddingTop(15).Text($"KeyName: {key}")
                            .FontSize(12)
                            .Bold();

                        col.Item().Text($"HasRootModule: {root} | HasGenericModule: {generic} | IsConsistent (Raw): {consistent}")
                            .FontSize(9);

                        var normalizedModules = GetNormalizedModules(doc);

                        // Calculate normalized status for entire key
                        bool cultureConsistent = AreCultureValuesConsistent(normalizedModules);

                        // Culture Consistency Summary
                        if (cultureConsistent)
                        {
                            col.Item().PaddingTop(5).Text("✓ All modules use consistent translations")
                                .FontSize(9).Bold().FontColor("#008000");
                        }
                        else
                        {
                            col.Item().PaddingTop(5).Text("✗ Inconsistent translations detected")
                                .FontSize(9).Bold().FontColor("#FF0000");

                            // Show mismatch details
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

                        // Normalized Values Table Only
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

                                Console.WriteLine(
                                    $"{moduleData.ModuleName} | " +
                                    $"en: {moduleData.NormalizedEnUS} | " +
                                    $"de: {moduleData.NormalizedDeDE} | " +
                                    $"fr: {moduleData.NormalizedFrFR} | " +
                                    $"it: {moduleData.NormalizedItIT}"
                                );
                            }
                        });
                    }
                });
            });
        });

        document.GeneratePdf("DuplicateKeyReport.pdf");
    }
}