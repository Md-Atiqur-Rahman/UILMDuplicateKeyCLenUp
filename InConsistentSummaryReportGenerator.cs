using MongoDB.Bson;
using MongoDB.Driver;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using System.Text.RegularExpressions;

namespace ConsoleApp;

public static class InConsistentSummaryReportGenerator
{
    private class ModuleDetail
    {
        public string KeyName { get; set; }
        public int TotalModules { get; set; }
        public int MatchingModules { get; set; }
        public int NonMatchingModules { get; set; }
        public string ReferenceModule { get; set; }

        public List<string> MatchedModulesList { get; set; } = new();
        public List<string> UnmatchedModulesList { get; set; } = new();
        public List<MismatchDetail> MismatchDetails { get; set; } = new();
    }

    private class MismatchDetail
    {
        public string Culture { get; set; }
        public string ReferenceValue { get; set; }
        public string DifferentValue { get; set; }
        public string ModuleName { get; set; }
    }

    private class FilterSummary
    {
        public string Filter { get; set; }
        public string HasRootModule { get; set; }
        public string HasGenericModule { get; set; }
        public int SameLanguageFalseCount { get; set; }

        public List<ModuleDetail> Details { get; set; } = new();
    }

    private class ModuleInfo
    {
        public string ModuleName { get; set; }
        public Dictionary<string, string> CultureValues { get; set; } = new();
        public int Occurrence { get; set; } // Track which occurrence this is
    }

    public static async Task GenerateInConsistentSummaryReport(
        string? keyNameFilter = null,
        bool? hasRootModule = null,
        bool? hasGenericModule = null)
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("UILM_DB");
        var collection = database.GetCollection<BsonDocument>("Duplicate_Key_Report");

        var filterBuilder = Builders<BsonDocument>.Filter;
        var filter = filterBuilder.Empty;

        filter &= filterBuilder.Eq("Isconsistent", false);

        if (!string.IsNullOrEmpty(keyNameFilter))
            filter &= filterBuilder.Eq("KeyName", keyNameFilter);

        if (hasRootModule.HasValue)
            filter &= filterBuilder.Eq("HasRootModule", hasRootModule.Value);

        if (hasGenericModule.HasValue)
            filter &= filterBuilder.Eq("HasGenericModule", hasGenericModule.Value);

        var results = await collection.Find(filter).ToListAsync();

        if (!results.Any())
        {
            Console.WriteLine("No Inconsistent records found.");
            return;
        }

        GenerateSummaryTablePdf(results);


        Console.WriteLine("✅ Inconsistent summary Report Generated : InconsistentSummaryReport.pdf");
    }

    private static string NormalizeValue(string value)
    {
        if (string.IsNullOrEmpty(value))
            return "";

        value = value.Trim().ToLower();

        value = Regex.Replace(value, @"[^\w\s]", "");
        value = Regex.Replace(value, @"\s+", "");

        return value;
    }

    private static List<FilterSummary> GetFilterSummaries(List<BsonDocument> results)
    {
        var summaries = new Dictionary<string, FilterSummary>
        {
            { "A", new FilterSummary { Filter = "A", HasRootModule = "False", HasGenericModule = "False" } },
            { "B", new FilterSummary { Filter = "B", HasRootModule = "False", HasGenericModule = "True" } },
            { "C", new FilterSummary { Filter = "C", HasRootModule = "True", HasGenericModule = "False" } },
            { "D", new FilterSummary { Filter = "D", HasRootModule = "True", HasGenericModule = "True" } }
        };

        var expectedCultures = new[] { "en-US", "de-DE", "fr-FR", "it-IT" };

        var keyGroupedResults = results.GroupBy(doc => doc["KeyName"].AsString).ToList();

        foreach (var keyGroup in keyGroupedResults)
        {
            var firstDoc = keyGroup.First();

            bool root = firstDoc["HasRootModule"].AsBoolean;
            bool generic = firstDoc["HasGenericModule"].AsBoolean;

            string filterKey = root switch
            {
                false when !generic => "A",
                false when generic => "B",
                true when !generic => "C",
                true when generic => "D",
                _ => ""
            };

            if (string.IsNullOrEmpty(filterKey))
                continue;

            string keyName = keyGroup.Key;

            // Use List to preserve all module occurrences (including duplicates)
            var moduleList = new List<ModuleInfo>();

            foreach (var doc in keyGroup)
            {
                var modules = doc["Modules"].AsBsonArray;

                foreach (var module in modules)
                {
                    var moduleName = module["Module"].AsString;
                    var resources = module["Resources"].AsBsonArray;

                    var moduleInfo = new ModuleInfo
                    {
                        ModuleName = moduleName,
                        Occurrence = moduleList.Count(m => m.ModuleName == moduleName) + 1
                    };

                    foreach (var res in resources)
                    {
                        var culture = res["Culture"].AsString;
                        var value = res["Value"].AsString;

                        moduleInfo.CultureValues[culture] = value;
                    }

                    moduleList.Add(moduleInfo);
                }
            }

            // Determine reference module (first occurrence)
            string referenceModule = "";
            
            if (moduleList.FirstOrDefault(m => m.ModuleName == "app-root") != null)
                referenceModule = "app-root";
            else if (moduleList.FirstOrDefault(m => m.ModuleName == "generic-app") != null)
                referenceModule = "generic-app";
            else
                referenceModule = moduleList.FirstOrDefault()?.ModuleName ?? "";

            if (string.IsNullOrEmpty(referenceModule))
                continue;

            var referenceModule_Info = moduleList.First(m => m.ModuleName == referenceModule);
            var referenceValues = referenceModule_Info.CultureValues;

            var matchedModules = new List<string>();
            var unmatchedModules = new List<string>();
            var mismatchDetails = new List<MismatchDetail>();

            // Reference module always matched
            matchedModules.Add($"{referenceModule_Info.ModuleName} (Occurrence {referenceModule_Info.Occurrence})");

            foreach (var moduleInfo in moduleList.OrderBy(m => m.ModuleName).ThenBy(m => m.Occurrence))
            {
                if (moduleInfo.ModuleName == referenceModule_Info.ModuleName && moduleInfo.Occurrence == referenceModule_Info.Occurrence)
                    continue;

                var moduleValues = moduleInfo.CultureValues;

                bool isMatch = true;

                foreach (var culture in expectedCultures)
                {
                    var refValue = referenceValues.ContainsKey(culture)
                        ? NormalizeValue(referenceValues[culture])
                        : "";

                    var modValue = moduleValues.ContainsKey(culture)
                        ? NormalizeValue(moduleValues[culture])
                        : "";

                    if (refValue != modValue)
                    {
                        isMatch = false;

                        mismatchDetails.Add(new MismatchDetail
                        {
                            Culture = culture,
                            ReferenceValue = referenceValues.ContainsKey(culture)
                                ? referenceValues[culture]
                                : "MISSING",

                            DifferentValue = moduleValues.ContainsKey(culture)
                                ? moduleValues[culture]
                                : "MISSING",

                            ModuleName = $"{moduleInfo.ModuleName} (Occurrence {moduleInfo.Occurrence})"
                        });
                    }
                }

                string displayName = $"{moduleInfo.ModuleName} (Occurrence {moduleInfo.Occurrence})";
                
                if (isMatch)
                    matchedModules.Add(displayName);
                else
                    unmatchedModules.Add(displayName);
            }

            var detail = new ModuleDetail
            {
                KeyName = keyName,
                TotalModules = moduleList.Count,  // ✅ Now shows ALL 3 modules
                MatchingModules = matchedModules.Count,
                NonMatchingModules = unmatchedModules.Count,
                ReferenceModule = $"{referenceModule_Info.ModuleName} (Occurrence {referenceModule_Info.Occurrence})",
                MatchedModulesList = matchedModules,
                UnmatchedModulesList = unmatchedModules,
                MismatchDetails = mismatchDetails
            };

            summaries[filterKey].Details.Add(detail);
            summaries[filterKey].SameLanguageFalseCount++;
        }

        return summaries.Values.Where(s => s.SameLanguageFalseCount > 0).ToList();
    }
    private static void GenerateSummaryTablePdf(List<BsonDocument> results)
    {
        var filterSummaries = GetFilterSummaries(results);
        int grandTotal = filterSummaries.Sum(s => s.SameLanguageFalseCount);
        int totalDuplicateModules = filterSummaries.Sum(s => s.Details.Sum(d => d.TotalModules));

        var document = Document.Create(container =>
        {
            container.Page(page =>
            {
                page.Size(PageSizes.A4);
                page.Margin(20);

                page.Content().Column(col =>
                {
                    col.Item().Text("Duplicate Key Module Culture Mismatch Summary Report")
                        .FontSize(18).Bold();

                    col.Item().Text($"Generated: {DateTime.Now}")
                        .FontSize(10);

                    col.Item().PaddingTop(10).Text($"Total Duplicate Keys: {grandTotal}")
                        .FontSize(11).Bold();

                    col.Item().Text($"Total Duplicate Modules: {totalDuplicateModules}")
                        .FontSize(11).Bold();

                    foreach (var summary in filterSummaries)
                    {
                        col.Item().PaddingTop(15)
                            .Text($"Filter {summary.Filter} - HasRootModule: {summary.HasRootModule} | HasGenericModule: {summary.HasGenericModule}")
                            .FontSize(11).Bold();

                        int filterDuplicateKeys = summary.SameLanguageFalseCount;
                        int filterDuplicateModules = summary.Details.Sum(d => d.TotalModules);

                        col.Item().Text($"Duplicate Keys: {filterDuplicateKeys} | Duplicate Modules: {filterDuplicateModules}")
                            .FontSize(10);

                        col.Item().PaddingTop(10).Table(table =>
                        {
                            table.ColumnsDefinition(columns =>
                            {
                                columns.RelativeColumn(0.5f); // #
                                columns.RelativeColumn(2f);   // KeyName
                                columns.RelativeColumn(1.2f); // Total Modules
                                columns.RelativeColumn(1f);   // Matching
                                columns.RelativeColumn(1f);   // Non-Matching
                                columns.RelativeColumn(2f);   // MatchingModule
                                columns.RelativeColumn(2f);   // Non-MatchingModule
                            });

                            table.Header(header =>
                            {
                                header.Cell().Background("#4472C4").Text("#").FontSize(9).Bold().FontColor(Colors.White);
                                header.Cell().Background("#4472C4").Text("KeyName").FontSize(9).Bold().FontColor(Colors.White);
                                header.Cell().Background("#4472C4").Text("Total Modules").FontSize(9).Bold().FontColor(Colors.White);
                                header.Cell().Background("#4472C4").Text("Matching").FontSize(9).Bold().FontColor(Colors.White);
                                header.Cell().Background("#4472C4").Text("Non-Matching").FontSize(9).Bold().FontColor(Colors.White);
                                header.Cell().Background("#4472C4").Text("MatchingModule").FontSize(9).Bold().FontColor(Colors.White);
                                header.Cell().Background("#4472C4").Text("Non-MatchingModule").FontSize(9).Bold().FontColor(Colors.White);
                            });

                            int index = 1;
                            foreach (var detail in summary.Details.OrderBy(d => d.KeyName))
                            {
                                table.Cell().Text(index.ToString()).FontSize(9);
                                table.Cell().Text(detail.KeyName).FontSize(9);
                                table.Cell().Text(detail.TotalModules.ToString()).FontSize(9);
                                table.Cell().Text(detail.MatchingModules.ToString()).FontSize(9);
                                table.Cell().Text(detail.NonMatchingModules.ToString()).FontSize(9);
                                table.Cell().Text(string.Join(", ", detail.MatchedModulesList)).FontSize(8);
                                table.Cell().Text(string.Join(", ", detail.UnmatchedModulesList)).FontSize(8);
                                index++;
                            }

                            // Total row for this filter
                            table.Cell().Background("#D3D3D3").Text("Total").FontSize(9).Bold();
                            table.Cell().Background("#D3D3D3").Text(summary.Details.Count.ToString()).FontSize(9).Bold();
                            table.Cell().Background("#D3D3D3").Text(filterDuplicateModules.ToString()).FontSize(9).Bold();
                            table.Cell().Background("#D3D3D3").Text(summary.Details.Sum(d => d.MatchingModules).ToString()).FontSize(9).Bold();
                            table.Cell().Background("#D3D3D3").Text(summary.Details.Sum(d => d.NonMatchingModules).ToString()).FontSize(9).Bold();
                            table.Cell().Background("#D3D3D3").Text("").FontSize(9);
                            table.Cell().Background("#D3D3D3").Text("").FontSize(9);
                        });
                    }

                    // Grand total section
                    col.Item().PaddingTop(20).Border(1).Padding(10).Column(grandTotalCol =>
                    {
                        grandTotalCol.Item().Text("GRAND TOTAL").FontSize(12).Bold();
                        grandTotalCol.Item().PaddingTop(5).Text($"Total Duplicate Keys: {grandTotal}").FontSize(11).Bold();
                        grandTotalCol.Item().Text($"Total Duplicate Modules: {totalDuplicateModules}").FontSize(11).Bold();
                    });
                });
            });
        });

        document.GeneratePdf("InconsistentSummaryReport.pdf");
    }
}