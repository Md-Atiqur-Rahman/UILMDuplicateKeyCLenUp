using MongoDB.Bson;
using MongoDB.Driver;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Previewer;

namespace ConsoleApp;

public static class DuplicateReportAnalyzer
{
    private class SummaryStats
    {
        public int TotalRecords { get; set; }
        public int CultureValuesSameTrue { get; set; }
        public int CultureValuesSameFalse { get; set; }
    }

    public static async Task GenerateSummaryReport(
        string? keyNameFilter = null,
        bool? hasRootModule = null,
        bool? hasGenericModule = null,
        bool? allCultureValuesSame = null)
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

        if (allCultureValuesSame.HasValue)
            filter &= filterBuilder.Eq("AllCultureValuesSame", allCultureValuesSame.Value);

        var results = await collection.Find(filter).ToListAsync();

        if (!results.Any())
        {
            Console.WriteLine("No matching records found.");
            return;
        }

        GeneratePdf(results);

        Console.WriteLine("✅ PDF Report Generated : DuplicateKeyReport.pdf");
    }

    private static Dictionary<string, SummaryStats> GetSummaryStatistics(List<BsonDocument> results)
    {
        var summaryData = new Dictionary<string, SummaryStats>
        {
            { "A", new SummaryStats() },  // HasRootModule: False, HasGenericModule: False
            { "B", new SummaryStats() },  // HasRootModule: False, HasGenericModule: True
            { "C", new SummaryStats() },  // HasRootModule: True, HasGenericModule: False
            { "D", new SummaryStats() }   // HasRootModule: True, HasGenericModule: True
        };

        foreach (var doc in results)
        {
            bool root = doc["HasRootModule"].AsBoolean;
            bool generic = doc["HasGenericModule"].AsBoolean;
            bool same = doc["AllCultureValuesSame"].AsBoolean;

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
                summaryData[key].TotalRecords++;
                if (same)
                    summaryData[key].CultureValuesSameTrue++;
                else
                    summaryData[key].CultureValuesSameFalse++;
            }
        }

        return summaryData;
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
                    col.Item().Text("Duplicate Key Analysis Report")
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
                            columns.RelativeColumn(1.5f);
                            columns.RelativeColumn(1.8f);
                            columns.RelativeColumn(1.8f);
                            columns.RelativeColumn(1.2f);
                            columns.RelativeColumn(1.5f);
                            columns.RelativeColumn(1.5f);
                        });

                        // Header row
                        table.Header(header =>
                        {
                            header.Cell().Background("#4472C4").Text("Filter").FontSize(9).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("HasRootModule").FontSize(9).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("HasGenericModule").FontSize(9).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("Total").FontSize(9).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("SameLanguageTrue").FontSize(9).Bold().FontColor(Colors.White);
                            header.Cell().Background("#4472C4").Text("SameLanguageFalse").FontSize(9).Bold().FontColor(Colors.White);
                        });

                        // Data rows
                        var rows = new[] { ("A", false, false), ("B", false, true), ("C", true, false), ("D", true, true) };
                        int grandTotal = 0, grandSameTrue = 0, grandSameFalse = 0;

                        foreach (var (letter, hasRoot, hasGeneric) in rows)
                        {
                            var stats = summaryStats[letter];
                            grandTotal += stats.TotalRecords;
                            grandSameTrue += stats.CultureValuesSameTrue;
                            grandSameFalse += stats.CultureValuesSameFalse;

                            table.Cell().Background("#E7E6E6").Text(letter).FontSize(9).Bold();
                            table.Cell().Text(hasRoot.ToString()).FontSize(9);
                            table.Cell().Text(hasGeneric.ToString()).FontSize(9);
                            table.Cell().Text(stats.TotalRecords.ToString()).FontSize(9).Bold();
                            table.Cell().Text(stats.CultureValuesSameTrue.ToString()).FontSize(9);
                            table.Cell().Text(stats.CultureValuesSameFalse.ToString()).FontSize(9);
                        }

                        // Total row
                        table.Cell().Background("#D3D3D3").Text("Total").FontSize(9).Bold();
                        table.Cell().Background("#D3D3D3").Text("").FontSize(9);
                        table.Cell().Background("#D3D3D3").Text("").FontSize(9);
                        table.Cell().Background("#D3D3D3").Text(grandTotal.ToString()).FontSize(9).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandSameTrue.ToString()).FontSize(9).Bold();
                        table.Cell().Background("#D3D3D3").Text(grandSameFalse.ToString()).FontSize(9).Bold();
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
                        bool same = doc["AllCultureValuesSame"].AsBoolean;

                        col.Item().PaddingTop(15).Text($"KeyName: {key}")
                            .FontSize(12)
                            .Bold();

                        col.Item().Text($"HasRootModule: {root} | HasGenericModule: {generic} | AllCultureValuesSame: {same}")
                            .FontSize(9);

                        col.Item().PaddingTop(8).Table(table =>
                        {
                            table.ColumnsDefinition(columns =>
                            {
                                columns.RelativeColumn(1);
                                columns.RelativeColumn(1);
                                columns.RelativeColumn(1);
                                columns.RelativeColumn(1);
                                columns.RelativeColumn(1);
                            });

                            // Header row
                            table.Header(header =>
                            {
                                header.Cell().Background("#CCCCCC").Text("Module").FontSize(8).Bold();
                                header.Cell().Background("#CCCCCC").Text("en-US").FontSize(8).Bold();
                                header.Cell().Background("#CCCCCC").Text("de-DE").FontSize(8).Bold();
                                header.Cell().Background("#CCCCCC").Text("fr-FR").FontSize(8).Bold();
                                header.Cell().Background("#CCCCCC").Text("it-IT").FontSize(8).Bold();
                            });

                            // Data rows
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

                                table.Cell().Text(moduleName).FontSize(8);
                                table.Cell().Text(en).FontSize(8);
                                table.Cell().Text(de).FontSize(8);
                                table.Cell().Text(fr).FontSize(8);
                                table.Cell().Text(it).FontSize(8);

                                Console.WriteLine($"{moduleName} | {en} | {de} | {fr} | {it}");
                            }
                        });
                    }
                });
            });
        });

        document.GeneratePdf("DuplicateKeyReport.pdf");
    }
}