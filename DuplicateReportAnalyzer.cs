using MongoDB.Bson;
using MongoDB.Driver;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Previewer;

namespace ConsoleApp;

public static class DuplicateReportAnalyzer
{
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

    private static void GeneratePdf(List<BsonDocument> results)
    {
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

                    col.Item().PaddingTop(20).Text("");

                    foreach (var doc in results)
                    {
                        string key = doc["KeyName"].AsString;
                        bool root = doc["HasRootModule"].AsBoolean;
                        bool generic = doc["HasGenericModule"].AsBoolean;
                        bool same = doc["AllCultureValuesSame"].AsBoolean;

                        col.Item().Text($"KeyName: {key}")
                            .FontSize(14)
                            .Bold();

                        col.Item().Text($"HasRootModule: {root} | HasGenericModule: {generic} | AllCultureValuesSame: {same}")
                            .FontSize(10);

                        col.Item().PaddingTop(10).Table(table =>
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
                                header.Cell().Background("#CCCCCC").Text("Module").FontSize(9).Bold();
                                header.Cell().Background("#CCCCCC").Text("en-US").FontSize(9).Bold();
                                header.Cell().Background("#CCCCCC").Text("de-DE").FontSize(9).Bold();
                                header.Cell().Background("#CCCCCC").Text("fr-FR").FontSize(9).Bold();
                                header.Cell().Background("#CCCCCC").Text("it-IT").FontSize(9).Bold();
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

                                table.Cell().Text(moduleName).FontSize(9);
                                table.Cell().Text(en).FontSize(9);
                                table.Cell().Text(de).FontSize(9);
                                table.Cell().Text(fr).FontSize(9);
                                table.Cell().Text(it).FontSize(9);

                                Console.WriteLine($"{moduleName} | {en} | {de} | {fr} | {it}");
                            }
                        });

                        col.Item().PaddingTop(15).Text("");
                    }
                });
            });
        });

        document.GeneratePdf("DuplicateKeyReport.pdf");
    }
}