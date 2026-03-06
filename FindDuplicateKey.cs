using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ConsoleApp;

public class FindDuplicateKey
{
    public static void FindKey()
    {

        // JSON read
        var json = File.ReadAllText("E:\\EcoHubEssentials\\RND\\ConsoleApp\\uilm_json.json");
        var items = JsonSerializer.Deserialize<List<TranslationItem>>(json);

        var duplicateGroups = items
            .GroupBy(x => x.KeyName)
            .Where(g => g.Count() > 1)
            .ToList();
        var outputPath = $"duplicate-report-{DateTime.Now:yyyyMMddHHmmss}.txt";
        var outputPathOnlyduplicateKeyName = $"duplicate-keyName-{DateTime.Now:yyyyMMddHHmmss}.txt";
        var sb = new StringBuilder();
        var sbkeyName = new StringBuilder();
        if (duplicateGroups.Any())
        {
            sb.AppendLine("Duplicate Key Report");
            sb.AppendLine();
            sb.AppendLine($"Total Duplicate Key Count: {duplicateGroups.Count}");
            sb.AppendLine("====================================");
            sb.AppendLine();

            sbkeyName.AppendLine("Duplicate Key Name Report");
            sbkeyName.AppendLine();
            sbkeyName.AppendLine($"Total Duplicate Key Count: {duplicateGroups.Count}");
            sbkeyName.AppendLine("====================================");
            sbkeyName.AppendLine();
            int i = 0;
            foreach (var group in duplicateGroups)
            {
                i++;
                sbkeyName.AppendLine($"Sl:{i}. {group.Key}");
                File.WriteAllText(outputPathOnlyduplicateKeyName, sbkeyName.ToString());


                sb.AppendLine($"KeyName: {group.Key}");
                sb.AppendLine($"Total Count: {group.Count()}");

                var moduleGroups = group.GroupBy(x => x.Module);

                sb.AppendLine("Module Breakdown:");

                foreach (var moduleGroup in moduleGroups)
                {
                    sb.AppendLine($"   - {moduleGroup.Key}  ({moduleGroup.Count()})");
                    foreach (var item in moduleGroup)
                    {
                        sb.AppendLine($"       Id: {item._id}");
                        var value = item.Resources.Where(r => r.Culture == "en-US").Select(r => r.Value).FirstOrDefault() ?? "N/A";
                        sb.AppendLine();
                        sb.AppendLine($"           Value: {value}");

                    }
                }

                sb.AppendLine("------------------------------------");
                sb.AppendLine();
            }

            sb.AppendLine($"Total Duplicate Keys: {duplicateGroups.Count}");
        }
        else
        {
            sb.AppendLine("No duplicate key found.");
        }

        // ---------------- UNIQUE MODULE SECTION ----------------
        sb.AppendLine();
        sb.AppendLine("Unique Module List");
        sb.AppendLine("====================================");

        var uniqueModules = items
            .Select(x => x.Module)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct()
            .OrderBy(x => x)
            .ToList();

        for (int i = 0; i < uniqueModules.Count; i++)
        {
            sb.AppendLine($"{i + 1}. {uniqueModules[i]}");
        }

        sb.AppendLine();
        sb.AppendLine($"Total Unique Modules: {uniqueModules.Count}");

        File.WriteAllText(outputPath, sb.ToString());

        Console.WriteLine("Duplicate report generated successfully!");

    }
}
