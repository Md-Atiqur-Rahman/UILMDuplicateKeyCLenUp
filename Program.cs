// See https://aka.ms/new-console-template for more information
using ConsoleApp;
using QuestPDF.Infrastructure;

// Set Community license (for organizations with < $1M annual revenue)
QuestPDF.Settings.License = LicenseType.Community;


await DuplicateReportAnalyzer.GenerateSummaryReport(
    keyNameFilter: null,
    allCultureValuesSame: false
);
