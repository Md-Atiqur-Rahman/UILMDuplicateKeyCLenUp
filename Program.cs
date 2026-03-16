// See https://aka.ms/new-console-template for more information
using ConsoleApp;
using QuestPDF.Infrastructure;

// Set Community license (for organizations with < $1M annual revenue)
QuestPDF.Settings.License = LicenseType.Community;

//await UilmService.CallGetApi("21b9e142-48ef-46cc-81c5-50907ba36f72");

//await DuplicateDetector.GenerateDuplicateReport();
await TestDuplicateReportAnalyzer.GenerateSummaryReport(
    keyNameFilter: null,
    //hasRootModule: false,
    //hasGenericModule: false,
    isConsistent: true,
    isDeletePermission: false
);
//await DuplicateReportAnalyzer.GenerateSummaryReport(
//    keyNameFilter: "SALUTATION_MS",
//    hasRootModule: true,
//    hasGenericModule: false,
//    isConsistent: true,
//    isDeletePermission: true
//);

//Generate InConsistentSummary Report
//await InConsistentSummaryReportGenerator.GenerateInConsistentSummaryReport(
//    keyNameFilter: null
//);