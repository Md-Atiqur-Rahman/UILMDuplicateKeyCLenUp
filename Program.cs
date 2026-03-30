// See https://aka.ms/new-console-template for more information
using ConsoleApp;
using QuestPDF.Infrastructure;

// Set Community license (for organizations with < $1M annual revenue)
QuestPDF.Settings.License = LicenseType.Community;

//await UilmService.CallGetApi("e2a0d946-0235-42b1-8acd-3dc712d71801");
//await UilmBulkOperationService.ExecuteFilterACleanupMigration();
//await DuplicateDetector.GenerateDuplicateReport();
await InconsistentDuplicateCleaner.GenerateSummaryReport(
    keyNameFilter: null,
    hasRootModule: false,
    hasGenericModule: false,
    isConsistent: false,
    isDeletePermission: false
);
//await TestDuplicateReportAnalyzer.GenerateSummaryReport(
//    keyNameFilter: null,
//    hasRootModule: true,
//    hasGenericModule: true,
//    isConsistent: false,
//    isDeletePermission: false
//);
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