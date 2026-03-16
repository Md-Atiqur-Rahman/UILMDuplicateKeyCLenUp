// See https://aka.ms/new-console-template for more information
using ConsoleApp;
using QuestPDF.Infrastructure;

// Set Community license (for organizations with < $1M annual revenue)
QuestPDF.Settings.License = LicenseType.Community;

await UilmService.CallGetApi("082bb458-42a7-4b64-bf78-a0ae7be76a7b");

//await DuplicateDetector.GenerateDuplicateReport();

//await DuplicateReportAnalyzer.GenerateSummaryReport(
//    keyNameFilter: null,
//    //hasRootModule: false,
//    //hasGenericModule: false,
//    isConsistent: true,
//    isDeletePermission: false
//);

//Generate InConsistentSummary Report
//await InConsistentSummaryReportGenerator.GenerateInConsistentSummaryReport(
//    keyNameFilter: null
//);