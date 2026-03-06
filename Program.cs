// See https://aka.ms/new-console-template for more information
using ConsoleApp;
//Console.WriteLine("Enter your FileId.");
//var fileId = Console.ReadLine();
//if(fileId == null)
//{     Console.WriteLine("FileId cannot be null. Exiting.");
//    return;
//}
//var accessToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZW5hbnRfaWQiOiJkN2U1NTU0Yzc1ODU0MWRiOGExODY5NGI2NGVmNDIzZCIsInN1YiI6ImJsb2Nrc3w4MzBjYzM5Zi1kZDg1LTQ4YzAtOGExYi1mYWE1Yjg4MTQxZmYiLCJ1c2VyX2lkIjoiODMwY2MzOWYtZGQ4NS00OGMwLThhMWItZmFhNWI4ODE0MWZmIiwiaWF0IjoxNzcyMzc3MTYxLCJvcmdfaWQiOiJkZWZhdWx0IiwiZW1haWwiOiJhdGlxdXIuaGltZWxAc2VsaXNlZ3JvdXAuY29tIiwidXNlcl9uYW1lIjoiYXRpcXVyLmhpbWVsQHNlbGlzZWdyb3VwLmNvbSIsIm5hbWUiOiIiLCJwaG9uZSI6IiIsInJvbGVzIjoidXNlciIsIm5iZiI6MTc3MjM3NzE2MSwiZXhwIjoxNzcyODU3MTYxLCJpc3MiOiJTZWxpc2UtQmxvY2tzIiwiYXVkIjoiaHR0cHM6Ly9jbG91ZC5zZWxpc2VibG9ja3MuY29tIn0.L1R60bPTe54z1RhoLkHys_Pw4VU6W1MRKChWX2_qTbb9JRyIjRE50B9ZdslpoU7GxQPLubWrG80_ZBQ_0Yuh5eGVP4KN_x3KBY2pK9j_R1XnND90rjBhRUrIjdgBA_4Ln4iaMuuwEIVM-IjcRIX_uqwb4VuHpLjsB6Egpct2_UOeeoKnWTusoU6C-D2as09KJgUDs9gDngm8xC2BygH1mzI1F5Kgup6s8NHKsZS5QmH_6WUV9Cu1t9gpzoGQT2hu6nQPH7hjVcOvIpM3iR_2d07mnfF5vUBy0SiuWNPTlGDB0U53nQsfMmZRgoB0fplH2AOiabR9PqcKEBtMN17AbQ";

//UilmService.CallGetApi(fileId, accessToken).Wait();

//DuplicateDetector.GenerateDuplicateReport().Wait();

await DuplicateReportAnalyzer.GenerateSummaryReport(
    keyNameFilter: null,
    allCultureValuesSame: false
);
