using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp.Models;

public class DuplicateKeyReport
{
    public string KeyName { get; set; }
    public int TotalDuplicateCount { get; set; }
    public bool HasRootModule { get; set; }
    public bool HasGenericModule { get; set; }
    public bool Isconsistent { get; set; }
    public List<ModuleInfo> Modules { get; set; }
    public bool Processed { get; set; }
}

public class ModuleInfo
{
    public string Module { get; set; }
    public string Id { get; set; }
    public Dictionary<string, string> Values { get; set; }
}


