using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp;

public class TranslationItem
{
    public string _id { get; set; }

    public string KeyName { get; set; }

    public string Module { get; set; }

    public List<Resources> Resources { get; set; }
}

public class Resources
{
    public string Value { get; set; }
    public string Culture { get; set; } = "en-US";
}