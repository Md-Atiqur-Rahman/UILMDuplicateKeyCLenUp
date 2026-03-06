using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp;

public class UilmKey
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string ItemId { get; set; }   // maps to "_id"

    public string TenantId { get; set; }
    public string ModuleId { get; set; }
    public string Module { get; set; }
    public object Routes { get; set; }
    public string KeyName { get; set; }
    public bool IsPartiallyTranslated { get; set; }

    public List<ResourceItem> Resources { get; set; }
}

public class ResourceItem
{
    public string Value { get; set; }
    public string Culture { get; set; }
    public int CharacterLength { get; set; }
}
