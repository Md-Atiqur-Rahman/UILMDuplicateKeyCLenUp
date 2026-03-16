using MongoDB.Bson;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace ConsoleApp;

public static class UilmBulkOperationService
{
    private const string BaseUrl = "";
    private const string ProjectKey = "";
    private const string BlocksKey = "";
    private const string BearerToken = "";

    public class UilmKeyForBulkSave
    {
        public string ItemId { get; set; }
        public string KeyName { get; set; }
        public string ModuleId { get; set; }
        public ResourceItemForSave[] Resources { get; set; }
        public string[] Routes { get; set; }
        public bool IsPartiallyTranslated { get; set; }
        public bool IsNewKey { get; set; }
        public DateTime LastUpdateDate { get; set; }
        public DateTime CreateDate { get; set; }
        public string Context { get; set; }
        public bool ShouldPublish { get; set; }
        public string ProjectKey { get; set; }
    }

    public class ResourceItemForSave
    {
        public string Value { get; set; }
        public string Culture { get; set; }
        public int CharacterLength { get; set; }
    }

    private static HttpClient CreateHttpClient()
    {
        var client = new HttpClient();
        client.BaseAddress = new Uri(BaseUrl);
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", BearerToken);
        client.DefaultRequestHeaders.Accept.Clear();
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/plain"));
        client.DefaultRequestHeaders.Add("x-blocks-key", BlocksKey);
        return client;
    }

    /// <summary>
    /// Check if item exists in UILM
    /// </summary>
    public static async Task<bool> ItemExistsAsync(string itemId)
    {
        try
        {
            using var client = CreateHttpClient();
            var url = $"/uilm/v1/Key/Get?ItemId={itemId}&ProjectKey={ProjectKey}";
            var response = await client.GetAsync(url);
            return response.IsSuccessStatusCode;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error checking item existence: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Delete single key from UILM
    /// </summary>
    private static async Task<bool> DeleteKeyAsync(string itemId, HttpClient client)
    {
        try
        {
            var url = $"/uilm/v1/Key/Delete?ItemId={itemId}&ProjectKey={ProjectKey}";
            
            var response = await client.DeleteAsync(url);
            var content = await response.Content.ReadAsStringAsync();

            if (response.IsSuccessStatusCode)
            {
                Console.WriteLine($"   ✓ Deleted: {itemId}");
                return true;
            }
            else
            {
                Console.WriteLine($"   ✗ Failed to delete: {itemId} - {response.StatusCode}");
                return false;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ✗ Error deleting {itemId}: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Bulk delete items from UILM (using individual delete calls)
    /// </summary>
    public static async Task<(bool Success, string Message)> BulkDeleteAsync(List<string> itemIds)
    {
        try
        {
            if (!itemIds.Any())
                return (false, "No items to delete");

            Console.WriteLine($"\n🗑️  Deleting {itemIds.Count} items from UILM...");
            
            using var client = CreateHttpClient();
            int successCount = 0;
            int failureCount = 0;

            foreach (var itemId in itemIds)
            {
                bool success = await DeleteKeyAsync(itemId, client);
                if (success)
                    successCount++;
                else
                    failureCount++;

                // Small delay to avoid rate limiting
                await Task.Delay(100);
            }

            string message = $"Deleted: {successCount}/{itemIds.Count} items successfully. Failed: {failureCount}";
            bool overallSuccess = failureCount == 0;

            if (overallSuccess)
            {
                Console.WriteLine($"\n✅ Bulk Delete Successful: {successCount} items deleted");
            }
            else
            {
                Console.WriteLine($"\n⚠️  Bulk Delete Partial: {successCount} deleted, {failureCount} failed");
            }

            return (overallSuccess, message);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error during bulk delete: {ex.Message}");
            return (false, ex.Message);
        }
    }

    /// <summary>
    /// Bulk save items to UILM
    /// </summary>
    public static async Task<(bool Success, string Message)> BulkSaveAsync(List<UilmKeyForBulkSave> keys)
    {
        try
        {
            if (!keys.Any())
                return (false, "No items to save");

            Console.WriteLine($"\n💾 Saving {keys.Count} items to UILM...");

            using var client = CreateHttpClient();
            int successCount = 0;
            int failureCount = 0;

            foreach (var key in keys)
            {
                bool success = await SaveKeyAsync(key, client);
                if (success)
                    successCount++;
                else
                    failureCount++;

                // Small delay to avoid rate limiting
                await Task.Delay(100);
            }

            string message = $"Saved: {successCount}/{keys.Count} items successfully. Failed: {failureCount}";
            bool overallSuccess = failureCount == 0;

            if (overallSuccess)
            {
                Console.WriteLine($"\n✅ Bulk Save Successful: {successCount} items saved");
            }
            else
            {
                Console.WriteLine($"\n⚠️  Bulk Save Partial: {successCount} saved, {failureCount} failed");
            }

            return (overallSuccess, message);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error during bulk save: {ex.Message}");
            return (false, ex.Message);
        }
    }

    /// <summary>
    /// Save single key to UILM
    /// </summary>
    private static async Task<bool> SaveKeyAsync(UilmKeyForBulkSave key, HttpClient client)
    {
        try
        {
            var url = $"/uilm/v1/Key/SaveKeys?ProjectKey={ProjectKey}";
            
            key.ItemId = Guid.NewGuid().ToString();
            key.ProjectKey = ProjectKey;
            foreach (var resource in key.Resources ?? Array.Empty<UilmBulkOperationService.ResourceItemForSave>())
            {
                resource.CharacterLength = resource.Value?.Length ?? 0;
            }
            var options = new JsonSerializerOptions 
            { 
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true,
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            
            var jsonContent = JsonSerializer.Serialize(new[] { key }, options);
            
            Console.WriteLine($"\n📤 Sending request for: {key.KeyName}");
            Console.WriteLine($"   URL: {client.BaseAddress}{url}");
            Console.WriteLine($"   Payload:\n{jsonContent}");
            
            var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

            var response = await client.PostAsync(url, content);
            var responseContent = await response.Content.ReadAsStringAsync();

            Console.WriteLine($"   Status: {response.StatusCode}");
            Console.WriteLine($"   Response: {responseContent}");

            if (response.IsSuccessStatusCode)
            {
                Console.WriteLine($"   ✓ Saved: {key.KeyName}");
                return true;
            }
            else
            {
                Console.WriteLine($"   ✗ Failed to save: {key.KeyName} - {response.StatusCode}");
                Console.WriteLine($"   Response Body:\n{responseContent}");
                return false;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ✗ Error saving {key.KeyName}: {ex.Message}");
            Console.WriteLine($"   Exception: {ex.StackTrace}");
            return false;
        }
    }
}