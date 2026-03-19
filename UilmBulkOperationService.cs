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
        public bool? IsPartiallyTranslated { get; set; }  // ✅ Changed to nullable
        public bool? IsNewKey { get; set; }  // ✅ Changed to nullable
        public DateTime? LastUpdateDate { get; set; }  // ✅ Changed to nullable
        public DateTime? CreateDate { get; set; }  // ✅ Changed to nullable
        public string Context { get; set; }
        public bool? ShouldPublish { get; set; }  // ✅ Changed to nullable
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
    public static async Task<bool> DeleteKeyAsync(string itemId, HttpClient client)
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
    public static async Task<bool> SaveKeyAsync(UilmKeyForBulkSave key, HttpClient client)
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


    public static List<string> GetInsertedIds()
    {
        return new List<string>
        {
            "c5257443-1946-4318-a021-ebd3a20b7de3",
            "cca436da-c01d-4a31-961d-b2ae9cf0d913",
            "14aee612-7e43-4103-b2ec-1ba84cb4311c",
            "30bb0528-f528-4166-983a-51693b54c404",
            "b65d04e4-9e80-4f5b-aa07-321531883c76",
            "901cb8d0-3f41-426f-b1bc-3a5eefdb3b0f",
            "fde15f7e-0cb9-4c65-b21d-1769c4721a1b",
            "2c947d85-61b7-4f6e-9b68-b7bc0ff4eb50",
            "5a71a916-4de4-4816-93b3-29f27e0f8917",
            "aac9a192-6509-4ec5-8063-027d096b5737",
            "467768c8-3d38-4860-b6a0-0df6768d15bf",
            "933fa8b6-a3b6-4e96-9e53-c2e02488dec5",
            "e487d79f-f128-4539-bfc1-1e9eaf941902",
            "81a6f362-bfac-4234-bfa1-29e4bf9a9e7c",
            "38687579-7218-4515-bfd3-152720e82d72",
            "a8f89e4c-e924-49b0-a4f1-29e9ae7ff54b",
            "1929ef3d-20f7-4428-b5d4-3a877e10e676",
            "d77eb2c6-5ed4-4890-babf-5dc68635ab4d",
            "a92c69ef-f9e0-48ee-96cf-07ef411c7458",
            "e47f2967-9d4f-4fad-af5c-cdf8fccac21f",
            "36f17cee-3e44-4867-948b-b74782d4c165",
            "7fe8c1c3-5318-4c6f-9f01-cc593fe0c9e7",
            "806f7f34-0300-4767-8c85-e16d76bf74c6",
            "5b49bdc4-f1af-4374-ad78-9c3d5b93c5d2",
            "0eec819d-a53c-4e91-9d3d-28796880894f",
            "4733b515-af4c-479c-8c29-0ee8929094eb",
            "fa304d0e-8660-4cb3-af89-5cc99e63a5a6",
            "84870d16-103b-4353-8bf8-70152dd08f5d",
            "d874b36c-5816-4ec6-af49-58bf4557b414",
            "2fbf4f36-6c31-4233-b2ca-3cdda0b66b47",
            "afe458f0-77e7-4069-b080-4a37a6e344e5",
            "6cbfa9a0-a22f-4786-908c-7749665e948a",
            "3bb95cf1-c43b-4147-8cab-0997e8124f1d",
            "8b3e9a08-e078-4c6d-8fe0-ed0ab16fdaef",
            "3ecf3b3e-5dee-4873-8561-90623033801e",
            "d5d5071c-08de-42ed-afcf-746586bc532a",
            "6debe18a-b656-4be7-a3c2-a84725e46c09",
            "faf99303-86fa-497e-a6e1-e4304e4a445e",
            "1d19e5af-d3eb-445a-b90d-a09fc73b6d1e",
            "fb9ece95-7f43-487e-9d76-b307b3e3bbb4",
            "9fccefa9-23b5-4be5-8d50-6685bc4110f6",
            "56401b0f-6a7f-4fca-9e9c-498efc165c4a",
            "9d864ab2-7951-43eb-8b3d-c04bac804299",
            "0a54dee2-3ccf-4435-98a2-5b88a9ba5397",
            "72596557-3da8-4b3f-9f33-a0ca2a929805",
            "36bf6794-b324-40de-a53c-2347d1053654",
            "2491f875-3050-4df1-996d-a24f458616da",
            "993ae836-eda4-48d6-8a27-eafcf5724a3e",
            "6d31a7ef-1a22-451c-baab-93bfb009c7bd",
            "7375a1cb-1b28-493d-a36e-41081b6b4003",
            "a0136f3f-f7f3-4de6-afbe-02fe52f8fc30",
            "b99e6c4e-075d-41dc-ae19-c29de1681241",
            "ba857b8a-8b80-4d5b-b5b0-c2cc2f1ef3da",
            "052effc3-d284-4f32-8e08-d0cf1392b709",
            "27000425-3520-43e6-9993-4d7f373b3b13",
            "74baecf9-385f-4477-9acf-c12f81b72413",
            "2df3652f-0a5d-4b44-9662-31deef1456a2",
            "b47047dc-02fc-446e-9132-6fcaf825d86e",
            "dc172c6c-cbb7-47a6-85ba-015a02c4a258",
            "c951e99c-1b93-4889-8910-c643e0615271",
            "c85ba616-dce8-491f-8409-ff14da60d0cb",
            "4edf158b-1735-4ce1-911c-b2075ec14ae0",
            "467fba9b-b59b-4a06-bc4c-87761c201780",
            "06b548a1-f1bf-4ae0-91b8-08882b656b17",
            "eff929bd-bf08-4df0-a066-7b50dd923861",
            "dcca5948-0d2b-4f29-b468-63b6cd145c3b",
            "3fb26952-915d-47ec-96cf-6ba5616a20a2",
            "10c1f452-f365-4515-b006-48d00dc2ae30",
            "b6ce6f63-bb2c-438f-9620-195276b743ca",
            "a2bf5347-f8a0-4272-b657-7b20221b2da1",
            "a054bf23-c474-4e41-b6dc-2398d6b47b93",
            "96c2de6a-70ff-4269-95bb-c93b7aa6c017",
            "db39fb23-ed75-475d-926b-69c87f439189",
            "61aac331-1391-416a-a020-4c29044d690b",
            "7b7118a5-845c-4c49-a0ad-4324d4a43953",
            "8b824d45-b6b4-418a-8122-8995b433f060",
            "b2fa4a22-bf31-4b9b-92d3-7c23a6d43b6b",
            "aa1dc048-5744-4f03-9757-00e08e2daa48",
            "77bb47b4-2ad4-41ab-a741-4efe63de9414",
            "9be54b77-627b-42c6-881d-ce9fb73d1929",
            "8c5c768e-d304-44a8-a3cd-b37e3bafa2ae",
            "641e8feb-83ec-4b52-bd57-c8cbbcf9835d",
            "78b52fe3-262f-4d05-bcd6-94a151403e56",
            "97ab6cf4-b1f3-4584-b188-4a450cb26a36",
            "685b633f-1f1a-439e-aacb-a63bb729ee4d",
            "6a57d230-bc01-496e-ae6b-2f5448c43f45",
            "a0325ce5-252c-40de-af2d-0227a3943455",
            "f983fbd5-7049-435c-bf15-36625e85bb13",
            "d4bcec60-05fb-4823-a02b-931b62097532",
            "e1ff146c-e743-481b-8d66-1c2a0cb42f6a",
            "b29231b9-e9b8-4b3c-b652-c0e7399afa43",
            "65b95133-5b7b-49d2-8de2-a1cfb6153b3d",
            "06eb460a-346d-4ba5-ac7c-60655324fce1",
            "cdf83674-a9cc-44ac-8c80-7dc67ad582df",
            "06c9e527-862f-41af-b4e6-f8ed75b76802",
            "5f34f06f-73ab-403e-84e9-8ddffe64c633",
            "108b4ae9-f003-4fa4-91d2-4e6c7c58179b",
            "1337cd35-f83d-4668-ac51-e721daf99788",
            "8ea72012-ab37-4751-ae24-591bf8ca7489",
            "193abcb6-9316-42aa-9d27-9b28e687b9a5",
            "a726fe90-fd66-433e-8c8f-66bdba0f4c55",
            "a85f4373-a863-41f2-a791-229885534114",
            "fc215b32-56b9-41cc-8e6e-5ff29f94ddde",
            "26fe5732-a4aa-4195-852c-1ed0db16f7ed",
            "5f892ba7-2b00-4feb-8939-976e4104a6ae",
            "d1eccee7-7392-42dc-99b6-8b9d9a21b3cd",
            "9a106667-8112-42c3-a8b8-d2c8d9409560",
            "67dae1fc-7572-4ce8-82ac-73c7dccd8419",
            "057cfee4-8e6e-424d-8b22-3544d4983b3d",
            "007e31e5-7f83-4d02-a4d3-97e0d0752e9d",
            "ce4f5097-8159-45a5-8f53-4d5beb651e15",
            "66d7a967-fc2d-40bd-9861-fc0fdec5f096",
            "cda3c05e-3dd0-40be-a0cf-fb29c62a14ce",
            "32e8c886-c036-4f3c-9f27-d958b44f1c42",
            "5b983371-3096-4540-be1a-395702a94520",
            "18495056-ca3e-449f-b060-1efb90227c65",
            "3f2d4e19-2075-4ebc-9ab3-209efc0ebe89",
            "50d6f8a1-9687-4a4e-98ab-710902f631cd",
            "6c706741-ee54-4f31-8f10-7d5687ea50b7",
            "f7fe7cc2-9473-4e33-95eb-db85020df81c",
            "3d7f2875-68ea-403f-adc2-3891f04659b8",
            "55cfbf50-6e2a-4b54-8c31-a376c765eaed",
            "57b63c47-2d22-42c6-a079-d525ba4dcf37",
            "38fa9dad-2a5d-463d-82e1-8490e26f0ed4",
            "94b4e77c-4029-4693-afe2-c655e4303f52",
            "337518b8-61c9-4e0c-8028-0f19ebd1afc7",
            "9341c653-d8ba-4967-9498-e7c10fddb1eb",
            "5e8363ce-d408-4259-a02e-670f7c3e2290",
            "c3f1cc4b-23e3-4be8-894b-ee630036f743",
            "13e47e9d-331a-41d3-b90b-2cdb7f045035",
            "242e2ef9-bb0f-4d39-995c-dbc8932486c7",
            "45e720f5-efd1-4907-801f-6d2b0bd42a9e",
            "7ed65916-3fb3-4236-81f9-615cedd38d42",
            "e4fb9dc6-e878-4895-8784-9cfb126b0930",
            "402e49a2-8152-4b40-b0ac-a8807ea106a1",
            "e6f30e5e-de87-43b4-8cc9-260a999a087d",
            "655c19d7-d5b3-4c3e-9068-d61f6297f9b0",
            "5ad7d2d4-6d15-4916-b00b-ad53ed74dacb",
            "88dee0f3-64ba-4337-b2c1-2bd6c811a9a6",
            "cff3245a-547e-4056-b6ef-8c50fadb99a8",
            "02ccad61-dc90-42bd-8c13-24d7dbeb169c",
            "75caa312-6159-43a5-8fcc-41eef3e4685f",
            "1237a47a-f094-4ea2-a950-e1122854b417",
            "e37e087a-afb8-402a-9ede-80c12c412346",
            "0151a160-1d81-45c5-8905-305d977700c5",
            "4956dd6b-fcda-4899-b712-fe28e9b66fce",
            "65d2eda5-2213-48d7-bd3c-1b59694466f1",
            "23beba43-7d7a-4956-85bf-c4d9d9085ad0",
            "bf665aa2-13ba-4187-9760-8823fc9ab33f",
            "5c157141-15ae-4a48-9df7-bfb0af85e4c0",
            "79f9500a-2012-49d5-bd89-cbb763172ae5",
            "098d6f40-bdd1-45bc-ad15-2ad4f9e26873",
            "62e1760a-b1f9-48a9-bfc1-380b66b864f2",
            "854d1719-b5df-4173-8cb2-1b5d28a70c5f",
            "64833c59-6724-4fa7-8c71-f8704e42e0ef",
            "3e6ec867-caf8-4a4a-b2f7-80c44d8a0b79",
            "8981716b-91ba-43a1-9af8-e2db335685a3"
        };
    }

    public static async Task ExecuteFilterACleanupMigration()
    {
        Console.WriteLine("\n🔧 Starting Filter A Cleanup Migration (Optimized)...\n");

        var insertedAppRootIds = GetInsertedIds();
        const string AppRootModuleId = "5aa9386f-78cf-4084-bc20-76a38b627ea4";
        const string GenericAppModuleId = "f43680b9-df15-4c01-998d-eac8fac79d25";

        // Step 1: Fetch all items in ONE batch (not in foreach loop)
        Console.WriteLine($"📥 Step 1: Fetching all {insertedAppRootIds.Count} app-root items data from UILM in batch...");
        var appRootItemsData = await GetBatchItemsByIdsAsync(insertedAppRootIds, AppRootModuleId, ProjectKey);

        if (!appRootItemsData.Any())
        {
            Console.WriteLine("❌ No app-root items found in UILM.");
            return;
        }

        Console.WriteLine($"✅ Retrieved {appRootItemsData.Count} app-root items\n");

        // Step 2: Prepare new generic-app items for saving
        Console.WriteLine($"💾 Step 2: Preparing {appRootItemsData.Count} new generic-app items...");
        var itemsToSave = new List<UilmKeyForBulkSave>();
        var itemsToDelete = new List<string>();

        foreach (var appRootItem in appRootItemsData)
        {
            var newItem = new UilmKeyForBulkSave
            {
                ItemId = Guid.NewGuid().ToString(),
                KeyName = appRootItem.KeyName,
                ModuleId = GenericAppModuleId,
                Resources = appRootItem.Resources ?? Array.Empty<UilmBulkOperationService.ResourceItemForSave>(),
                Routes = null,
                IsPartiallyTranslated = appRootItem.IsPartiallyTranslated ?? false,  // ✅ Handle null
                IsNewKey = appRootItem.IsNewKey ?? true,  // ✅ Handle null
                LastUpdateDate = appRootItem.LastUpdateDate ?? DateTime.UtcNow,  // ✅ Handle null
                CreateDate = appRootItem.CreateDate ?? DateTime.UtcNow,  // ✅ Handle null
                Context = "Auto-generated from Filter A cleanup migration",
                ShouldPublish = appRootItem.ShouldPublish ?? true,  // ✅ Handle null
                ProjectKey = ProjectKey
            };


            itemsToSave.Add(newItem);
            itemsToDelete.Add(appRootItem.ItemId);
            Console.WriteLine($"   ✓ Prepared: {appRootItem.KeyName}");
        }

        // Step 3: Bulk save all new generic-app items (ONE call)
        Console.WriteLine($"\n📤 Step 3: Bulk saving {itemsToSave.Count} new generic-app items to UILM...");
        var (saveSuccess, saveMessage) = await BulkSaveAsync(itemsToSave);

        if (!saveSuccess)
        {
            Console.WriteLine($"❌ Save operation failed: {saveMessage}");
            return;
        }

        Console.WriteLine($"✅ Successfully saved {itemsToSave.Count} generic-app items\n");

        // Step 4: Bulk delete all old app-root items (ONE call)
        Console.WriteLine($"🗑️  Step 4: Bulk deleting {itemsToDelete.Count} old app-root items from UILM...");
        var (deleteSuccess, deleteMessage) = await BulkDeleteAsync(itemsToDelete);

        if (deleteSuccess)
        {
            Console.WriteLine($"✅ Successfully deleted {itemsToDelete.Count} app-root items");
        }
        else
        {
            Console.WriteLine($"⚠️  Delete operation result: {deleteMessage}");
        }

        Console.WriteLine($"\n✅ Filter A Cleanup Migration Completed!");
        Console.WriteLine($"   • Fetched: {appRootItemsData.Count} items");
        Console.WriteLine($"   • Saved: {itemsToSave.Count} new generic-app items");
        Console.WriteLine($"   • Deleted: {itemsToDelete.Count} old app-root items");
    }

    private static async Task<List<UilmKeyForBulkSave>> GetBatchItemsByIdsAsync(
        List<string> itemIds,
        string moduleId,
        string projectKey)
    {
        var results = new List<UilmKeyForBulkSave>();

        try
        {
            // ✅ Process in smaller batches to avoid timeouts (e.g., 20 at a time)
            int batchSize = 20;
            for (int i = 0; i < itemIds.Count; i += batchSize)
            {
                var batch = itemIds.Skip(i).Take(batchSize).ToList();
                Console.WriteLine($"   Processing batch {i / batchSize + 1}/{(itemIds.Count + batchSize - 1) / batchSize}...");

                foreach (var itemId in batch)
                {
                    try
                    {
                        using var client = new HttpClient();
                        client.BaseAddress = new Uri("https://api.seliseblocks.com");
                        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", BearerToken);
                        client.DefaultRequestHeaders.Accept.Clear();
                        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/plain"));
                        client.DefaultRequestHeaders.Add("x-blocks-key", BlocksKey);

                        var url = $"/uilm/v1/Key/Get?ItemId={itemId}&ProjectKey={projectKey}";
                        var response = await client.GetAsync(url);

                        if (response.IsSuccessStatusCode)
                        {
                            var responseContent = await response.Content.ReadAsStringAsync();

                            try
                            {
                                var data = JsonSerializer.Deserialize<UilmKeyForBulkSave>(
                                    responseContent,
                                    new JsonSerializerOptions
                                    {
                                        PropertyNameCaseInsensitive = true,
                                        DefaultBufferSize = 128
                                    }
                                );

                                if (data != null)
                                {
                                    results.Add(data);
                                    Console.WriteLine($"      ✓ {data.KeyName}");
                                }
                            }
                            catch (JsonException jsonEx)
                            {
                                Console.WriteLine($"      ⚠️  JSON parsing error for {itemId}: {jsonEx.Message}");
                                Console.WriteLine($"         Response: {responseContent.Substring(0, Math.Min(200, responseContent.Length))}...");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"      ✗ HTTP {response.StatusCode} for {itemId}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"      ✗ Error fetching {itemId}: {ex.Message}");
                    }

                    // Small delay between requests
                    await Task.Delay(50);
                }
            }

            Console.WriteLine($"✅ Successfully fetched {results.Count} items");
            return results;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error fetching batch items: {ex.Message}");
            return new List<UilmKeyForBulkSave>();
        }
    }


}