using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ConsoleApp;

public static class UilmService
{
    private const string BaseUrl = "https://api.seliseblocks.com";
    private const string ProjectKey = "";
    private const string BlocksKey = "";

    private const string BearerToken = "";
    public static async Task CallGetApi(string itemId)
    {
        using var client = new HttpClient();
        client.BaseAddress = new Uri(BaseUrl);

        // Authorization Header
        client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", BearerToken);

        // Accept Header
        client.DefaultRequestHeaders.Accept.Add(
            new MediaTypeWithQualityHeaderValue("text/plain"));

        // Custom Header
        client.DefaultRequestHeaders.Add("x-blocks-key", BlocksKey);

        // Build URL
        var url = $"/uilm/v1/Key/Get?ItemId={itemId}&ProjectKey={ProjectKey}";

        var response = await client.GetAsync(url);
        var responseContent = await response.Content.ReadAsStringAsync();
        if (response.IsSuccessStatusCode)
        {
            
            try
            {
                var jsonDoc = JsonDocument.Parse(responseContent);
                var formattedJson = JsonSerializer.Serialize(
                    jsonDoc,
                    new JsonSerializerOptions { WriteIndented = true }
                );

                Console.WriteLine("Success Response (Formatted JSON):");
                Console.WriteLine(formattedJson);
            }
            catch
            {
                Console.WriteLine("Response is not valid JSON:");
                Console.WriteLine(responseContent);
            }
            Console.WriteLine("--------------------------");
            Console.WriteLine("--------------------------");
            var data = JsonSerializer.Deserialize<UilmKey>(
            responseContent,
            new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
            Console.WriteLine("Are you sure you want to delete the data? (Y/N)");
            var input = Console.ReadLine();
            if (input?.Trim().ToUpper() == "Y")
            {
                await SaveToMongo(data);
                await DeleteKeyAsync(data.ItemId, client);
            }
            else
            {
                Console.WriteLine("Operation cancelled.");
            }

        }
        else
        {
            Console.WriteLine($"Failed: {response.StatusCode}");
            Console.WriteLine(responseContent);
        }
    }

    public static async Task SaveToMongo(UilmKey data)
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("UILM_DB");
        var collection = database.GetCollection<UilmKey>("TranslationKeys");

        var filter = Builders<UilmKey>.Filter.Eq(x => x.ItemId, data.ItemId);

        await collection.ReplaceOneAsync(
            filter,
            data,
            new ReplaceOptions { IsUpsert = true });

        Console.WriteLine("Saved to MongoDB successfully ✅");
    }

    public static async Task DeleteKeyAsync(string itemId, HttpClient client)
    {

        // Accept header
        client.DefaultRequestHeaders.Accept.Add(
            new MediaTypeWithQualityHeaderValue("*/*"));


        var url = $"/uilm/v1/Key/Delete?ItemId={itemId}&ProjectKey={ProjectKey}";

        var response = await client.DeleteAsync(url);
        var content = await response.Content.ReadAsStringAsync();

        if (response.IsSuccessStatusCode)
        {
            Console.WriteLine("✅ Delete Successful");
            Console.WriteLine(content);
        }
        else
        {
            Console.WriteLine($"❌ Delete Failed: {response.StatusCode}");
            Console.WriteLine(content);
        }
    }
}
