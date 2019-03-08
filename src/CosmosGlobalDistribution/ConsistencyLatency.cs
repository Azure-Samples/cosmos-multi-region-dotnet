using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Diagnostics;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace CosmosGlobalDistribution
{

    /*
        * Resources needed for this demo:
        * 
        * 
        *   Eventual => Cosmos DB account: Replication: Single-Master, Write Region: West US 2, Read Region: Central US, Consistency: Eventual
        *   Strong 1K Miles => Cosmos DB account: Replication: Single-Master, Write Region: West US 2, Read Region: Central US, Consistency: Strong
        *   Strong 2K Miles => Cosmos DB account: Replication: Single-Master, Write Region: West US 2, Read Region: East US 2, Consistency: Strong
        *   
    */
    public class ConsistencyLatency
    {
        private string databaseName;
        private string containerName;
        private readonly Uri databaseUri;
        private readonly Uri containerUri;
        private readonly string PartitionKeyProperty = Environment.GetEnvironmentVariable("PartitionKeyProperty");
        private readonly string PartitionKeyValue = Environment.GetEnvironmentVariable("PartitionKeyValue");
        private DocumentClient clientEventual;      //West US 2 => Central US (1000 miles)
        private DocumentClient clientStrong1kMiles; //West US 2 => Central US (1000 miles)
        private DocumentClient clientStrong2kMiles; //West US 2 => East Us 2 (2000 miles)

        private Bogus.Faker<SampleCustomer> customerGenerator = new Bogus.Faker<SampleCustomer>().Rules((faker, customer) =>
            {
                customer.Id = Guid.NewGuid().ToString();
                customer.Name = faker.Name.FullName();
                customer.City = faker.Person.Address.City.ToString();
                customer.Region = faker.Person.Address.State.ToString();
                customer.PostalCode = faker.Person.Address.ZipCode.ToString();
                customer.MyPartitionKey = Environment.GetEnvironmentVariable("PartitionKeyValue");
                customer.UserDefinedId = faker.Random.Int(0, 1000);
            });

        public ConsistencyLatency()
        {
            string endpoint, key, region;

            databaseName = Environment.GetEnvironmentVariable("database");
            containerName = Environment.GetEnvironmentVariable("container");
            databaseUri = UriFactory.CreateDatabaseUri(databaseName);
            containerUri = UriFactory.CreateDocumentCollectionUri(databaseName, containerName);
            region = Environment.GetEnvironmentVariable("ConsistencyLatencyRegion");

            //Shared connection policy
            ConnectionPolicy policy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
            };
            policy.SetCurrentLocation(region);

            //Eventual consistency client
            endpoint = Environment.GetEnvironmentVariable("EventualEndpoint");
            key = Environment.GetEnvironmentVariable("EventualKey");
            clientEventual = new DocumentClient(new Uri(endpoint), key, policy, ConsistencyLevel.Eventual);
            clientEventual.OpenAsync().GetAwaiter().GetResult();

            //Strong consistency client 1K miles
            endpoint = Environment.GetEnvironmentVariable("Strong1kMilesEndpoint");
            key = Environment.GetEnvironmentVariable("Strong1kMilesKey");
            clientStrong1kMiles = new DocumentClient(new Uri(endpoint), key, policy, ConsistencyLevel.Strong);
            clientStrong1kMiles.OpenAsync().GetAwaiter().GetResult();

            //Strong consistency client 2K miles
            endpoint = Environment.GetEnvironmentVariable("Strong2kMilesEndpoint");
            key = Environment.GetEnvironmentVariable("Strong2kMilesKey");
            clientStrong2kMiles = new DocumentClient(new Uri(endpoint), key, policy, ConsistencyLevel.Strong);
            clientStrong2kMiles.OpenAsync().GetAwaiter().GetResult();
        }
        public async Task Initialize(ILogger logger)
        {
            try
            {
                logger.LogInformation("Consistency/Latency Initialize");
                //Database definition
                Database database = new Database { Id = databaseName };

                //Container definition
                PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                partitionKeyDefinition.Paths.Add(PartitionKeyProperty);
                DocumentCollection container = new DocumentCollection { Id = containerName, PartitionKey = partitionKeyDefinition };

                //create the database for all three accounts
                await clientEventual.CreateDatabaseIfNotExistsAsync(database);
                await clientStrong1kMiles.CreateDatabaseIfNotExistsAsync(database);
                await clientStrong2kMiles.CreateDatabaseIfNotExistsAsync(database);

                //Throughput - RUs
                RequestOptions options = new RequestOptions { OfferThroughput = 1000 };

                //Create the container for all three accounts
                await clientEventual.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
                await clientStrong1kMiles.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
                await clientStrong2kMiles.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
            }
            catch (DocumentClientException dcx)
            {
                logger.LogInformation(dcx.Message);
            }
        }
        public async Task<List<ResultData>> RunDemo(ILogger logger)
        {
            List<ResultData> results = new List<ResultData>();

            try
            {
                logger.LogInformation("Test Latency between Eventual Consistency vs Strong Consistency at 1000 and 2000 miles");

                results.AddRange(await WriteBenchmark(logger, clientEventual, "1000 miles"));
                results.AddRange(await WriteBenchmark(logger, clientStrong1kMiles, "1000 miles"));
                results.AddRange(await WriteBenchmark(logger, clientStrong2kMiles, "2000 miles"));

                logger.LogInformation("Summary");
                foreach (ResultData r in results)
                {
                    logger.LogInformation($"{r.Test}\tAvg Latency: {r.AvgLatency} ms\tAverage RU: {r.AvgRU}");
                }

                logger.LogInformation($"Test concluded.");
            }
            catch (DocumentClientException dcx)
            {
                logger.LogInformation(dcx.Message);
            }

            return results;
        }
        private async Task<List<ResultData>> WriteBenchmark(ILogger logger, DocumentClient client, string distance)
        {
            List<ResultData> results = new List<ResultData>();

            Stopwatch stopwatch = new Stopwatch();
            int i = 0;
            int total = 100;
            long lt = 0;
            double ru = 0;

            //Write tests for account with Eventual consistency
            string region = Helpers.ParseEndpoint(client.WriteEndpoint);
            string consistency = client.ConsistencyLevel.ToString();

            logger.LogInformation($"Test {total} writes account in {region} with {consistency} consistency level, and replica {distance} away.");

            for (i = 0; i < total; i++)
            {
                SampleCustomer customer = customerGenerator.Generate();
                stopwatch.Start();
                    ResourceResponse<Document> response = await client.CreateDocumentAsync(containerUri, customer);
                stopwatch.Stop();
                logger.LogInformation($"Write: Item {i} of {total}, Region: {region}, Latency: {stopwatch.ElapsedMilliseconds} ms, Request Charge: {response.RequestCharge} RUs");
                    lt += stopwatch.ElapsedMilliseconds;
                    ru += response.RequestCharge;
                stopwatch.Reset();
            }
            results.Add(new ResultData
                {
                    Test = $"Test with {consistency} Consistency",
                    AvgLatency = (lt / total).ToString(),
                    AvgRU = Math.Round(ru / total).ToString()
                });

            logger.LogInformation("Summary");
            logger.LogInformation($"Test 100 writes against account in {region} with {consistency} consistency level, with replica {distance} away");
            
            logger.LogInformation($"Average Latency:\t{(lt / total)} ms");
            logger.LogInformation($"Average Request Units:\t{Math.Round(ru / total)} RUs");

            return results;
        }
        public async Task CleanUp()
        {
            try
            { 
                await clientEventual.DeleteDatabaseAsync(databaseUri);
                await clientStrong1kMiles.DeleteDatabaseAsync(databaseUri);
                await clientStrong2kMiles.DeleteAttachmentAsync(databaseUri);
            }
            catch {}
        }
    }
}
