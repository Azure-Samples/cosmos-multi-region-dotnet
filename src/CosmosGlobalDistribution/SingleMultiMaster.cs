using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;

namespace CosmosGlobalDistribution
{

    /*
     * Resources needed for this demo:
     * 
     *   Single Master => Cosmos DB account: Replication: Single-Master, Write Region: East US 2, Read Region: West US 2, Consistency: Eventual
     *   Multi-Master => Cosmos DB account: Replication: Multi-Master, Write Region: East US 2, West US 2, North Europe, Consistency: Eventual
     *   
    */
    public class SingleMultiMaster
    {
        private string databaseName;
        private string containerName;
        private string storedProcName;
        private Uri databaseUri;
        private Uri containerUri;
        private Uri storedProcUri;
        private string PartitionKeyProperty = Environment.GetEnvironmentVariable("PartitionKeyProperty");
        private string PartitionKeyValue = Environment.GetEnvironmentVariable("PartitionKeyValue");
        private DocumentClient clientSingle;
        private DocumentClient clientMulti;

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

        public SingleMultiMaster()
        {
            string endpoint, key, region;

            databaseName = Environment.GetEnvironmentVariable("database");
            containerName = Environment.GetEnvironmentVariable("container");
            storedProcName = Environment.GetEnvironmentVariable("storedproc");
            databaseUri = UriFactory.CreateDatabaseUri(databaseName);
            containerUri = UriFactory.CreateDocumentCollectionUri(databaseName, containerName);
            storedProcUri = UriFactory.CreateStoredProcedureUri(databaseName, containerName, storedProcName);

            //Single-Master Connection Policy
            ConnectionPolicy policySingleMaster = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
            };
            region = Environment.GetEnvironmentVariable("SingleMultiMasterRegion");
            policySingleMaster.SetCurrentLocation(region);

            // Create the Single-Master account client
            endpoint = Environment.GetEnvironmentVariable("SingleMasterEndpoint");
            key = Environment.GetEnvironmentVariable("SingleMasterKey");
            clientSingle = new DocumentClient(new Uri(endpoint), key, policySingleMaster, ConsistencyLevel.Eventual);
            clientSingle.OpenAsync().GetAwaiter().GetResult();

            //Multi-Master Connection Policy
            ConnectionPolicy policyMultiMaster = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                UseMultipleWriteLocations = true //Required for Multi-Master
            };
            region = Environment.GetEnvironmentVariable("SingleMultiMasterRegion");
            policyMultiMaster.SetCurrentLocation(region); //Enable multi-homing

            // Create the Multi-Master account client
            endpoint = Environment.GetEnvironmentVariable("MultiMasterEndpoint");
            key = Environment.GetEnvironmentVariable("MultiMasterKey");
            clientMulti = new DocumentClient(new Uri(endpoint), key, policyMultiMaster, ConsistencyLevel.Eventual);
            clientMulti.OpenAsync().GetAwaiter().GetResult();
        }
        public async Task Initialize(ILogger logger, string localPath)
        {
            try
            { 
                logger.LogInformation("Single/Multi Master Initialize");
                //Database definition
                Database database = new Database { Id = databaseName };

                //Throughput - RUs
                RequestOptions options = new RequestOptions { OfferThroughput = 1000 };

                //Container properties
                PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                partitionKeyDefinition.Paths.Add(PartitionKeyProperty);

                //Container definition
                DocumentCollection container = new DocumentCollection { Id = containerName, PartitionKey = partitionKeyDefinition };

                //Stored Procedure definition
                StoredProcedure spBulkUpload = new StoredProcedure
                {
                    Id = "spBulkUpload",
                    Body = File.ReadAllText(Path.Combine(localPath, @"spBulkUpload.js"))
                };

                //Single-Master
                await clientSingle.CreateDatabaseIfNotExistsAsync(database);
                await clientSingle.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
                await clientSingle.CreateStoredProcedureAsync(containerUri, spBulkUpload);

                //Multi-Master (For multi-master, define DB throughput as there are 3 containers)
                await clientMulti.CreateDatabaseIfNotExistsAsync(database);
                await clientMulti.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
                await clientMulti.CreateStoredProcedureAsync(containerUri, spBulkUpload);
            }
            catch(DocumentClientException dcx)
            {
                logger.LogInformation(dcx.Message);
            }
        }
        public async Task LoadData(ILogger logger)
        {
            //Pre-Load data
            await Populate(logger, clientSingle);
            await Populate(logger, clientMulti);
        }

        private async Task Populate(ILogger logger, DocumentClient client)
        {
            List<SampleCustomer> sampleCustomers = customerGenerator.Generate(100);

            int inserted = 0;

            RequestOptions options = new RequestOptions
            {
                PartitionKey = new PartitionKey(PartitionKeyValue)
            };

            while (inserted < sampleCustomers.Count)
            {
                try
                {
                    StoredProcedureResponse<int> result = await client.ExecuteStoredProcedureAsync<int>(storedProcUri, options, sampleCustomers.Skip(inserted));
                    inserted += result.Response;
                    logger.LogInformation($"Inserted {inserted} items.");
                }
                catch (DocumentClientException ex) when (ex.StatusCode == System.Net.HttpStatusCode.Gone)
                {
                    ;
                }
            }
        }

        public async Task<List<ResultData>> RunDemo(ILogger logger)
        {
            List<ResultData> results = new List<ResultData>();

            try
            {
                logger.LogInformation($"Test read and write latency between a Single-Master and Multi-Master account");

                results.AddRange(await ReadBenchmark(logger, clientSingle, "Single-Master"));
                results.AddRange(await WriteBenchmark(logger, clientSingle, "Single-Master"));
                results.AddRange(await ReadBenchmark(logger, clientMulti, "Multi-Master"));
                results.AddRange(await WriteBenchmark(logger, clientMulti, "Multi-Master"));

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
        private async Task<List<ResultData>> ReadBenchmark(ILogger logger, DocumentClient client, string accountType)
        {
            List<ResultData> results = new List<ResultData>();
            string region = Helpers.ParseEndpoint(client.ReadEndpoint);
            Stopwatch stopwatch = new Stopwatch();

            FeedOptions feedOptions = new FeedOptions
            {
                PartitionKey = new PartitionKey(PartitionKeyValue)
            };
            string sql = "SELECT * FROM c";
            var items = client.CreateDocumentQuery(containerUri, sql, feedOptions).ToList();

            int i = 0;
            int total = items.Count;
            long lt = 0;
            double ru = 0;

            logger.LogInformation($"Test {total} reads against {accountType} account in {region}.");

            RequestOptions requestOptions = new RequestOptions
            {
                PartitionKey = new PartitionKey(PartitionKeyValue)
            };

            foreach (Document item in items)
            {
                stopwatch.Start();
                    ResourceResponse<Document> response = await client.ReadDocumentAsync(item.SelfLink, requestOptions);
                stopwatch.Stop();
                logger.LogInformation($"Read {i} of {total}, region: {region}, Latency: {stopwatch.ElapsedMilliseconds} ms, Request Charge: {response.RequestCharge} RUs");
                lt += stopwatch.ElapsedMilliseconds;
                ru += response.RequestCharge;
                i++;
                stopwatch.Reset();
            }

            results.Add(new ResultData
            {
                Test = $"Test reads against {accountType} account in {region}",
                AvgLatency = (lt / total).ToString(),
                AvgRU = Math.Round(ru / total).ToString()
            });
            
            logger.LogInformation("Summary");
            logger.LogInformation($"Test {total} reads against {accountType} account in {region}");

            logger.LogInformation($"Average Latency:\t{(lt / total)} ms");
            logger.LogInformation($"Average Request Units:\t{Math.Round(ru / total)} RUs");

            return results;
        }
        private async Task<List<ResultData>> WriteBenchmark(ILogger logger, DocumentClient client, string accountType)
        {
            List<ResultData> results = new List<ResultData>();

            string region = Helpers.ParseEndpoint(client.WriteEndpoint);
            Stopwatch stopwatch = new Stopwatch();

            int i = 0;
            int total = 100;
            long lt = 0;
            double ru = 0;

            logger.LogInformation($"Test {total} writes against {accountType} account in {region}.");

            for(i=0; i < total; i++)
            {
                SampleCustomer customer = customerGenerator.Generate();
                stopwatch.Start();
                    ResourceResponse<Document> response = await client.CreateDocumentAsync(containerUri, customer);
                stopwatch.Stop();
                logger.LogInformation($"Write {i} of {total}, to region: {region}, Latency: {stopwatch.ElapsedMilliseconds} ms, Request Charge: {response.RequestCharge} RUs");
                lt += stopwatch.ElapsedMilliseconds;
                ru += response.RequestCharge;
                stopwatch.Reset();
            }

            results.Add(new ResultData
            {
                Test = $"Test writes against {accountType} account in {region}",
                AvgLatency = (lt / total).ToString(),
                AvgRU = Math.Round(ru / total).ToString()
            });

            logger.LogInformation("Summary");
            logger.LogInformation($"Test {total} writes against {accountType} account in {region}");

            logger.LogInformation($"Average Latency:\t{(lt / total)} ms");
            logger.LogInformation($"Average Request Units:\t{Math.Round(ru / total)} RUs");

            return results;
        }
        public async Task CleanUp()
        {
            try
            {
                await clientSingle.DeleteDatabaseAsync(databaseUri);
                await clientMulti.DeleteDatabaseAsync(databaseUri);
            }
            catch { }
        }
    }
}
