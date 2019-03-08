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
        * Shared for all demos in this solution
        * - Functions running on App Service B3+ instance.
        * 
        *   Single Region => Cosmos DB account: Replication: Single-Master, Write Region: Southeast Asia, Consistency: Eventual
        *   Multi-Region => Cosmos DB account: Replication: Single-Master, Write Region: Southeast Asia, Read Region: West US 2, Consistency: Eventual
        *   
    */

    public class SingleMultiRegion
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

        public SingleMultiRegion()
        {
            string endpoint, key, region;
            
            databaseName = Environment.GetEnvironmentVariable("database");
            containerName = Environment.GetEnvironmentVariable("container");
            storedProcName = Environment.GetEnvironmentVariable("storedproc");
            databaseUri = UriFactory.CreateDatabaseUri(databaseName);
            containerUri = UriFactory.CreateDocumentCollectionUri(databaseName, containerName);
            storedProcUri = UriFactory.CreateStoredProcedureUri(databaseName, containerName, storedProcName);

            //Single-Region account client
            endpoint = Environment.GetEnvironmentVariable("SingleRegionEndpoint");
            key = Environment.GetEnvironmentVariable("SingleRegionKey");
            region = Environment.GetEnvironmentVariable("SingleRegionRegion");

            ConnectionPolicy policy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
            };

            policy.SetCurrentLocation(region);
            clientSingle = new DocumentClient(new Uri(endpoint), key, policy, ConsistencyLevel.Eventual);
            clientSingle.OpenAsync().GetAwaiter().GetResult();

            //Multi-Region account client
            endpoint = Environment.GetEnvironmentVariable("MultiRegionEndpoint");
            key = Environment.GetEnvironmentVariable("MultiRegionKey");
            region = Environment.GetEnvironmentVariable("MultiRegionRegion");

            policy.SetCurrentLocation(region);
            clientMulti = new DocumentClient(new Uri(endpoint), key, policy, ConsistencyLevel.Eventual);
            clientMulti.OpenAsync().GetAwaiter().GetResult();
        }

        public async Task Initialize(ILogger logger, string localPath)
        {
            try
            {
                logger.LogInformation("Single/Multi Region Initialize");
                //Database definition
                Database database = new Database { Id = databaseName };

                //Throughput - RUs
                RequestOptions options = new RequestOptions { OfferThroughput = 1000 };

                //Container definition
                PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                partitionKeyDefinition.Paths.Add(PartitionKeyProperty);
                DocumentCollection container = new DocumentCollection { Id = containerName, PartitionKey = partitionKeyDefinition };

                //Stored Procedure definition
                StoredProcedure spBulkUpload = new StoredProcedure
                {
                    Id = "spBulkUpload",
                    Body = File.ReadAllText(Path.Combine(localPath, @"spBulkUpload.js"))
                };

                //Single Region
                await clientSingle.CreateDatabaseIfNotExistsAsync(database);
                await clientSingle.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
                await clientSingle.CreateStoredProcedureAsync(containerUri, spBulkUpload);

                //Multi Region
                await clientMulti.CreateDatabaseIfNotExistsAsync(database);
                await clientMulti.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
                await clientMulti.CreateStoredProcedureAsync(containerUri, spBulkUpload);                
            }
            catch (DocumentClientException dcx)
            {
                logger.LogInformation(dcx.Message);
            }
        }

        public async Task LoadData(ILogger logger)
        {
            await Populate(logger, clientSingle);
            await Populate(logger, clientMulti);
        }

        private async Task Populate(ILogger logger, DocumentClient client)
        {
            List<SampleCustomer> sampleCustomers = customerGenerator.Generate(50);

            int inserted = 0;

            RequestOptions options = new RequestOptions
            {
                PartitionKey = new PartitionKey(PartitionKeyValue)
            };

            while(inserted < sampleCustomers.Count)
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
                logger.LogInformation("Test Read Latency between a Single Region Account vs Multi-Region Account");

                results.AddRange(await ReadBenchmark(logger, clientSingle, "Single-Region"));
                results.AddRange(await ReadBenchmark(logger, clientMulti, "Multi-Region"));

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

        private async Task<List<ResultData>> ReadBenchmark(ILogger logger, DocumentClient client, string replicaType)
        {
            List<ResultData> results = new List<ResultData>();

            Stopwatch stopwatch = new Stopwatch();

            string region = Helpers.ParseEndpoint(client.ReadEndpoint);

            FeedOptions feedOptions = new FeedOptions
            {
                PartitionKey = new PartitionKey(PartitionKeyValue)
            };
            string sql = "SELECT c._self FROM c";
            var items = client.CreateDocumentQuery(containerUri, sql, feedOptions).ToList();

            int i = 0;
            int total = items.Count;
            long lt = 0;
            double ru = 0;

            logger.LogInformation($"Test {total} reads against {replicaType} account in {region} from West US 2.");

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
                Test = $"Test {replicaType}",
                AvgLatency = (lt / total).ToString(),
                AvgRU = Math.Round(ru / total).ToString()
            });

            logger.LogInformation("Summary");
            logger.LogInformation($"Test {total} reads against {replicaType} account in {region}");

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
