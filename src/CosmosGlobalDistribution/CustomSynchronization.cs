using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;

namespace CosmosGlobalDistribution
{
    /*
    * Resources needed for this demo:
    * 
    *   Custom => Cosmos DB account: Replication: Multi-Master, Write Region: West US 2, East US 2, West US, East US, Consistency: Session
    *   Strong => Cosmos DB account: Replication: Multi-Master, Write Region: West US 2, East US 2, West US, East US, Consistency: Strong
    *   
*/
    public class CustomSynchronization
    {
        private string databaseName;
        private string containerName;
        private Uri databaseUri;
        private Uri containerUri;
        private string PartitionKeyProperty = Environment.GetEnvironmentVariable("PartitionKeyProperty");
        private string PartitionKeyValue = Environment.GetEnvironmentVariable("PartitionKeyValue");
        private DocumentClient readClient;
        private DocumentClient writeClient;
        private DocumentClient strongClient;

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

        public CustomSynchronization()
        {
            string endpoint, key, writeRegion, readRegion;

            databaseName = Environment.GetEnvironmentVariable("database");
            containerName = Environment.GetEnvironmentVariable("container");
            databaseUri = UriFactory.CreateDatabaseUri(databaseName);
            containerUri = UriFactory.CreateDocumentCollectionUri(databaseName, containerName);
            writeRegion = Environment.GetEnvironmentVariable("WriteRegion");
            readRegion = Environment.GetEnvironmentVariable("readRegion");

            //Shared endpoint and key
            endpoint = Environment.GetEnvironmentVariable("CustomSyncEndpoint");
            key = Environment.GetEnvironmentVariable("CustomSyncKey");

            //Write client
            ConnectionPolicy writePolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                UseMultipleWriteLocations = true
            };
            writePolicy.SetCurrentLocation(writeRegion);
            writeClient = new DocumentClient(new Uri(endpoint), key, writePolicy);

            //Read client policy
            ConnectionPolicy readPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };
            readPolicy.SetCurrentLocation(readRegion);
            readClient = new DocumentClient(new Uri(endpoint), key, readPolicy, ConsistencyLevel.Session);

            //Strong consistency client
            ConnectionPolicy strongPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
            };
            strongPolicy.SetCurrentLocation(writeRegion);
            endpoint = Environment.GetEnvironmentVariable("StrongEndpoint");
            key = Environment.GetEnvironmentVariable("StrongKey");
            strongClient = new DocumentClient(new Uri(endpoint), key, strongPolicy, ConsistencyLevel.Strong);

            writeClient.OpenAsync().GetAwaiter().GetResult();
            readClient.OpenAsync().GetAwaiter().GetResult();
            strongClient.OpenAsync().GetAwaiter().GetResult();
        }

        public async Task Initialize(ILogger logger)
        {
            try
            {
                logger.LogInformation("Custom Synchronization Initialize");
                //Database definition
                Database database = new Database { Id = databaseName };

                //Container definition
                PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                partitionKeyDefinition.Paths.Add(PartitionKeyProperty);
                DocumentCollection container = new DocumentCollection { Id = containerName, PartitionKey = partitionKeyDefinition };

                //create the database for all accounts
                await writeClient.CreateDatabaseIfNotExistsAsync(database);
                await strongClient.CreateDatabaseIfNotExistsAsync(database);

                //Throughput - RUs
                RequestOptions options = new RequestOptions { OfferThroughput = 1000 };

                //Create the container for all accounts
                await writeClient.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
                await strongClient.CreateDocumentCollectionIfNotExistsAsync(databaseUri, container, options);
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
                logger.LogInformation("Test Latency between Strong Consistency all regions vs. single region");

                results.AddRange(await WriteBenchmarkStrong(logger, strongClient));
                results.AddRange(await WriteBenchmarkCustomSync(logger, writeClient, readClient));

                logger.LogInformation("All Tests Summary");
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

        private async Task<List<ResultData>> WriteBenchmarkStrong(ILogger logger, DocumentClient client)
        {
            List<ResultData> results = new List<ResultData>();
            Stopwatch stopwatch = new Stopwatch();

            int i = 0;
            int total = 100;
            long lt = 0;
            double ru = 0;

            string region = Helpers.ParseEndpoint(client.WriteEndpoint);
            string consistency = client.ConsistencyLevel.ToString();

            logger.LogInformation($"Test {total} writes account in {region} with {consistency} consistency between all replicas.");

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
                Test = $"Test {total} writes in {region} with {consistency} consistency between all replicas",
                AvgLatency = (lt / total).ToString(),
                AvgRU = Math.Round(ru / total).ToString()
            });

            logger.LogInformation("Test Summary");
            logger.LogInformation($"Test {total} writes account in {region} with {consistency} consistency between all replicas");
            
            logger.LogInformation($"Average Latency:\t{(lt / total)} ms");
            logger.LogInformation($"Average Request Units:\t{Math.Round(ru / total)} RUs");
            return results;
        }

        private async Task<List<ResultData>> WriteBenchmarkCustomSync(ILogger logger, DocumentClient writeClient, DocumentClient readClient)
        {
            List<ResultData> results = new List<ResultData>();
            Stopwatch stopwatch = new Stopwatch();

            int i = 0;
            int total = 100;
            long lt = 0;
            double ru = 0;
            long ltAgg = 0;
            double ruAgg = 0;

            string writeRegion = Helpers.ParseEndpoint(writeClient.WriteEndpoint);
            string readRegion = Helpers.ParseEndpoint(readClient.ReadEndpoint);
            string consistency = writeClient.ConsistencyLevel.ToString();

            logger.LogInformation($"Test {total} writes in {writeRegion} with {consistency} consistency between all replicas except {readRegion} with Strong consistency.");

            PartitionKey partitionKeyValue = new PartitionKey(PartitionKeyValue);

            for (i = 0; i < total; i++)
            {
                SampleCustomer customer = customerGenerator.Generate();

                stopwatch.Start();
                    ResourceResponse<Document> writeResponse = await writeClient.CreateDocumentAsync(containerUri, customer);
                stopwatch.Stop();
                        lt += stopwatch.ElapsedMilliseconds;
                        ru += writeResponse.RequestCharge;
                stopwatch.Reset();

                stopwatch.Start();
                    ResourceResponse<Document> readResponse = await readClient.ReadDocumentAsync(writeResponse.Resource.SelfLink, 
                        new RequestOptions { PartitionKey = partitionKeyValue, SessionToken = writeResponse.SessionToken});
                stopwatch.Stop();
                        lt += stopwatch.ElapsedMilliseconds;
                        ru += readResponse.RequestCharge;
                stopwatch.Reset();
                logger.LogInformation($"Write/Read: Item {i} of {total}, Region: {writeRegion}, Latency: {lt} ms, Request Charge: {ru} RUs");

                ltAgg += lt;
                ruAgg += ru;
                lt = 0;
                ru = 0;
            }
            results.Add(new ResultData
            {
                Test = $"Test {total} writes in {writeRegion} with {consistency} consistency between all replicas except {readRegion} with Strong consistency",
                AvgLatency = (ltAgg / total).ToString(),
                AvgRU = Math.Round(ruAgg / total).ToString()
            });

            logger.LogInformation("Test Summary");
            logger.LogInformation($"Test {total} writes in {writeRegion} with {consistency} consistency between all replicas except {readRegion} with Strong consistency");

            logger.LogInformation($"Average Latency:\t{(ltAgg / total)} ms");
            logger.LogInformation($"Average Request Units:\t{Math.Round(ruAgg / total)} RUs");

            return results;
        }

        public async Task CleanUp()
        {
            try
            {
                await writeClient.DeleteDatabaseAsync(databaseUri);
                await strongClient.DeleteDatabaseAsync(databaseUri);
            }
            catch { }
        }
    }
}
