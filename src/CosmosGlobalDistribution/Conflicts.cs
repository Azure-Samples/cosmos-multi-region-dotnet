using System;
using System.Collections.Generic;
using System.Net;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.IO;
using Microsoft.Extensions.Logging;

namespace CosmosGlobalDistribution
{

    /*
     * Resources needed for this demo:
     * 
     *   Shared with SingleMultiMaster.cs
     *   Multi-Master => Cosmos DB account: Replication: Multi-Master, Write Region: East US 2, West US 2, North Europe, Consistency: Session
     *   
    */

    public class Conflicts
    {
        private List<DocumentClient> clients;
        private string databaseName;
        private readonly Uri databaseUri;
        private string lwwContainerName;
        private readonly Uri lwwContainerUri;
        private string udpContainerName;
        private readonly Uri udpContainerUri;
        private string noneContainerName;
        private readonly Uri noneContainerUri;
        private readonly string PartitionKeyProperty = Environment.GetEnvironmentVariable("PartitionKeyProperty");
        private readonly string PartitionKeyValue = Environment.GetEnvironmentVariable("PartitionKeyValue");

        private Bogus.Faker<SampleCustomer> customerGenerator = new Bogus.Faker<SampleCustomer>().Rules((faker, customer) =>
        {
            customer.Id = Guid.NewGuid().ToString();
            customer.Name = faker.Name.FullName();
            customer.City = faker.Person.Address.City.ToString();
            customer.Region = faker.Person.Address.State.ToString(); //replaced by code below for inserts/updates
            customer.PostalCode = faker.Person.Address.ZipCode.ToString();
            customer.MyPartitionKey = Environment.GetEnvironmentVariable("PartitionKeyValue");
            customer.UserDefinedId = faker.Random.Int(0, 1000);
        });

        public Conflicts()
        {
            databaseName = Environment.GetEnvironmentVariable("database");
            lwwContainerName = Environment.GetEnvironmentVariable("LwwPolicyContainer");
            udpContainerName = Environment.GetEnvironmentVariable("UdpPolicyContainer");
            noneContainerName = Environment.GetEnvironmentVariable("NoPolicyContainer");

            databaseUri = UriFactory.CreateDatabaseUri(databaseName);
            lwwContainerUri = UriFactory.CreateDocumentCollectionUri(databaseName, lwwContainerName);
            udpContainerUri = UriFactory.CreateDocumentCollectionUri(databaseName, udpContainerName);
            noneContainerUri = UriFactory.CreateDocumentCollectionUri(databaseName, noneContainerName);

            string endpoint = Environment.GetEnvironmentVariable("MultiMasterEndpoint");
            string key = Environment.GetEnvironmentVariable("MultiMasterKey");
            List<string> regions = Environment.GetEnvironmentVariable("ConflictRegions").Split(';').ToList();

            clients = new List<DocumentClient>();
            foreach (string region in regions)
            {
                ConnectionPolicy policy = new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                    UseMultipleWriteLocations = true //Multiple write locations
                };
                policy.SetCurrentLocation(region);
                DocumentClient client = new DocumentClient(new Uri(endpoint), key, policy, ConsistencyLevel.Eventual);
                client.OpenAsync().GetAwaiter().GetResult();
                clients.Add(client);
            }
        }
        public async Task Initialize(ILogger logger, string localPath)
        {
            try
            {
                logger.LogInformation("MultiMaster Conflicts Initialize");

                //Database definition
                Database database = new Database { Id = databaseName };

                //create the database
                await clients[0].CreateDatabaseIfNotExistsAsync(database);

                //Shared Container properties
                PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
                partitionKeyDefinition.Paths.Add(PartitionKeyProperty);

                //Conflict Policy for Container using Last Writer Wins
                ConflictResolutionPolicy lwwPolicy = new ConflictResolutionPolicy
                {
                    Mode = ConflictResolutionMode.LastWriterWins,
                    ConflictResolutionPath = "/userDefinedId"
                };

                //Throughput - RUs
                RequestOptions options = new RequestOptions { OfferThroughput = 1000 };

                DocumentCollection containerLww = new DocumentCollection
                {
                    Id = lwwContainerName,
                    PartitionKey = partitionKeyDefinition,
                    ConflictResolutionPolicy = lwwPolicy
                };
                await clients[0].CreateDocumentCollectionIfNotExistsAsync(databaseUri, containerLww, options);

                string udpStoredProcName = "spConflictUDP";
                Uri spUri = UriFactory.CreateStoredProcedureUri(databaseName, udpContainerName, udpStoredProcName);

                //Conflict Policy for Container with User-Defined Stored Procedure
                ConflictResolutionPolicy udpPolicy = new ConflictResolutionPolicy
                {
                    Mode = ConflictResolutionMode.Custom,
                    ConflictResolutionProcedure = spUri.ToString()
                };

                DocumentCollection containerUdp = new DocumentCollection
                {
                    Id = udpContainerName,
                    PartitionKey = partitionKeyDefinition,
                    ConflictResolutionPolicy = udpPolicy
                };
                await clients[0].CreateDocumentCollectionIfNotExistsAsync(databaseUri, containerUdp, options);

                //Stored Procedure definition
                StoredProcedure spConflictUdp = new StoredProcedure
                {
                    Id = udpStoredProcName,
                    Body = File.ReadAllText(Path.Combine(localPath, $@"{udpStoredProcName}.js"))
                };

                //Create the Conflict Resolution stored procedure
                await clients[0].CreateStoredProcedureAsync(udpContainerUri, spConflictUdp);


                //Conflict Policy for Container with no Policy and writing to Conflicts Feed
                ConflictResolutionPolicy nonePolicy = new ConflictResolutionPolicy
                {
                    Mode = ConflictResolutionMode.Custom
                };

                DocumentCollection containerNone = new DocumentCollection
                {
                    Id = noneContainerName,
                    PartitionKey = partitionKeyDefinition,
                    ConflictResolutionPolicy = nonePolicy
                };
                await clients[0].CreateDocumentCollectionIfNotExistsAsync(databaseUri, containerNone, options);
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
                logger.LogInformation("Multi Master Conflict Resolution");
                results.AddRange(await GenerateInsertConflicts(logger, lwwContainerUri, "Generate insert conflicts on container with Last Writer Wins Policy (Max UserDefinedId Wins)."));
                results.AddRange(await GenerateInsertConflicts(logger, udpContainerUri, "Generate insert conflicts on container with User Defined Procedure Policy (Min UserDefinedId Wins)."));
                results.AddRange(await GenerateUpdateConflicts(logger, noneContainerUri, "Generate update conficts on container with no Policy defined, write to Conflicts Feed."));

                logger.LogInformation($"Test concluded.");
            }
            catch (DocumentClientException dcx)
            {
                logger.LogInformation(dcx.Message);
            }

            return results;
        }
        private async Task<List<ResultData>> GenerateInsertConflicts(ILogger logger, Uri collectionUri, string test)
        {
            List<ResultData> results = new List<ResultData>();

            try
            {
                bool isConflicts = false;

                logger.LogInformation($"{test}");

                while (!isConflicts)
                {
                    List<Task<SampleCustomer>> tasks = new List<Task<SampleCustomer>>();

                    SampleCustomer customer = customerGenerator.Generate();

                    foreach (DocumentClient client in clients)
                    {
                        tasks.Add(InsertItemAsync(logger, client, collectionUri, customer));
                    }

                    SampleCustomer[] insertedItems = await Task.WhenAll(tasks);

                    isConflicts = IsConflicts(logger, insertedItems);
                    if(isConflicts)
                    {
                        foreach (var conflict in insertedItems)
                        {
                            try
                            {
                                results.Add(new ResultData()
                                {
                                    Test = $"Generated Conflict in container {collectionUri} - Name: {conflict.Name}, City: {conflict.City}, UserDefId: {conflict.UserDefinedId}, Region: {conflict.Region}"
                                });
                            }
                            catch (Exception ex)
                            {

                                logger.LogError(ex, "test");
                            }
                        }
                    }
                }
            }
            catch (DocumentClientException dcx)
            {
                logger.LogInformation(dcx.Message);
            }

            return results;
        }
        private async Task<SampleCustomer> InsertItemAsync(ILogger logger, DocumentClient client, Uri collectionUri, SampleCustomer item)
        {
            //DeepCopy the item
            item = Helpers.Clone(item);

            //Update UserDefinedId for each item to random number for Conflict Resolution
            item.UserDefinedId = Helpers.RandomNext(0, 1000);
            //Update the write region to the client regions so we know which client wrote the item
            item.Region = Helpers.ParseEndpoint(client.WriteEndpoint);

            logger.LogInformation($"Attempting insert - Name: {item.Name}, City: {item.City}, UserDefId: {item.UserDefinedId}, Region: {item.Region}");

            try
            {
                var response =  await client.CreateDocumentAsync(collectionUri, item);
                return (SampleCustomer)(dynamic)response.Resource;
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    //Item has already replicated so return null
                    return null;
                }
                if (ex.StatusCode == System.Net.HttpStatusCode.Gone)
                {
                    return await InsertItemAsync(logger, client, collectionUri, item);
                }

                throw;
            }
        }
        private async Task<List<ResultData>> GenerateUpdateConflicts(ILogger logger, Uri collectionUri, string test)
        {
            List<ResultData> results = new List<ResultData>();

            try
            {
                bool isConflicts = false;

                logger.LogInformation($"{test}");

                logger.LogInformation($"Inserting an item to create an update conflict on.");

                //Generate a new customer, set the region property
                SampleCustomer customer = customerGenerator.Generate();

                SampleCustomer insertedItem = await InsertItemAsync(logger, clients[0], collectionUri, customer);

                logger.LogInformation($"Wait 2 seconds to allow item to replicate.");
                await Task.Delay(2000);

                RequestOptions requestOptions = new RequestOptions
                {
                    PartitionKey = new PartitionKey(PartitionKeyValue)
                };

                while (!isConflicts)
                {
                    IList<Task<SampleCustomer>> tasks = new List<Task<SampleCustomer>>();

                    SampleCustomer item = await clients[0].ReadDocumentAsync<SampleCustomer>(insertedItem.SelfLink, requestOptions);
                    logger.LogInformation($"Original - Name: {item.Name}, City: {item.City}, UserDefId: {item.UserDefinedId}, Region: {item.Region}");

                    foreach (DocumentClient client in clients)
                    {
                        tasks.Add(UpdateItemAsync(logger, client, collectionUri, item));
                    }

                    SampleCustomer[] updatedItems = await Task.WhenAll(tasks);

                    //Delay to allow data to replicate
                    await Task.Delay(2000);

                    isConflicts = IsConflicts(logger, updatedItems);

                    if (isConflicts)
                    {
                        foreach(var conflict in updatedItems)
                        {
                            try
                            {
                                results.Add(new ResultData()
                                {
                                    Test = $"Generated Conflict in container {collectionUri} - Name: {conflict.Name}, City: {conflict.City}, UserDefId: {conflict.UserDefinedId}, Region: {conflict.Region}"
                                });
                            }
                            catch (Exception ex)
                            {

                                logger.LogError(ex, "test");
                            }
                        }
                    }
                }
            }
            catch(DocumentClientException dcx)
            {
                logger.LogInformation(dcx.Message);
            }

            return results;
        }
        private async Task<SampleCustomer> UpdateItemAsync(ILogger logger, DocumentClient client, Uri collectionUri, SampleCustomer item)
        {
            //DeepCopy the item
            item = Helpers.Clone(item);

            //Make a change to the item to update.
            item.Region = Helpers.ParseEndpoint(client.WriteEndpoint);
            item.UserDefinedId = Helpers.RandomNext(0, 1000);

            logger.LogInformation($"Update - Name: {item.Name}, City: {item.City}, UserDefId: {item.UserDefinedId}, Region: {item.Region}");

            try
            {
                var response = await client.ReplaceDocumentAsync(item.SelfLink, item, new RequestOptions
                {
                    AccessCondition = new AccessCondition
                    {
                        Type = AccessConditionType.IfMatch,
                        Condition = item.ETag
                    }
                });
                return (SampleCustomer)(dynamic)response.Resource;
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode == HttpStatusCode.PreconditionFailed || ex.StatusCode == HttpStatusCode.NotFound)
                {
                    //No conflict is induced.
                    return null;
                }
                throw;
            }
        }
        private bool IsConflicts(ILogger logger, SampleCustomer[] items)
        {
            int operations = 0;
            //Non-null items are successful conflicts
            foreach (var item in items)
            {
                if (item != null)
                {
                    ++operations;
                }
            }

            if (operations > 1)
            {
                logger.LogInformation($"Conflicts generated. Confirm in Portal.");
                return true;
            }
            else
            {
                logger.LogInformation($"No conflicts generated. Retrying to induce conflicts");
            }
            return false;
        }
        public async Task CleanUp()
        {
            try
            {
                await clients[0].DeleteDatabaseAsync(databaseUri);
            }
            catch { }
        }
    }
}
