using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace CosmosGlobalDistributionFunctions
{
    public static class DemoStateExtensions
    {
        public static async Task<DemoState> GetDemoStateAsync(this CloudTable cloudTable, string demo, bool checkIfExists = true)
        {
            if (checkIfExists)
            {
                await cloudTable.CreateIfNotExistsAsync();
            }

            TableOperation retrieveOperation = TableOperation.Retrieve<DemoState>("DemoState", demo);
            TableResult retrievedResult = await cloudTable.ExecuteAsync(retrieveOperation);
            if (retrievedResult.Result != null)
            {
                return (DemoState)retrievedResult.Result;
            }

            return new DemoState(demo);
        }

        public static async Task UpdateDemoState(this CloudTable cloudTable, DemoState state)
        {
            state.LastExecution = DateTime.UtcNow;
            TableOperation updateOperation = TableOperation.InsertOrReplace(state);
            TableResult retrievedResult = await cloudTable.ExecuteAsync(updateOperation);
        }
    }
}
