using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CosmosGlobalDistribution;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;

namespace CosmosGlobalDistributionFunctions
{
    public static class SingleMultiRegionDemo
    {
        private const string DemoName = "SingleMultiRegionDemo";
        private static SingleMultiRegion singleMultiRegion = new SingleMultiRegion();

        [FunctionName("SingleMultiRegionDemo")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context,
            [SignalR(HubName = "console", ConnectionStringSetting = "SIGNALR")] IAsyncCollector<SignalRMessage> signalRMessages,
            [Table("GlobalDistributionDemos")] CloudTable cloudTable)
        {
            SignalRLogger logger = new SignalRLogger(log, signalRMessages);
            List<ResultData> results = null;
            try
            {
                var state = await cloudTable.GetDemoStateAsync(DemoName, false);
                if (state.Initialized)
                {
                    results = await singleMultiRegion.RunDemo(logger);
                    await cloudTable.UpdateDemoState(state);
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }

            return new OkObjectResult(results);
        }

        [FunctionName("SingleMultiRegionDemoInitialize")]
        public static async Task<IActionResult> Initialize(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context,
            [SignalR(HubName = "console", ConnectionStringSetting = "SIGNALR")] IAsyncCollector<SignalRMessage> signalRMessages,
            [Table("GlobalDistributionDemos")] CloudTable cloudTable)
        {
            SignalRLogger logger = new SignalRLogger(log, signalRMessages);

            try
            {
                var state = await cloudTable.GetDemoStateAsync(DemoName);
                if (!state.Initialized)
                {
                    await singleMultiRegion.Initialize(logger, context.FunctionAppDirectory);
                    await Task.Delay(1000);
                    await singleMultiRegion.LoadData(logger);
                    state.Initialized = true;
                    await cloudTable.UpdateDemoState(state);
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }

            return new OkObjectResult("Initialized");
        }

        [FunctionName("SingleMultiRegionDemoCleanUp")]
        public static async Task CleanUp(
            [TimerTrigger("0 */5 * * * *")] TimerInfo timerInfo,
            [Table("GlobalDistributionDemos")] CloudTable cloudTable,
            ILogger log)
        {
            try
            {
                var state = await cloudTable.GetDemoStateAsync(DemoName);
                log.LogInformation($"{DemoName} is initialized {state.Initialized}");
                if (state.Initialized && DateTime.UtcNow.Subtract(state.LastExecution).TotalMinutes > 120)
                {
                    await singleMultiRegion.CleanUp();
                    state.Initialized = false;
                    await cloudTable.UpdateDemoState(state);
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }
        }
    }
}
