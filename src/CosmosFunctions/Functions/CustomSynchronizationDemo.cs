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
    public static class CustomSynchronizationDemo
    {
        private const string DemoName = "CustomSynchronizationDemo";
        private static CustomSynchronization customSynchronization = new CustomSynchronization();

        [FunctionName("CustomSynchronizationDemo")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            [SignalR(HubName = "console", ConnectionStringSetting = "SIGNALR")] IAsyncCollector<SignalRMessage> signalRMessages,
            [Table("GlobalDistributionDemos")] CloudTable cloudTable)
        {
            SignalRLogger logger = new SignalRLogger(log, signalRMessages);
            try
            {
                var state = await cloudTable.GetDemoStateAsync(DemoName, false);
                if (state.Initialized)
                {
                    await customSynchronization.RunDemo(logger);
                    await cloudTable.UpdateDemoState(state);
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }

            return new OkResult();
        }

        [FunctionName("CustomSynchronizationDemoInitialize")]
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
                    await customSynchronization.Initialize(logger);
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

        [FunctionName("CustomSynchronizationDemoCleanUp")]
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
                    await customSynchronization.CleanUp();
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
