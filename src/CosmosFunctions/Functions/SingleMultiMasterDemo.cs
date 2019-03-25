using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using CosmosGlobalDistribution;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;

namespace CosmosGlobalDistributionFunctions
{
    public static class SingleMultiMasterDemo
    {
        private static SingleMultiMaster singleMultiMaster = new SingleMultiMaster();
        private static bool initialized = false;
        private static DateTime lastExecution = DateTime.MinValue;

        [FunctionName("SingleMultiMasterDemo")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context,
            [SignalR(HubName = "console", ConnectionStringSetting = "SIGNALR")] IAsyncCollector<SignalRMessage> signalRMessages)
        {
            SignalRLogger logger = new SignalRLogger(log, signalRMessages);
            List<ResultData> results = null;
            try
            {
                if (initialized)
                {
                    results = await singleMultiMaster.RunDemo(logger);
                    lastExecution = DateTime.UtcNow;
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }
            finally
            {
                await singleMultiMaster.CleanUp();
            }

            return new OkObjectResult(results);
        }

        [FunctionName("SingleMultiMasterDemoInitialize")]
        public static async Task<IActionResult> Initialize(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context,
            [SignalR(HubName = "console", ConnectionStringSetting = "SIGNALR")] IAsyncCollector<SignalRMessage> signalRMessages)
        {
            SignalRLogger logger = new SignalRLogger(log, signalRMessages);

            try
            {
                if (!initialized)
                {
                    await singleMultiMaster.Initialize(logger, context.FunctionAppDirectory);
                    await Task.Delay(1000);
                    await singleMultiMaster.LoadData(logger);
                    initialized = true;
                    lastExecution = DateTime.UtcNow;
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }

            return new OkObjectResult("Initialized");
        }

        [FunctionName("SingleMultiMasterDemoCleanUp")]
        public static async Task<IActionResult> CleanUp(
            [TimerTrigger("0 */5 * * * *")] TimerInfo timerInfo,
            ILogger log)
        {
            try
            {
                if (initialized && DateTime.UtcNow.Subtract(lastExecution).TotalMinutes > 120)
                {
                    await singleMultiMaster.CleanUp();
                    initialized = false;
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }

            return new OkObjectResult("Cleanup completed successfully.");
        }
    }
}
