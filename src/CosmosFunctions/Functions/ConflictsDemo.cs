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
    public static class ConflictsDemo
    {
        private static Conflicts conflicts = new Conflicts();
        private static bool initialized = false;
        private static DateTime lastExecution = DateTime.MinValue;

        [FunctionName("ConflictsDemo")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context,
            [SignalR(HubName = "console", ConnectionStringSetting = "SIGNALR")] IAsyncCollector<SignalRMessage> signalRMessages)
        {
            List<ResultData> results = null;
            SignalRLogger logger = new SignalRLogger(log, signalRMessages);
            try
            {
                if (initialized)
                {
                    results = await conflicts.RunDemo(logger);
                    lastExecution = DateTime.UtcNow;
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }

            return new OkObjectResult(results);
        }

        [FunctionName("ConflictsDemoInitialize")]
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
                    await conflicts.Initialize(logger, context.FunctionAppDirectory);
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

        [FunctionName("ConflictsDemoCleanUp")]
        public static async Task<IActionResult> CleanUp(
            [TimerTrigger("0 */5 * * * *")] TimerInfo timerInfo,
            ILogger log)
        {
            try
            {
                if (initialized && DateTime.UtcNow.Subtract(lastExecution).TotalMinutes > 120)
                {
                    await conflicts.CleanUp();
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
