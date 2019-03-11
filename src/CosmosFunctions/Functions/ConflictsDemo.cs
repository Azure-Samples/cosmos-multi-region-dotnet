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
                await conflicts.Initialize(logger, context.FunctionAppDirectory);
                initialized = true;
                await Task.Delay(1000);
                results = await conflicts.RunDemo(logger);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }

            return new OkObjectResult(results);
        }

        [FunctionName("ConflictsDemoCleanUp")]
        public static async Task<IActionResult> CleanUp(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                if (initialized)
                {
                    await conflicts.CleanUp();
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
