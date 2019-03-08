using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using CosmosGlobalDistribution;
using System.Collections.Generic;

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
            ExecutionContext context)
        {
            List<ResultData> results = null;
            try
            {
                await conflicts.Initialize(log, context.FunctionAppDirectory);
                initialized = true;
                await Task.Delay(1000);
                results = await conflicts.RunDemo(log);
            }
            catch(Exception ex)
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
