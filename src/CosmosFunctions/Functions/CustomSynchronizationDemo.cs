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
    public static class CustomSynchronizationDemo
    {
        private static CustomSynchronization customSynchronization = new CustomSynchronization();

        [FunctionName("CustomSynchronizationDemo")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            List<ResultData> results = null;
            try
            {
                await customSynchronization.Initialize(log);
                await Task.Delay(1000);
                results = await customSynchronization.RunDemo(log);
            }
            catch(Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }
            finally
            {
                await customSynchronization.CleanUp();
            }

            return new OkObjectResult(results);
        }
    }
}
