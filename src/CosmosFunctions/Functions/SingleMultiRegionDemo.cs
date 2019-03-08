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
    public static class SingleMultiRegionDemo
    {
        private static SingleMultiRegion singleMultiRegion = new SingleMultiRegion();

        [FunctionName("SingleMultiRegionDemo")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context)
        {
            List<ResultData> results = null;
            try
            {
                await singleMultiRegion.Initialize(log, context.FunctionAppDirectory);
                await Task.Delay(1000);
                await singleMultiRegion.LoadData(log);
                results = await singleMultiRegion.RunDemo(log);
            }
            catch(Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }
            finally
            {
                await singleMultiRegion.CleanUp();
            }

            return new OkObjectResult(results);
        }
    }
}
