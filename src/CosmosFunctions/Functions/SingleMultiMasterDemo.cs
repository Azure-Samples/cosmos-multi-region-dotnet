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
                await singleMultiMaster.Initialize(logger, context.FunctionAppDirectory);
                await Task.Delay(1000);
                await singleMultiMaster.LoadData(logger);
                results = await singleMultiMaster.RunDemo(logger);
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
    }
}
