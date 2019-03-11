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
    public static class ConsistencyLatencyDemo
    {
        private static ConsistencyLatency consistencyLatency = new ConsistencyLatency();

        [FunctionName("ConsistencyLatencyDemo")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            [SignalR(HubName = "console", ConnectionStringSetting = "SIGNALR")] IAsyncCollector<SignalRMessage> signalRMessages)
        {
            SignalRLogger logger = new SignalRLogger(log, signalRMessages);
            List<ResultData> results = null;
            try
            {
                await consistencyLatency.Initialize(logger);
                await Task.Delay(1000);
                results = await consistencyLatency.RunDemo(logger);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Operation failed");
            }
            finally
            {
                await consistencyLatency.CleanUp();
            }

            return new OkObjectResult(results);
        }
    }
}
