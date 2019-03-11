using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Extensions.Logging;

namespace CosmosGlobalDistributionFunctions
{
    internal class SignalRLogger : ILogger
    {
        private readonly ILogger logger;
        private readonly IAsyncCollector<SignalRMessage> signalRMessagesCollector;
        public SignalRLogger(ILogger logger, IAsyncCollector<SignalRMessage> signalRMessagesCollector)
        {
            this.logger = logger;
            this.signalRMessagesCollector = signalRMessagesCollector;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return this.logger.BeginScope<TState>(state);
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return this.logger.IsEnabled(logLevel);
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            this.logger.Log(logLevel, eventId, state, exception, formatter);
            this.signalRMessagesCollector.AddAsync(new SignalRMessage()
            {
                Target = "console",
                Arguments = new[] { formatter(state, exception) }
            }).GetAwaiter().GetResult();
        }
    }
}
