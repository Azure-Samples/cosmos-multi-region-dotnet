using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace CosmosGlobalDistributionFunctions
{
    public class DemoState : TableEntity
    {
        public DemoState()
        {

        }

        public DemoState(string demo)
        {
            this.RowKey = demo;
            this.PartitionKey = "DemoState";
        }

        public DateTime LastExecution { get; set; }

        public bool Initialized { get; set; }
    }
}
