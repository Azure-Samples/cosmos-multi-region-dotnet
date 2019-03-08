using System.Net;
using System.IO;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using System.Linq;
using Microsoft.Azure.WebJobs.Extensions.Http;
using System;
using MimeTypes;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Net.Http.Headers;

namespace CosmosGlobalDistributionFunctions
{
    public static class FileServer
    {
        const string staticFilesFolder = "www";
        static readonly string defaultPage =
            string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DEFAULT_PAGE")) ?
            "index.html" : Environment.GetEnvironmentVariable("DEFAULT_PAGE");

        /// <summary>
        /// This HttpTriggered function acts as static file server. 
        /// Based on https://github.com/anthonychu/azure-functions-static-file-server
        /// </summary>
        [FunctionName("FileServer")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req, ILogger log, ExecutionContext context)
        {
            var filePath = GetFilePath(req, log, context);

            var response = new HttpResponseMessage(HttpStatusCode.OK);
            var stream = new FileStream(filePath, FileMode.Open);
            response.Content = new StreamContent(stream);
            response.Content.Headers.ContentType = new MediaTypeHeaderValue(GetMimeType(filePath));
            return response;
        }

        private static string GetFilePath(HttpRequest req, ILogger log, ExecutionContext context)
        {
            var pathValue = req.Query
                .FirstOrDefault(q => string.Compare(q.Key, "file", true) == 0)
                .Value;

            var path = pathValue.FirstOrDefault() ?? "";

            var staticFilesPath = Path.GetFullPath(Path.Combine(context.FunctionAppDirectory, staticFilesFolder));
            var fullPath = Path.GetFullPath(Path.Combine(staticFilesPath, path));

            if (!IsInDirectory(staticFilesPath, fullPath))
            {
                throw new ArgumentException("Invalid path");
            }

            var isDirectory = Directory.Exists(fullPath);
            if (isDirectory)
            {
                fullPath = Path.Combine(fullPath, defaultPage);
            }

            return fullPath;
        }

        private static bool IsInDirectory(string parentPath, string childPath)
        {
            var parent = new DirectoryInfo(parentPath);
            var child = new DirectoryInfo(childPath);

            var dir = child;
            do
            {
                if (dir.FullName == parent.FullName)
                {
                    return true;
                }
                dir = dir.Parent;
            } while (dir != null);

            return false;
        }

        private static string GetMimeType(string filePath)
        {
            var fileInfo = new FileInfo(filePath);
            return MimeTypeMap.GetMimeType(fileInfo.Extension);
        }
    }
}
