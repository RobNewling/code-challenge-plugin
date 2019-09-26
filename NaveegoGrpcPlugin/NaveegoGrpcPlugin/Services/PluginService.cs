using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Plugin;

namespace NaveegoGrpcPlugin
{
    public class PluginService : Plugin.Plugin.PluginBase
    {
        private readonly ILogger<PluginService> _logger;
        public PluginService(ILogger<PluginService> logger)
        {
            _logger = logger;
        }

        public override Task<DiscoverResponse> Discover(DiscoverRequest request, ServerCallContext context)
        {
            var examine = request.Settings.FileGlob;
            return Task.FromResult(new DiscoverResponse { Schemas = { new Schema { Name = "Test" } } });
        }

        


    }
}
