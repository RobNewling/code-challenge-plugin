using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Plugin;
using Ganss.IO;
using System.IO;
using CsvHelper;

namespace NaveegoGrpcPlugin
{
    public class PluginService : Plugin.Plugin.PluginBase
    {
        private readonly ILogger<PluginService> _logger;
        private List<Schema> discoveredSchemas;
        public PluginService(ILogger<PluginService> logger)
        {
            _logger = logger;
            discoveredSchemas = new List<Schema>();
        }

        public override Task<DiscoverResponse> Discover(DiscoverRequest request, ServerCallContext context)
        {
            var examine = request.Settings.FileGlob;
            LookForFiles(examine);
            return Task.FromResult(new DiscoverResponse { Schemas = { discoveredSchemas } });
        }

        private void LookForFiles(string fileGlob)
        {
            _logger.LogInformation("Looking for files in {glob}", fileGlob);
            try
            {

                var foundFiles = Glob.Expand(fileGlob);

                if (!foundFiles.Any())
                {
                    _logger.LogInformation("Found no file matches");
                }

                foreach (var file in foundFiles)
                {
                    _logger.LogInformation("Found {file}. Looking into it...", file.Name);
                    InvestigateFile(file.FullName);
                }

            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error looking for files in glob");
            }

        }

        private void InvestigateFile(string filePath)
        {
            try
            {
                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader))
                {
                    csv.Read();
                    csv.ReadHeader();

                    var headers = csv.Context.HeaderRecord;
                    var props = CreateProperties(headers);
                    var foundSchema = CheckForExistingSchema(props);
                    if (foundSchema != null)
                    {
                        AppendFileToSchema(foundSchema, filePath);
                    }
                    else
                    {
                        discoveredSchemas.Add(CreateSchema(filePath, props));
                    }


                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error opening csv");
            }


        }

        private Schema CheckForExistingSchema(List<Property> props)
        {

            foreach (var schema in discoveredSchemas)
            {
                var schemaArray = schema.Properties.ToArray();
                var propsArray = props.ToArray();
                if (schemaArray.Length == propsArray.Length && schemaArray.Intersect(propsArray).Count() == schemaArray.Length)
                {
                    _logger.LogInformation("Found same schema called {name}. ", schema.Name);
                    return schema;
                }
            }
            return null;
        }

        private void AppendFileToSchema(Schema schema, string filePath)
        {
            schema.Settings += ";" + filePath;
        }

        private Schema CreateSchema(string fileName, List<Property> props)
        {
            string schemaName = "Schema" + (discoveredSchemas.Count + 1);
            _logger.LogInformation("Creating schema called {name}. ", schemaName);
            return new Schema { Name = schemaName, Settings = fileName, Properties = { props } };
        }

        private List<Property> CreateProperties(string[] propsToCreate)
        {
            List<Property> newProps = new List<Property>();
            foreach (var header in propsToCreate)
            {
                newProps.Add(new Property { Name = header }); // not including Type yet
            }

            return newProps;
        }
    }
}