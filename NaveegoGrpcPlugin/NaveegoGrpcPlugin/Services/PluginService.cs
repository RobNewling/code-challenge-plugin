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
using Google.Protobuf.Collections;
using System.Text.Json;
using System.Globalization;

namespace NaveegoGrpcPlugin
{
    public class PluginService : Plugin.Plugin.PluginBase
    {
        private readonly ILogger<PluginService> _logger;
        private List<Schema> discoveredSchemas;
        private string errorMsg;
        private readonly char delimiter;
        public PluginService(ILogger<PluginService> logger)
        {
            _logger = logger;
            delimiter = ';';
            discoveredSchemas = new List<Schema>();
        }

        public override Task<DiscoverResponse> Discover(DiscoverRequest request, ServerCallContext context)
        {
            _logger.LogInformation("Received request to discover schemas.");
            var examine = request.Settings.FileGlob;
            LookForFiles(examine);
            return Task.FromResult(new DiscoverResponse { Schemas = { discoveredSchemas } });

        }

        public override async Task Publish(PublishRequest request, IServerStreamWriter<PublishRecord> responseStream, ServerCallContext context)
        {
            _logger.LogInformation("Received request to publish records.");
            var filePaths = request.Schema.Settings.Split(delimiter);
            var props = request.Schema.Properties;
            foreach (var file in filePaths)
            {
                await GetDataToStream(file, props, responseStream);
            }
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
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error looking for files in glob");
            }

        }

        private void InvestigateFile(string filePath)
        {
            try
            {

                var props = CreateProperties(filePath);
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
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error investigating file.");
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
            schema.Settings += delimiter + filePath;
        }

        private Schema CreateSchema(string fileName, List<Property> props)
        {
            string schemaName = "Schema" + (discoveredSchemas.Count + 1);
            _logger.LogInformation("Creating schema called {name}. ", schemaName);
            return new Schema { Name = schemaName, Settings = fileName, Properties = { props } };
        }

        private List<Property> CreateProperties(string filePath)
        {
            List<Types> scannedColumns = ScanForTypes(filePath);

            List<Property> newProps = new List<Property>();

            foreach (var column in scannedColumns)
            {
                int max = 0;
                string typeName = string.Empty;
                foreach (var candidate in column.TypeVotes)
                {
                    if (candidate.Value > max)
                    {
                        max = candidate.Value;
                        typeName = column.TypeNameConvert(candidate.Key);
                    }
                }
                newProps.Add(new Property { Name = column.ColumnName, Type = typeName });
            }

            return newProps;
        }

        private List<Types> ScanForTypes(string filePath)
        {
            List<Types> types = new List<Types>();
            try
            {
                _logger.LogInformation("Scanning for types on {file}", filePath);

                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader))
                {
                    foreach (var record in csv.GetRecords<dynamic>())
                    {

                        foreach (KeyValuePair<string, object> col in record)
                        {
                            var colName = col.Key.ToString();
                            var field = col.Value.ToString();
                            if (types.Where(w => w.ColumnName == colName).Any())
                            {
                                var colType = types.Where(w => w.ColumnName == colName).First();
                                colType.DetectTypes(field);
                            }
                            else
                            {
                                types.Add(new Types(colName, field));
                            }

                        }

                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while scanning CSVs.");
            }

            return types;
        }

        private async Task GetDataToStream(string filePath, RepeatedField<Property> props, IServerStreamWriter<PublishRecord> responseStream)
        {
            try
            {
                _logger.LogInformation("Publishing records for {file}", filePath);
                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader))
                {

                    foreach (var record in csv.GetRecords<dynamic>())
                    {
                        var newPublishRecord = PrepareRecordForPublish(record, props);
                        await responseStream.WriteAsync(newPublishRecord); 
                    }

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error publishing records.");
            }
        }

        public PublishRecord PrepareRecordForPublish(dynamic record, RepeatedField<Property> props)
        {
            string data = string.Empty;
            bool isInvalidRecord = false;
            errorMsg = string.Empty;
            var fullRecord = new List<dynamic>();

            try
            {
                foreach (KeyValuePair<string, object> col in record)
                {
                    string typeName = NameToTypeConvert(props.Where(w => w.Name == col.Key.ToString()).Select(s => s.Type).FirstOrDefault());
                    if (CanConvert(col.Value, Type.GetType(typeName)))
                    {
                        var convertedToType = Convert.ChangeType(col.Value, Type.GetType(typeName));
                        if(convertedToType.GetType() == typeof(DateTime))
                        {
                            fullRecord.Add(ToRfc3339String((DateTime)convertedToType));
                        }
                        else
                        {
                            fullRecord.Add(convertedToType);
                        }
                    
                    }
                    else
                    {
                        fullRecord.Add(null);
                        isInvalidRecord = true;
                    }
                }

                data = JsonSerializer.Serialize(fullRecord);

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error preparing to publish record.");
            }

            return new PublishRecord { Data = data, Invalid = isInvalidRecord, Error = errorMsg };
        }

        public bool CanConvert(object objToCast, Type type)
        {
            try
            {
                Convert.ChangeType(objToCast, type);
                return true;
            }
            catch (Exception ex)
            {
                errorMsg = ex.Message;
                _logger.LogWarning(ex, "Could not convert to desired data type.");
                return false;
            }

        }

        public static string NameToTypeConvert(string name)
        {
            switch (name)
            {
                case "integer":
                    return "System.Int32";
                case "number":
                    return "System.Decimal";
                case "datetime":
                    return "System.DateTime";
                case "boolean":
                    return "System.Boolean";
                case "string":
                    return "System.String";
                default:
                    return "System.String";
            }
        }

        //source/adapted from https://sebnilsson.com/blog/c-datetime-to-rfc3339-iso-8601/
        public static string ToRfc3339String(DateTime dateTime)
        {
            return dateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffZ", DateTimeFormatInfo.InvariantInfo);
        }

    }
}
