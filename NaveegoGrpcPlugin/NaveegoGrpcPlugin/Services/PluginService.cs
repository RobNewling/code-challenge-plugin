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
using System.ComponentModel;

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
            var examine = request.Settings.FileGlob;
            LookForFiles(examine);
            return Task.FromResult(new DiscoverResponse { Schemas = { discoveredSchemas } });
        }

        public override async Task Publish(PublishRequest request, IServerStreamWriter<PublishRecord> responseStream, ServerCallContext context)
        {
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
            catch (Exception e)
            {
                _logger.LogError(e, "Error looking for files in glob");
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
            string[] headers;
            List<Types> types = new List<Types>();
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader))
            {
                csv.Read();
                csv.ReadHeader();

                headers = csv.Context.HeaderRecord;
                
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

            List<Property> newProps = new List<Property>();
            foreach (var type in types)
            {
                int max = 0;
                string typeName = string.Empty;
                foreach (var myType in type.TypeVotes)
                {
                    if (myType.Value > max)
                    {
                        max = myType.Value;
                        typeName = type.TypeNameConvert(myType.Key);
                    }
                }
                newProps.Add(new Property { Name = type.ColumnName, Type = typeName }); 
            }

            return newProps;
        }


        private async Task GetDataToStream(string filePath, RepeatedField<Property> props, IServerStreamWriter<PublishRecord> responseStream)
        {
            try
            {
                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader))
                {

                    foreach (var record in csv.GetRecords<dynamic>())
                    {
                        var fullRecord = new List<dynamic>();
                        bool isInvalidRecord = false;
                        foreach (KeyValuePair<string, object> col in record)
                        {
                            string typeName = NameToTypeConvert(props.Where(w => w.Name == col.Key.ToString()).Select(s => s.Type).FirstOrDefault());

                            var canIt = TypeDescriptor.GetConverter(Type.GetType(typeName));
                            if (CanConvert(col.Value, Type.GetType(typeName)))
                            {
                                var convertedToType = Convert.ChangeType(col.Value, Type.GetType(typeName));
                                if (convertedToType == null)
                                    isInvalidRecord = true;
                                fullRecord.Add(convertedToType);
                            }
                            else
                            {
                                fullRecord.Add(col.Value.ToString());
                            }


                        }
                        var data = JsonSerializer.Serialize(fullRecord);
                        //var newPublishRecord = PrepareRecordForPublish(record, props);
                        await responseStream.WriteAsync(new PublishRecord { Data = data, Invalid = isInvalidRecord });
                    }

                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error opening csv");
            }
        }

        public PublishRecord PrepareRecordForPublish(dynamic record, RepeatedField<Property> props)
        {
            string data = string.Empty;
            bool isInvalidRecord = false;
            errorMsg = string.Empty;
            var fullRecord = new List<dynamic>();

            foreach (KeyValuePair<string, object> col in record)
            {
                string typeName = NameToTypeConvert(props.Where(w => w.Name == col.Key.ToString()).Select(s => s.Type).FirstOrDefault());
                if (CanConvert(col.Value, Type.GetType(typeName)))
                {
                    var convertedToType = Convert.ChangeType(col.Value, Type.GetType(typeName));
                    fullRecord.Add(convertedToType);
                }
                else
                {
                    isInvalidRecord = true;
                }
            }

            if (isInvalidRecord)
            {
                fullRecord = null;
            }

            data = JsonSerializer.Serialize(fullRecord);

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

    }
}
