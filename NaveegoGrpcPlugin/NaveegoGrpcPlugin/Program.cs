using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Events;

namespace NaveegoGrpcPlugin
{
    public class Program
    {
        public static int Main(string[] args)
        {

            Log.Logger = new LoggerConfiguration()
               .MinimumLevel.Information()
               .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
               .Enrich.FromLogContext()
               //.WriteTo.Console()
               .WriteTo.File("PluginLog.txt", rollingInterval: RollingInterval.Day)
               .CreateLogger();

            try
            {
                AppDomain.CurrentDomain.ProcessExit += ProcessExitHandler;
                var configuration = new ConfigurationBuilder()
                        .SetBasePath(Directory.GetCurrentDirectory())
                        .AddJsonFile("appsettings.json")
                        .Build();
                CreateHostBuilder(args, configuration).Build().Run();
                return 0;
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Host terminated unexpectedly");
                return 1;
            }
            finally
            {
                Log.CloseAndFlush();
            }

        }

        private static void ProcessExitHandler(object sender, EventArgs e)
        {
            Console.WriteLine("Shutting down");
        }

        // Additional configuration is required to successfully run gRPC on macOS.
        // For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682
        public static IHostBuilder CreateHostBuilder(string[] args, IConfiguration configuration) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureKestrel(options =>
                    {
                        var port = configuration.GetValue<int>("GrpcPort");
                        // Setup a HTTP/2 endpoint without TLS.
                        options.ListenLocalhost(port, o => o.Protocols =
                            HttpProtocols.Http2);
                    });
                    webBuilder.UseStartup<Startup>();
                    webBuilder.UseSerilog();
                });
    }
}
