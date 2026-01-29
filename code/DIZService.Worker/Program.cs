using Serilog;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DIZService.Worker
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Log.Information(args.Length > 0 ? "Argumente gefunden" : "Keine Argumente");

            string serviceName = args.Length > 0 ? args[0] : "DIZServiceBasic";
            string stage = args.Length > 1 ? args[1] : "ABC";

            var loggerConfig = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File($"logs/{serviceName}.log", rollingInterval: RollingInterval.Day);

            Log.Information($"Servicename: {serviceName} / Servicestage: {stage}");

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // Name des Event Logs und Source festlegen
                loggerConfig = loggerConfig.WriteTo.EventLog(
                    source: $"{serviceName}Source",
                    logName: $"{serviceName}Log",
                    manageEventSource: true // legt Source an, falls noch nicht vorhanden
                );
            }

            Log.Logger = loggerConfig.CreateLogger();

            var builder = Host.CreateApplicationBuilder(args);
            builder.Services.AddSingleton(new WorkerConfig { ServiceName = serviceName, Stage = stage });
            builder.Services.AddHostedService<Worker>();
            builder.Services.AddWindowsService(options =>
            {
                options.ServiceName = serviceName;
            });

            builder.Logging.ClearProviders();
            builder.Logging.AddConsole();           // plattformunabhängig
            builder.Logging.AddSerilog();
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                builder.Logging.AddEventLog();

            builder.Build().Run();
        }
    }

    public class WorkerConfig
    {
        public required string ServiceName { get; set; }
        public required string Stage { get; set; }
    }
}