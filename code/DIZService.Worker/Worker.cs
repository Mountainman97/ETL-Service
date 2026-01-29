using Serilog;
using DIZService.Core;
using System.Runtime.InteropServices;

namespace DIZService.Worker
{
    public class Worker(WorkerConfig config) : BackgroundService
    {
        private readonly WorkerConfig _config = config;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Log.Information(_config.ServiceName, 44);

            var loggerConfig = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File($"logs/{_config.ServiceName}.log", rollingInterval: RollingInterval.Day);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // name of eventlog and source
                loggerConfig = loggerConfig.WriteTo.EventLog(
                    source: $"{_config.ServiceName}Source",
                    logName: $"{_config.ServiceName}Log",
                    manageEventSource: true // creates source if not existing
                );
            }

            Log.Logger = loggerConfig.CreateLogger();

            // start initializing in background without blocking start of worker
            _ = Task.Run(() =>
            {
                try
                {
                    DBHelper helper = new(new Processor(_config.Stage, _config.ServiceName));
                    Helper h = new();

                    h.Log(
                        new Processor(_config.Stage, _config.ServiceName),
                        $"{_config.ServiceName} started!",
                        h._dummyTuple
                    );

                    string updateCMD = "UPDATE pc.ETL_Zeitplan_Ausfuehrungen " +
                                       "SET Ausgefuehrt = 1 " +
                                       "WHERE Ausgefuehrt = 0 ";

                    h.LogQuery(new Processor(_config.Stage, _config.ServiceName), updateCMD, -1, h._dummyTuple);
                    helper.ExecuteCommandDIZ(
                        new Processor(_config.Stage, _config.ServiceName), updateCMD, h._dummyTuple);
                }
                catch (Exception ex)
                {
                    Log.Error($"Init-Error: {ex}");
                }
            }, stoppingToken);

            try
            {
                // start processor once
                Processor processor = new(_config.Stage, _config.ServiceName, true);
                processor.StartProcessor();

                Log.Information($"{_config.ServiceName} started successfully");
            }
            catch (Exception ex)
            {
                Log.Error($"Error in Processor: {ex}");
            }

            // Worker stops itself, if token is not active
            await Task.CompletedTask;
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            Helper h = new();
            try
            {
                // set all Zeitplan_Ausfuehrungen to Ausgefuehrt = 1if not already 1
                string command = $"UPDATE pc.ETL_Zeitplan_Ausfuehrungen " +
                                 $"SET Ausgefuehrt = 1, " +
                                 $"Letzte_Aenderung = '{DateTime.Now:yyyy-MM-ddTHH:mm:ss.fff}', " +
                                 $"Letzte_Aenderung_Nutzer = suser_name() " +
                                 $"WHERE Ausgefuehrt = 0; ";

                DBHelper helper = new(new Processor(_config.Stage, _config.ServiceName));
                try
                {
                    helper.ExecuteCommandDIZ(
                        new Processor(_config.Stage, _config.ServiceName),
                        command,
                        new Tuple<int?, int?, int?, int?>(null, null, null, null)
                    );
                    Log.Information("Updating Zeitplan_Ausfuehrungen after Service Stop finished!");
                }
                catch
                {
                    Log.Information("Updating Zeitplan_Ausfuehrungen after Service Stop failed!");
                }

                Log.Information("ETL Service was stopped!");

                h.Log(
                    new Processor(_config.Stage, _config.ServiceName),
                    $"Stopping {_config.ServiceName}!",
                    h._dummyTuple
                );
            }
            catch (Exception e)
            {
                Log.Information($"Catched Exception while stopping service ({e})!");
                Environment.Exit(77);
            }

            // clean memory from unused objects
            Log.Information($"Total Memory before cleaning: {GC.GetTotalMemory(false)}");
            for (int i = 0; i <= GC.MaxGeneration; i++)
            {
                Log.Information($"Clean Generation: {i}");
                GC.Collect(i);
                Log.Information($"Total Memory: {GC.GetTotalMemory(false)}");
            }

            Log.Information($"Cleaned -> Service fully stopped!");
            h.Log(
                new Processor(_config.Stage, _config.ServiceName),
                $"{_config.ServiceName} fully Stopped!",
                h._dummyTuple
            );

            await base.StopAsync(cancellationToken);
        }
    }
}
