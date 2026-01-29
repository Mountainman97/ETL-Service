using DIZService.Core;
using System.Text.Json;

namespace DIZService.Tests
{
    public class VisualizerTestsSetup : IDisposable
    {
        public VisualizerTestsSetup()
        {
            string json = File.ReadAllText("../../../config.json");
            Dictionary<string, string>? settings = JsonSerializer.Deserialize<Dictionary<string, string>>(json);

            Environment.SetEnvironmentVariable("TEST_datasource", (settings ?? throw new Exception("config.json does not exist"))["datasource"]);
            Environment.SetEnvironmentVariable("TEST_user", (settings ?? throw new Exception("config.json does not exist"))["user"]);
            Environment.SetEnvironmentVariable("TEST_pwd", (settings ?? throw new Exception("config.json does not exist"))["pwd"]);
            Environment.SetEnvironmentVariable("TEST_db", (settings ?? throw new Exception("config.json does not exist"))["db"]);
        }

        public void Dispose()
        {
            Environment.SetEnvironmentVariable("TEST_datasource", null);
            Environment.SetEnvironmentVariable("TEST_user", null);
            Environment.SetEnvironmentVariable("TEST_pwd", null);
            Environment.SetEnvironmentVariable("TEST_db", null);

            GC.SuppressFinalize(this);
        }
    }

    // Test-Klasse, die den Setup ausführt
#pragma warning disable CS9113
    public class VisualizerTests(VisualizerTestsSetup _) : IClassFixture<VisualizerTestsSetup>
#pragma warning restore CS9113
    {
        [Fact]
        public void Vizualize()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            int workflowID = 101;
            Workflow w = new(workflowID, 2304, 104, processor, 101);

            Vizualiser v = new(processor, 4798, w);

            v.Vizualize(new Tuple<int?, int?, int?, int?>(1, 1, 1, 1));
        }
    }
}
