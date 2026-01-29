using DIZService.Core;
using System.Text.Json;

namespace DIZService.Tests
{
    public class WorkflowmanagerTestsSetup : IDisposable
    {
        public WorkflowmanagerTestsSetup()
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

    // Test-Klasse, die den Setup ausf�hrt
#pragma warning disable CS9113
    public class WorkflowmanagerTests(WorkflowmanagerTestsSetup setup) : IClassFixture<WorkflowmanagerTestsSetup>
#pragma warning restore CS9113
    {
        [Fact]
        public void ExistsWorkflow()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            int workflowID = 1;
            _ = new Workflow(workflowID, 1, 1, processor);

            Assert.True(processor.WorkflowManager.ExistsWorkflow(workflowID, processor._dummyTuple));
            Assert.False(processor.WorkflowManager.ExistsWorkflow(4, processor._dummyTuple));
        }

        /// <summary>
        /// Checks the funtions:
        ///     - GetScheduledWorkflows()
        ///     - GetInitializingWorkflows()
        ///     - GetExecutingWorkflows()
        ///     - GetFinishedWorkflows()
        ///     - GetFailedWorkflows()
        /// </summary>
        [Fact]
        public void GetWorkflowsInStages()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            Assert.Empty(processor.WorkflowManager.GetScheduledWorkflows());
            Assert.Empty(processor.WorkflowManager.GetInitializingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetExecutingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFinishedWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFailedWorkflows());

            Workflow w1 = new(1, 1, 1, processor);       // calls Create() and sets Workflow Scheduled
            Workflow w2 = new(2, 1, 1, processor);       // calls Create() and sets Workflow Scheduled

            Assert.Equal([w1, w2], processor.WorkflowManager.GetScheduledWorkflows());
            Assert.Empty(processor.WorkflowManager.GetInitializingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetExecutingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFinishedWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFailedWorkflows());

            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            Assert.Equal([w2], processor.WorkflowManager.GetScheduledWorkflows());
            Assert.Equal([w1], processor.WorkflowManager.GetInitializingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetExecutingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFinishedWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFailedWorkflows());

            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            Assert.Equal([w2], processor.WorkflowManager.GetScheduledWorkflows());
            Assert.Empty(processor.WorkflowManager.GetInitializingWorkflows());
            Assert.Equal([w1], processor.WorkflowManager.GetExecutingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFinishedWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFailedWorkflows());

            processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple);
            Assert.Equal([w2], processor.WorkflowManager.GetScheduledWorkflows());
            Assert.Empty(processor.WorkflowManager.GetInitializingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetExecutingWorkflows());
            Assert.Equal([w1], processor.WorkflowManager.GetFinishedWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFailedWorkflows());

            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w2, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w2, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowFailed(w2, processor._dummyTuple);
            Assert.Empty(processor.WorkflowManager.GetScheduledWorkflows());
            Assert.Empty(processor.WorkflowManager.GetInitializingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetExecutingWorkflows());
            Assert.Equal([w1], processor.WorkflowManager.GetFinishedWorkflows());
            Assert.Equal([w2], processor.WorkflowManager.GetFailedWorkflows());
        }

        [Fact]
        public void GetWorkflow()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            Assert.Empty(processor.WorkflowManager.GetZeitplanAusfuehrungenIDMapping());

            Workflow w1 = new(1, 1, 1, processor);       // calls Create() and sets Workflow Scheduled
            Workflow w2 = new(2, 2, 1, processor);       // calls Create() and sets Workflow Scheduled

            Assert.Equal(w1, processor.WorkflowManager.GetWorkflow(1, processor._dummyTuple));                           // get workflow
            Assert.Equal(w1, processor.WorkflowManager.GetWorkflow(WorkflowStage.Scheduled, 1, processor._dummyTuple));  // get workflow with stage

            Assert.Throws<ETLException>(() => processor.WorkflowManager.GetWorkflow(3, processor._dummyTuple));          // try get unknown workflow
            Assert.Throws<ETLException>(() => processor.WorkflowManager.GetWorkflow(WorkflowStage.Scheduled, 3, processor._dummyTuple));   // try get unknown workflow with stage

            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            Assert.Equal(w1, processor.WorkflowManager.GetWorkflow(1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.GetWorkflow(WorkflowStage.Scheduled, 1, processor._dummyTuple));  // try get workflow from wrong stage
            Assert.Equal(w1, processor.WorkflowManager.GetWorkflow(WorkflowStage.Initializing, 1, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            Assert.Equal(w1, processor.WorkflowManager.GetWorkflow(1, processor._dummyTuple));
            Assert.Equal(w1, processor.WorkflowManager.GetWorkflow(WorkflowStage.Executing, 1, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple);
            Assert.Equal(w1, processor.WorkflowManager.GetWorkflow(1, processor._dummyTuple));
            Assert.Equal(w1, processor.WorkflowManager.GetWorkflow(WorkflowStage.Finished, 1, processor._dummyTuple));

            // set workflow 2 to state failed
            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w2, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w2, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowFailed(w2, processor._dummyTuple);
            Assert.Equal(w2, processor.WorkflowManager.GetWorkflow(2, processor._dummyTuple));
            Assert.Equal(w2, processor.WorkflowManager.GetWorkflow(WorkflowStage.Failed, 2, processor._dummyTuple));
        }

        [Fact]
        public void NeutralizeWorkflow()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            Workflow w1 = new(1, 1, 1, processor); // calls Create() and sets Workflow Scheduled
            processor.WorkflowManager.NeutraliseWorkflow(w1, processor._dummyTuple);

            // check that workflow can not be found in any stage list
            Assert.Empty(processor.WorkflowManager.GetScheduledWorkflows());
            Assert.Empty(processor.WorkflowManager.GetInitializingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetExecutingWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFinishedWorkflows());
            Assert.Empty(processor.WorkflowManager.GetFailedWorkflows());

            // check that no mapping for zeitplanausfuehrung exists
            Assert.Empty(processor.WorkflowManager.GetZeitplanAusfuehrungenIDMapping());
            Assert.False(processor.WorkflowManager.ExistsWorkflow(1, processor._dummyTuple));
            Assert.False(processor.WorkflowManager.ExistsMapping(1, processor._dummyTuple));
        }

        /// <summary>
        /// Checks the functions:
        ///     - SetWorkflowInitializingToExecuting()
        ///     - SetWorkflowExecutingToFinished()
        ///     - SetWorkflowScheduled()
        ///     - SetWorkflowScheduledToInitializing()
        ///     - SetWorkflowFailed()
        ///     - NeutraliseWorkflow()
        /// </summary>
        [Fact]
        public void SetWorkflow()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            Workflow w1 = new(1, 1, 1, processor); // calls Create() and sets Workflow Scheduled

            // check that workflow gets not transfered to executing, finished or again scheduled
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowScheduled(w1, processor._dummyTuple));

            // check that workflow gets not transfered to scheduled, finished or again initializing
            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowScheduled(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.NeutraliseWorkflow(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.NeutraliseWorkflow(1, processor._dummyTuple));

            // check that workflow gets not transfered to scheduled, initializing or again to executing
            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowScheduled(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.NeutraliseWorkflow(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.NeutraliseWorkflow(1, processor._dummyTuple));

            // check that workflow gets not transfered to initializing, executing or again to finished
            processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple);
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.NeutraliseWorkflow(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.NeutraliseWorkflow(1, processor._dummyTuple));

            // check that workflow gets not transfered to initializing, executing or finished
            processor.WorkflowManager.SetWorkflowScheduled(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowFailed(w1, processor._dummyTuple);
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.NeutraliseWorkflow(w1, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.NeutraliseWorkflow(1, processor._dummyTuple));
        }

        [Fact]
        public void GetWorkflowStages()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            Assert.Equal(WorkflowStage.Unknown, processor.WorkflowManager.GetWorkflowStage(1, processor._dummyTuple));

            Workflow w1 = new(1, 1, 1, processor); // calls Create() and sets Workflow Scheduled

            Assert.Equal(WorkflowStage.Scheduled, processor.WorkflowManager.GetWorkflowStage(w1, processor._dummyTuple));
            Assert.Equal(WorkflowStage.Scheduled, processor.WorkflowManager.GetWorkflowStage(1, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            Assert.Equal(WorkflowStage.Initializing, processor.WorkflowManager.GetWorkflowStage(w1, processor._dummyTuple));
            Assert.Equal(WorkflowStage.Initializing, processor.WorkflowManager.GetWorkflowStage(1, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            Assert.Equal(WorkflowStage.Executing, processor.WorkflowManager.GetWorkflowStage(w1, processor._dummyTuple));
            Assert.Equal(WorkflowStage.Executing, processor.WorkflowManager.GetWorkflowStage(1, processor._dummyTuple));

            Assert.Equal(WorkflowStage.Unknown, processor.WorkflowManager.GetWorkflowStage(2, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowFailed(w1, processor._dummyTuple);
            Assert.Equal(WorkflowStage.Failed, processor.WorkflowManager.GetWorkflowStage(w1, processor._dummyTuple));
            Assert.Equal(WorkflowStage.Failed, processor.WorkflowManager.GetWorkflowStage(1, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowScheduled(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple);
            Assert.Equal(WorkflowStage.Finished, processor.WorkflowManager.GetWorkflowStage(w1, processor._dummyTuple));
            Assert.Equal(WorkflowStage.Finished, processor.WorkflowManager.GetWorkflowStage(1, processor._dummyTuple));
        }

        [Fact]
        public void IsWorkflowStages()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Unknown, 1, processor._dummyTuple));

            Workflow w1 = new(1, 1, 1, processor); // calls Create() and sets Workflow Scheduled

            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Scheduled, w1, processor._dummyTuple));
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Scheduled, 1, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Initializing, w1, processor._dummyTuple));
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Initializing, 1, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Executing, w1, processor._dummyTuple));
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Executing, 1, processor._dummyTuple));

            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Unknown, 2, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowFailed(w1, processor._dummyTuple);
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Failed, w1, processor._dummyTuple));
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Failed, 1, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowScheduled(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple);
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Finished, w1, processor._dummyTuple));
            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Finished, 1, processor._dummyTuple));
        }

        [Fact]
        public void WasExecutedOnce()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            Assert.True(processor.WorkflowManager.IsWorkflow(WorkflowStage.Unknown, 1, processor._dummyTuple));

            Workflow w1 = new(1, 1, 1, processor); // calls Create() and sets Workflow Scheduled

            Assert.False(processor.WorkflowManager.WasExecutedOnce(1, processor._dummyTuple));
            Assert.False(processor.WorkflowManager.WasExecutedOnce(2, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowFailed(w1, processor._dummyTuple);

            Assert.True(processor.WorkflowManager.WasExecutedOnce(1, processor._dummyTuple));
            Assert.False(processor.WorkflowManager.WasExecutedOnce(2, processor._dummyTuple));

            processor.WorkflowManager.SetWorkflowScheduled(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowScheduledToInitializing(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowInitializingToExecuting(w1, processor._dummyTuple);
            processor.WorkflowManager.SetWorkflowExecutingToFinished(w1, processor._dummyTuple);

            Assert.True(processor.WorkflowManager.WasExecutedOnce(1, processor._dummyTuple));
            Assert.False(processor.WorkflowManager.WasExecutedOnce(2, processor._dummyTuple));
        }

        /// <summary>
        /// checks the functions:
        ///     - AddMapping
        ///     - RemoveMapping
        ///     - GetZeitplanAusfuehrungenID
        /// </summary>
        [Fact]
        public void Mapping()
        {
            Processor processor = new("TEST", "TEST", false);
            processor.WorkflowManager = new WorkflowManager(processor, true);

            Workflow w1 = new(1, 1, 1, processor); // calls Create() and sets Workflow Scheduled

            Assert.Equal(new Dictionary<int, int> { { 1,1} }, processor.WorkflowManager.GetZeitplanAusfuehrungenIDMapping());

            processor.WorkflowManager.AddMapping(2, 1, processor._dummyTuple);
            Assert.Throws<ETLException>(() => processor.WorkflowManager.AddMapping(1, 2, processor._dummyTuple));

            Assert.Throws<ETLException>(() => processor.WorkflowManager.RemoveMapping(3, processor._dummyTuple));
            Assert.Throws<ETLException>(() => processor.WorkflowManager.GetZeitplanAusfuehrungenID(3, processor._dummyTuple));
            processor.WorkflowManager.RemoveMapping(1, processor._dummyTuple);
        }
    }
}
