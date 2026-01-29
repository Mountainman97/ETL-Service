using System.Data;

using NodaTime;
using Microsoft.Data.SqlClient;

namespace DIZService.Core
{
    internal class Scheduler(Processor processor) : Helper
    {
        private readonly Processor _processor = processor ??
                        throw new ETLException("Missing Processor in Scheduler");  // Processor that steers workflows

        private readonly SemaphoreSlim _lockProcessingWorkflows = new(1, 1);
        private readonly List<int> _processingWorkflows = [];

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        /// <summary>
        /// Returns the workflows from DB in wanted format that are active and need to possibly started
        /// </summary>
        /// <returns>DataTable of workflows</returns>
        public DataTable GetWorkflows()
        {
            // receive all active workflows
            DataTable workflows;
            try
            {
                workflows = _processor.DbHelper.GetDataTableFromQuery(
                    _processor, "SELECT * FROM pc.ETL_WORKFLOW WHERE IST_AKTIV = 1;", _dummyTuple);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetWorkflows",
                    "Retrieving workflows failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // add all workflows that are not running or scheduled yet to scheduler (DB table)
            try
            {
                AddWorkflowsToScheduler(workflows);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflows",
                   "Adding Workflows to Scheduler failed!",
                   ref DummySem,
                   _dummyTuple
                );
            }

            // receive all inactive workflows
            DataTable disabledWorkflows;
            try
            {
                disabledWorkflows = _processor.DbHelper.GetDataTableFromQuery(
                   _processor, "SELECT * FROM pc.ETL_WORKFLOW WHERE IST_AKTIV = 0;", _dummyTuple);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflows",
                   "Retrieving disabled workflows failed!",
                   ref DummySem,
                   _dummyTuple
                );
            }

            // disable ZeitplanAusfuehrungen
            try
            {
                DisableScheduledWorkflows(disabledWorkflows);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflows",
                   "Disabling workflows failed!",
                   ref DummySem,
                   _dummyTuple
                );
            }

            // DataTable that includes the workflows in needed format
            DataTable schedule;
            try
            {
                schedule = GetAllWorkflowsToStart();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflows",
                   "Retreiving data table with workflows to start failed!",
                   ref DummySem,
                   _dummyTuple
                );
            }

            // sort workflows to execute the soonest workflow
            try
            {
                schedule.DefaultView.Sort = "Anforderungszeitpunkt ASC";
                schedule = schedule.DefaultView.ToTable();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflows",
                   "Sorting schedule failed!",
                   ref DummySem,
                   _dummyTuple
                );
            }

            return schedule;  // all workflows that are scheduled and not yet executed or executing
        }

        /// <summary>
        /// check for each disabled workflow if it is captured in any scheduled list and removes them from all lists
        /// to enusure that it will not be executed
        /// </summary>
        /// <param name="workflows">DataTable containing needed workflow informations</param>
        /// <exception cref="ETLException">in case of any error</exception>
        private void DisableScheduledWorkflows(DataTable workflows)
        {
            bool changed = false;
            try
            {
                foreach (DataRow workflow in workflows.Rows)
                {
                    int workflowID = int.Parse(workflow["ETL_Workflow_ID"].ToString() ??
                                                    throw new ETLException("No ETL_Workflow_ID"));

                    if (_processor.WorkflowManager.IsWorkflow(WorkflowStage.Scheduled, workflowID, _dummyTuple))
                    {
                        Workflow w = _processor.WorkflowManager.GetWorkflow(
                            WorkflowStage.Scheduled, workflowID, _dummyTuple);

                        if (_processor.WorkflowManager.ExistsMapping(workflowID, _dummyTuple) &&
                            _processor.WorkflowManager.GetWorkflowStage(w, _dummyTuple) == WorkflowStage.Scheduled)
                        {
                            Task.Run(() => UpdateZeitplanAusfuehrung(
                                _processor,
                                _processor.WorkflowManager.GetZeitplanAusfuehrungenID(workflowID, _dummyTuple),
                                [
                                    new Tuple<string, object>("Ausgefuehrt", true)
                                ],
                                _dummyTuple
                            )).Wait();

                            _processor.WorkflowManager.RemoveMapping(workflowID, _dummyTuple);
                            w.SignalizeRemovedMapping();
                            _processor.WorkflowManager.NeutraliseWorkflow(
                                w, new Tuple<int?, int?, int?, int?>(w.GetProzesslaeufeID(), null, null, null));
                            changed = true;
                        }
                    }

                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflows",
                   "Checking for disabling workflows failed!",
                   ref DummySem,
                   _dummyTuple
                );
            }

            if (changed)
                Task.Run(() => Log(
                    _processor,
                    $"Applied disabled workflows: executing: " +
                    $"{_processor.WorkflowManager.GetExecutingWorkflows().Count} | scheduled: " +
                    $"{_processor.WorkflowManager.GetScheduledWorkflows().Count}",
                    _dummyTuple
                )).Wait();
        }

        /// <summary>
        /// Reads workflow from DB and adds it to listings and DB Schedule
        /// </summary>
        /// <param name="workflows">DataRow that contains the workflow information</param>
        /// <exception cref="ETLException">in case of any error</exception>
        private void AddWorkflowsToScheduler(DataTable workflows)
        {
            try
            {
                int beforeExecuting = _processor.WorkflowManager.GetExecutingWorkflows().Count;
                int beforeScheduled = _processor.WorkflowManager.GetScheduledWorkflows().Count;

                if (_processor.Debug)
                {
                    // check if a log shall be written
                    if (beforeExecuting != _processor.WorkflowManager.GetExecutingWorkflows().Count ||
                        beforeScheduled != _processor.WorkflowManager.GetScheduledWorkflows().Count ||
                        (DateTime.Now - _processor.LastSchedulerMessage).TotalMinutes >=
                            _processor.MaxWaitWithoutMessage
                    )
                    {
                        _processor.LastSchedulerMessage = DateTime.Now;
                        Task.Run(() => Log(
                            _processor,
                            $"Before: executing: {_processor.WorkflowManager.GetExecutingWorkflows().Count} | " +
                            $"scheduled: {_processor.WorkflowManager.GetScheduledWorkflows().Count}",
                            _dummyTuple
                        )).Wait();
                    }
                }

                // check for execution of each workflow
                foreach (DataRow workflow in workflows.Rows)
                {
                    int workflowID = int.Parse(workflow["ETL_WORKFLOW_ID"].ToString() ??
                                                    throw new ETLException("No ETL_Workflow_ID"));

                    // check if no other iteration works with this workflow
                    _lockProcessingWorkflows.Wait();
                    _usedSem = _lockProcessingWorkflows;
                    if (!_processingWorkflows.Contains(workflowID))
                    {
                        _processingWorkflows.Add(workflowID);
                        _lockProcessingWorkflows.Release();
                        _usedSem = null;

                        // Check if a workflow-instance of this ID exists
                        if (_processor.WorkflowManager.ExistsWorkflow(workflowID, _dummyTuple))
                        {
                            WorkflowStage wStage = _processor.WorkflowManager.GetWorkflowStage(
                                workflowID, _dummyTuple);
                            // check state
                            if (wStage == WorkflowStage.Scheduled)
                            {
                                // check for new execution date
                                // receive new requestdate
                                DateTime newRequestDate = SetWorkflowInScheduler(workflow).Item2;
                                // receive old requestdate
                                DateTime prevRequestDate;
                                try
                                {
                                    int zeitplanAusfuehrungenID = _processor.WorkflowManager.GetZeitplanAusfuehrungenID(
                                        workflowID, _dummyTuple);
                                    DataTable existing = _processor.DbHelper.GetDataTableFromQuery(
                                        _processor,
                                        $"SELECT * FROM pc.ETL_Zeitplan_Ausfuehrungen " +
                                        $"WHERE ETL_Workflow_ID = {workflowID} AND" +
                                        $" ETL_Zeitplan_Ausfuehrungen_ID = {zeitplanAusfuehrungenID}" +
                                        $" AND Startzeitpunkt IS NULL AND Ausgefuehrt = 0",
                                        _dummyTuple
                                    );

                                    if (existing.Rows.Count != 0)
                                    {
                                        prevRequestDate = DateTime.Parse(
                                            existing.Rows[0]["Anforderungszeitpunkt"].ToString() ??
                                                throw new ETLException("No Anforderungszeitpunkt"));
                                    }
                                    else
                                    {
                                        prevRequestDate = newRequestDate;
                                    }
                                }
                                catch (Exception e)
                                {
                                    throw HandleErrorCatch(
                                        _processor,
                                        e,
                                        "AddWorkflowsToScheduler",
                                        "Extracting previous TimeplanExecutionID failed!",
                                        ref DummySem,
                                        _dummyTuple
                                    );
                                }
                                // compare old execution date with new one and optionally update
                                if (newRequestDate != prevRequestDate)
                                {
                                    int zeitplanAusfuehrungenID = _processor.WorkflowManager.GetZeitplanAusfuehrungenID(
                                        workflowID, _dummyTuple
                                    );
                                    WorkflowStage stage = _processor.WorkflowManager.GetWorkflowStage(
                                        workflowID, _dummyTuple
                                    );
                                    Task.Run(() => Log(
                                        _processor,
                                        $"Updating the request date from {prevRequestDate} to {newRequestDate} " +
                                        $"for Workflow {zeitplanAusfuehrungenID}",
                                        _dummyTuple
                                    )).Wait();
                                    Task.Run(() => Log(
                                       _processor,
                                       $"State of Workflow with ID {workflowID} is {stage}",
                                       _dummyTuple
                                    )).Wait();
                                    // Task.Delay(20).Wait();
                                    Task.Run(() => Log(
                                        _processor,
                                        $"executing: {_processor.WorkflowManager.GetExecutingWorkflows().Count} | " +
                                        $"scheduled: {_processor.WorkflowManager.GetScheduledWorkflows().Count}",
                                        _dummyTuple
                                    )).Wait();

                                    try
                                    {
                                        Task.Run(() => UpdateZeitplanAusfuehrung(
                                            _processor,
                                            _processor.WorkflowManager.GetZeitplanAusfuehrungenID(
                                                workflowID, _dummyTuple),
                                            [new Tuple<string, object>("Anforderungszeitpunkt", newRequestDate)],
                                            _dummyTuple
                                        )).Wait();
                                    }
                                    catch (Exception e)
                                    {
                                        throw HandleErrorCatch(
                                        _processor,
                                        e,
                                        "AddWorkflowsToScheduler",
                                        "Updating request date failed!",
                                        ref DummySem,
                                        _dummyTuple
                                    );
                                    }
                                }
                            }
                            else if (wStage == WorkflowStage.Initializing || wStage == WorkflowStage.Executing)
                            {
                                continue;  // skip
                            }
                            else if (wStage == WorkflowStage.Finished || wStage == WorkflowStage.Failed)
                            {
                                // get time informaton (timeplanID, requestDate and datasourceID)
                                Tuple<int, DateTime, int> timeInformation = SetWorkflowInScheduler(workflow);
                                // add workflow to Scheduler in DB
                                int timeplanExecutionID = -1;
                                try
                                {
                                    Task.Run(() => {
                                        timeplanExecutionID = AddZeitplanAusfuehrung(
                                            _processor,
                                            timeInformation.Item1,
                                            workflowID,
                                            int.Parse(workflow["ETL_PAKETE_ID"].ToString() ??
                                                throw new ETLException("No ETL_Pakete_ID")),
                                            timeInformation.Item2,
                                            timeInformation.Item3
                                        );
                                    }).Wait();
                                }
                                catch (Exception e)
                                {
                                    throw HandleErrorCatch(
                                        _processor,
                                        e,
                                        "AddWorkflowsToScheduler",
                                        "Adding Workflow in DB Scheduler failed!",
                                        ref DummySem,
                                        _dummyTuple
                                    );
                                }
                                // get workflow and run Create()
                                try
                                {
                                    string fallbackTmp = workflow["ETL_FALLBACK_PAKETE_ID"].ToString() ??
                                            throw new ETLException("No ETL_Fallback_Pakete_ID");
                                    if (fallbackTmp == "")
                                    {
                                        _processor.WorkflowManager.GetWorkflow(
                                            workflowID, _dummyTuple
                                        ).Create(timeplanExecutionID);
                                    } else
                                    {
                                        _processor.WorkflowManager.GetWorkflow(
                                            workflowID, _dummyTuple
                                        ).Create(timeplanExecutionID, fallbackPackageID: int.Parse(fallbackTmp));
                                    }

                                    // reset counter for failed creation
                                    _processor.ResetFailedCreatingWorkflowErrorcount(workflowID);
                                } catch (Exception e)
                                {
                                    Task.Run(() => Log(
                                        _processor, "Creating Workflow failed!", _dummyTuple
                                    )).Wait();
                                    _processor.AddFailedCreatingWorkflow(workflowID);

                                    throw HandleErrorCatch(
                                        _processor,
                                        e,
                                        "AddWorkflowsToScheduler",
                                        "Failed creating and adding workflow!",
                                        ref DummySem,
                                        _dummyTuple
                                    );
                                }
                            }
                        }
                        else  // workflow was not executed before
                        {
                            // create a new workflow (Workflow(...))
                            Task.Run(() => Log(
                                _processor, $"Create a new Workflow", _dummyTuple)).Wait();
                            // get time informaton (timeplanID, requestDate and datasourceID)
                            Tuple<int, DateTime, int> timeInformation = SetWorkflowInScheduler(workflow);
                            // add workflow to Scheduler in DB
                            int timeplanExecutionID = -1;
                            try
                            {
                                Task.Run(() => {
                                    timeplanExecutionID = AddZeitplanAusfuehrung(
                                        _processor,
                                        timeInformation.Item1,
                                        workflowID,
                                        int.Parse(workflow["ETL_PAKETE_ID"].ToString() ??
                                            throw new ETLException("No ETL_Pakete_ID")),
                                        timeInformation.Item2,
                                        timeInformation.Item3
                                    );
                                }).Wait();
                            }
                            catch (Exception e)
                            {
                                throw HandleErrorCatch(
                                    _processor,
                                    e,
                                    "AddWorkflowsToScheduler",
                                    "Adding Workflow in DB Scheduler failed!",
                                    ref DummySem,
                                    _dummyTuple
                                );
                            }
                            try
                            {
                                string fallbackTmp = workflow["ETL_FALLBACK_PAKETE_ID"].ToString() ??
                                                        throw new ETLException("No ETL_Fallback_Pakete_ID");

                                if (fallbackTmp == "")
                                {
                                    _ = new Workflow(
                                        workflowID,
                                        timeplanExecutionID,
                                        int.Parse(workflow["ETL_PAKETE_ID"].ToString() ??
                                            throw new ETLException("No ETL_Pakete_ID")),
                                        _processor
                                    );
                                } else
                                {
                                    _ = new Workflow(
                                        workflowID,
                                        timeplanExecutionID,
                                        int.Parse(workflow["ETL_PAKETE_ID"].ToString() ??
                                            throw new ETLException("No ETL_Pakete_ID")),
                                        _processor,
                                        int.Parse(workflow["ETL_FALLBACK_PAKETE_ID"].ToString() ??
                                            throw new ETLException("No ETL_Fallback_Pakete_ID"))
                                    );
                                }
                            } catch (Exception e)
                            {
                                Task.Run(() => Log(
                                    _processor, "Creating new Workflow failed!", _dummyTuple
                                )).Wait();
                                throw HandleErrorCatch(
                                    _processor,
                                    e,
                                    "AddWorkflowsToScheduler",
                                    "Creating new Workflow failed!",
                                    ref DummySem,
                                    _dummyTuple
                                );
                            }
                        }

                        _lockProcessingWorkflows.Wait();
                        _usedSem = _lockProcessingWorkflows;
                        _processingWorkflows.Remove(workflowID);
                        _lockProcessingWorkflows.Release();
                        _usedSem = null;
                    }
                    else
                    {
                        _lockProcessingWorkflows.Release();
                        _usedSem = null;
                    }
                }

                // check if a log shall be written because something has changed or cyclic logging
                if (beforeExecuting != _processor.WorkflowManager.GetExecutingWorkflows().Count ||
                    beforeScheduled != _processor.WorkflowManager.GetScheduledWorkflows().Count ||
                    (DateTime.Now - _processor.LastSchedulerMessage).TotalMinutes >= _processor.MaxWaitWithoutMessage)
                {
                    Task.Run(() => Log(
                        _processor,
                        $"executing: {_processor.WorkflowManager.GetExecutingWorkflows().Count} | " +
                        $"scheduled: {_processor.WorkflowManager.GetScheduledWorkflows().Count}",
                        _dummyTuple
                    )).Wait();
                    _processor.LastSchedulerMessage = DateTime.Now;
                }
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "AddWorkflowsToScheduler",
                   "Preparing workflows failed!",
                   ref tmp,
                   _dummyTuple
                );
            }
        }

        /// <summary>
        /// reads all workflows from DB that are scheduled and need to be started
        /// </summary>
        /// <returns>DataTable with all scheduled worfklows to start</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DataTable GetAllWorkflowsToStart()
        {
            DataTable schedule = new();
            // get all workflows that have no startzeitpunkt and theirfore are not started yet
            try
            {
                using SqlConnection connection = new(
                    _processor.DbHelper.BaseConnectionsString.ConnectionString);
                connection.Open();
                SqlDataAdapter adapter = new(
                    "SELECT * " +
                    "FROM pc.ETL_Zeitplan_Ausfuehrungen " +
                    "WHERE Startzeitpunkt IS NULL AND Ausgefuehrt = 0",
                    connection
                );
                adapter.Fill(schedule);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetAllWorkflowsToStart",
                   "Retrieving Schedule from DB failed!",
                   ref DummySem,
                   _dummyTuple
                );
            }

            return schedule;
        }

        /// <summary>
        /// receives the timeplan information of the given workflow, calculates the next execution date and returns the
        /// collected information as tuple
        /// </summary>
        /// <param name="workflow">workflow to get time for</param>
        /// <returns>ID of timeplan, next execution date and ID of datasource</returns>
        private Tuple<int, DateTime, int> SetWorkflowInScheduler(DataRow workflow)
        {
            // get workflow information
            int paketeID, timeplanID, workflowID, datasourceID;
            try
            {
                timeplanID = int.Parse(workflow["ETL_ZEITPLAENE_ID"].ToString() ??
                                        throw new ETLException("No ETL_ZEITPLAENE_ID"));
                workflowID = int.Parse(workflow["ETL_WORKFLOW_ID"].ToString() ??
                                        throw new ETLException("No ETL_WORKFLOW_ID"));
                paketeID = int.Parse(workflow["ETL_PAKETE_ID"].ToString() ??
                                        throw new ETLException("No ETL_Pakete_ID"));
                datasourceID = int.Parse(workflow["Datenherkunft_ID"].ToString() ??
                                            throw new ETLException("No Datenherkunft_ID"));
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflow",
                   "Extracting Workflow Information failed",
                   ref DummySem,
                   _dummyTuple
                );
            }

            // receive the timeplan information
            Tuple<DateTime, string> information;
            try
            {
                information = GetTimeplanInformation(timeplanID, workflowID);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflow",
                   "Receiving timplan information failed!",
                   ref DummySem,
                   _dummyTuple
                );
            }

            // get datetime of next execution
            DateTime requestDate;
            try
            {
                requestDate = GetExecTime(workflowID, information.Item1, information.Item2, DateTime.Now, timeplanID);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetWorkflow",
                    "Receiving execution time failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            return new Tuple<int, DateTime, int>(
                timeplanID, (DateTime) requestDate, datasourceID
            );
        }

        /// <summary>
        /// returns the starttime and name of timeplan interval for given workflow and timeplan
        /// </summary>
        /// <param name="timeplanID">ID of timeplan</param>
        /// <param name="workflowID">ID of workflow</param>
        /// <returns>Tuple(start time, name of interval)</returns>
        /// /// <exception cref="ETLException">in case of any error</exception>
        private Tuple<DateTime, string> GetTimeplanInformation(int timeplanID, int workflowID)
        {
            // get timeplan of workflow
            DataTable timeplans;
            try
            {
                timeplans = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * FROM pc.ETL_ZEITPLAENE WHERE ETL_ZEITPLAENE_ID = {timeplanID}",
                    _dummyTuple
                );
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetTimeplanInformation",
                    "Retreiving workflow timeplan failed",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // check uniqueness of timeplan
            if (timeplans.Rows.Count != 1)
                throw new ETLException(
                    _processor,
                    $"There is more than one fitting timeplan for Workflow {workflowID}",
                    "GetTimeplanInformation",
                    _dummyTuple
                );

            // get timeplan interval id
            string timplanIntervalID;
            DateTime start;
            try
            {
                timplanIntervalID = timeplans.Rows[0]["ZEITPLAN_INTERVALLE_ID"].ToString() ??
                                        throw new ETLException("No ZEITPLAN_INTERVALLE_ID");
                // start time of timeplan
                start = DateTime.Parse(
                    (timeplans.Rows[0]["ANFANGSDATUM"].ToString() ??
                        throw new ETLException("No ANFANGSDATUM")).Split(' ')[0] + " " +
                    timeplans.Rows[0]["STARTZEIT"].ToString()
                );
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetTimeplanInformation",
                    "Extracting timeplan information failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // get interval information
            DataTable timeplanIntervalls;
            try
            {
                timeplanIntervalls = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * FROM conf.ZEITPLAN_INTERVALLE WHERE ZEITPLAN_INTERVALLE_ID = " +
                    $"{timplanIntervalID}",
                    _dummyTuple
                );
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetTimeplanInformation",
                    "Extracting time interval information failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // check for uniqueness of timeplan intervalle
            if (timeplanIntervalls.Rows.Count != 1)
                throw new ETLException(
                    _processor,
                    $"There is more than one fitting timeplan interval for Workflow {workflowID}",
                    "GetTimeplanInformation",
                    _dummyTuple
                );

            return new Tuple<DateTime, string>(start, timeplanIntervalls.Rows[0]["ZEITPLAN_INTERVALL"].ToString() ??
                                                throw new ETLException("No ZEITPLAN_INTERVALL"));
        }

        /// <summary>
        /// checks if the given timeplan interval has a flag zu execute directly
        /// </summary>
        /// <param name="etlZeitplanIntervalleID">ID of timeplan interval</param>
        /// <returns>true if direct execution enabled. false otherwise</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private bool CheckDirectExecution(int etlZeitplanIntervalleID)
        {
            // get timeplan with intervalleID and look at sofort
            string query = $"SELECT * FROM pc.ETL_Zeitplaene " +
                           $"WHERE ETL_Zeitplaene_ID = {etlZeitplanIntervalleID}";

            // retrieve the timeplan from DB
            DataRow timeplan;
            try
            {
                timeplan = _processor.DbHelper.GetDataTableFromQuery(_processor, query, _dummyTuple).Rows[0];
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateNextManuelExecution",
                    "Retrieving the workflows Timeplan failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            if (bool.Parse(timeplan["Sofort_Ausfuehrung"].ToString() ??
                            throw new ETLException("No Sofort_Ausfuehrung")))
                return true;

            return false;
        }

        /// <summary>
        /// Calculate the next execution time based on interval and start time
        /// </summary>
        /// <param name="workflowID">ID of workflow to calculate the startTime</param>
        /// <param name="startTime">The initial startTime in DB</param>
        /// <param name="intervall">name of ETL_ZeitplanIntervalle</param>
        /// <param name="now">Referenztime to bring time up to date</param>
        /// <param name="intervalleID">ID of ETL_ZeitplanIntervalle</param>
        /// <returns>The DateTime of the next startTime of given Workflow (if error occurs null)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DateTime GetExecTime(
            int workflowID,
            DateTime startTime,
            string intervall,
            DateTime now,
            int intervalleID
        )
        {
            // check if workflow shall be started directly and wasnt started before
            bool direct = CheckDirectExecution(intervalleID);
            if (direct && !_processor.WorkflowManager.WasExecutedOnce(workflowID, _dummyTuple))
                return DateTime.Now;

            // calculate the starting difference of seconds to start
            double diff;
            try
            {
                diff = (now - startTime).TotalSeconds;  // seconds until workflow needs to be started
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetExecTime",
                    "Calculation of time to start Workflow failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // add time until start time lies in the future and was not executed yet
            try
            {
                switch (intervall)
                {
                    case "Manuell":
                        return CalulateNextManuelExecution(intervalleID, workflowID);
                    case "Minute":
                        while (diff > 0)
                        {
                            startTime = startTime.AddMinutes(1);
                            diff = (now - startTime).TotalSeconds;
                        }
                        return startTime;
                    case "DreiMinuten":
                        while (diff > 0)
                        {
                            startTime = startTime.AddMinutes(3);
                            diff = (now - startTime).TotalSeconds;
                        }
                        return startTime;
                    case "Viertelstuendlich":
                        while (diff > 0)
                        {
                            startTime = startTime.AddMinutes(15);
                            diff = (now - startTime).TotalSeconds;
                        }
                        return startTime;
                    case "Stunde":
                        while (diff > 0)
                        {
                            startTime = startTime.AddHours(1);
                            diff = (now - startTime).TotalSeconds;
                        }
                        return startTime;
                    case "Täglich":
                        while (diff > 0)
                        {
                            startTime = startTime.AddDays(1);
                            diff = (now - startTime).TotalSeconds;
                        }
                        return startTime;
                    case "Woche":
                        while (diff > 0)
                        {
                            startTime = startTime.AddDays(7);
                            diff = (now - startTime).TotalSeconds;
                        }
                        return startTime;
                    case "Monat":
                        while (diff > 0)
                        {
                            startTime = startTime.AddMonths(1);
                            diff = (now - startTime).TotalSeconds;
                        }
                        return startTime;
                    case "Jährlich":
                        while (diff > 0)
                        {
                            startTime = startTime.AddYears(1);
                            diff = (now - startTime).TotalSeconds;
                        }
                        return startTime;
                    default:
                        return startTime;
                }
            }
            catch (Exception e)
            {
                if (e is ETLException)
                    throw new ETLException("catched");

                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetExecTime",
                    "Executing the new start time failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }
        }

        /// <summary>
        /// extracts needed information from DB and starts calculation for the next execution
        /// </summary>
        /// <param name="etlZeitplaeneID">ID of timeplan</param>
        /// <param name="workflowID">ID of workflow</param>
        /// <returns>DateTime of the next execution</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DateTime CalulateNextManuelExecution(int etlZeitplaeneID, int workflowID)
        {
            string query = $"SELECT * FROM pc.ETL_Zeitplaene " +
                            $"WHERE ETL_Zeitplaene_ID = {etlZeitplaeneID}";

            // retrieve the timeplan from DB
            DataRow timeplan;
            try
            {
                timeplan = _processor.DbHelper.GetDataTableFromQuery(_processor, query, _dummyTuple).Rows[0];
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateNextManuelExecution",
                    "Retrieving the workflows Timeplan failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // read start and end date
            DateTime anfangsdatum = DateTime.MaxValue;
            DateTime endedatum = DateTime.MaxValue;
            LocalTime startZeit = new();
            try
            {
                anfangsdatum = DateTime.Parse(timeplan["Anfangsdatum"].ToString() ??
                                                throw new ETLException("No Anfangsdatum"));
                string time;
                time = timeplan["Startzeit"].ToString() ?? throw new ETLException("No Startzeit");
                startZeit = new LocalTime(
                    int.Parse(time.Split(':')[0]), int.Parse(time.Split(':')[1]), int.Parse(time.Split(':')[2])
                );
                endedatum = DateTime.Parse(timeplan["Endedatum"].ToString() ?? throw new ETLException("No Endedatum"));
            }
            catch (Exception e)
            {
                // if we did not got the needed anfangsdatum we return null
                if (anfangsdatum == DateTime.MaxValue)
                    throw new ETLException(
                        _processor,
                        "Retrieving start and end date failed!",
                        "CalculateNextManuelExecution",
                        e
                    );
            }

            // retrieve the number of repetition per day and week
            int dayRepetition, weekRepetition;
            try
            {
                dayRepetition = int.Parse(timeplan["Tageswiederholung"].ToString() ??
                                            throw new ETLException("No Tageswiederholung"));
                weekRepetition = int.Parse(timeplan["Wochenwiederholung"].ToString() ??
                                            throw new ETLException("No Wochenwiederholung"));
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateNextManuelExecution",
                    "Retrieving repetition numbers failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // retrieve information about last of month and weeknumber
            bool monthLast;
            int monthWeek;
            try
            {
                monthLast = bool.Parse(timeplan["Monatsletzter"].ToString() ??
                                        throw new ETLException("No Monatsletzter"));
                monthWeek = int.Parse(timeplan["Woche_des_Monats"].ToString() ??
                                        throw new ETLException("No Woche_des_Monats"));
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateNextManuelExecution",
                    "Retrieving monthLast and weekNumber failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // retrieve information about executing every day and month
            bool everyDay, everyMonth;
            try
            {
                everyDay = bool.Parse(timeplan["An_jedem_Tag"].ToString() ??
                                        throw new ETLException("No An_jedem_Tag"));
                everyMonth = bool.Parse(timeplan["In_jedem_Monat"].ToString() ??
                                            throw new ETLException("No In_jedem_Monat"));
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateNextManuelExecution",
                    "Retrieving every day and month execution failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            List<string> weekdaysFull = [
                "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
            ];
            List<string> wochentageFull = [
                "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"
            ];
            List<string> monthsFull = [
                "January", "February", "March", "April", "May", "June",
                "Juli", "August", "September", "October", "November", "December"
            ];
            List<string> monateFull = [
                "Januar", "Februar", "Maerz", "April", "Mai", "Juni",
                "Juli", "August", "September", "Oktober", "November", "Dezember"
            ];

            // extract the weekdays to execute on
            try
            {
                weekdaysFull = ExtractTimesToExecuteOn(everyDay, wochentageFull, weekdaysFull, timeplan);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateNextManuelExecution",
                    $"Extracting the days to execute on failed! ({e.Message})",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // extract the months to execute in
            try
            {
                monthsFull = ExtractTimesToExecuteOn(everyMonth, monateFull, monthsFull, timeplan);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateNextManuelExecution",
                    $"Extracting the months to execute on failed! ({e.Message})",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // calculate the next execution date
            try
            {
                DateTime nextExecutionDate = GetNextExecutionDate(
                    workflowID,
                    DateTime.Now,
                    anfangsdatum,
                    endedatum,
                    startZeit,
                    weekdaysFull,
                    monthsFull,
                    monthLast,
                    dayRepetition,
                    weekRepetition,
                    monthWeek
                );

                // _processor.WorkflowManager.NeutraliseWorkflow(workflowID, s_dummyTuple);

                return nextExecutionDate;
            }
            catch (Exception e)
            {
                if (e is ETLException)
                    throw new ETLException("catched");

                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateNextManuelExecution",
                    "Extracting the next execution date failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }
        }

        /// <summary>
        /// returns list of months or weekdays to execute on as list of strings
        /// </summary>
        /// <param name="everytime">signalizes that the whole list can be returned</param>
        /// <param name="zeitenFull">list with all possible times (days/months) in german</param>
        /// <param name="timesFull">list with all possible times (days/months) in english</param>
        /// <param name="timeplan">DataRow including all timeplan information</param>
        /// <returns>list of months or weekdays to execute on</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private List<string> ExtractTimesToExecuteOn(
            bool everytime,
            List<string> zeitenFull,
            List<string> timesFull,
            DataRow timeplan
        )
        {
            try
            {
                if (!everytime)
                {
                    List<string> timesFullBak = new(timesFull);
                    foreach (string zeit in zeitenFull)
                    {
                        bool set = bool.Parse(timeplan[zeit].ToString() ?? throw new ETLException("No Zeit"));
                        if (!set)
                        {
                            LogMessageLocal(
                                processor,
                                $"Remove: {timesFullBak[zeitenFull.IndexOf(zeit)]} (Len: {timesFullBak.Count}) " +
                                $"({zeit})",
                                _dummyTuple
                            );
                            timesFull.Remove(timesFullBak[zeitenFull.IndexOf(zeit)]);
                        }
                    }

                    if (timesFull.Count == 0)
                        throw new ETLException(
                            "No times for execution were given, while executing everytime was not set!"
                        );
                }

                return timesFull;
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ExtractTimesToExecuteOn",
                    "Failed extracting time to execute on",
                    ref DummySem,
                    _dummyTuple
                );
            }
        }

        /// <summary>
        /// handles any optional case and determines the next execution date
        /// </summary>
        /// <param name="workflowID">ID of workflow</param>
        /// <param name="now">DateTime when next execution was requested</param>
        /// <param name="startDate">Anfangsdatum in DB (earliest execution at this date)</param>
        /// <param name="endDate">Endedatum in DB (no execution after this date)</param>
        /// <param name="startZeit">given start time in DB of timeplan</param>
        /// <param name="weekdays">List of weekday names to execute only</param>
        /// <param name="months">List of month names to execute only</param>
        /// <param name="monthLast">set to true when executing on month last day</param>
        /// <param name="dayRepetition">number of repetitions per day (0 when weekRepetitions != 0)</param>
        /// <param name="weekRepetition">number of repetitions per week (0 when dayRepetitions != 0)</param>
        /// <param name="monthWeek">number of week to only execute in</param>
        /// <returns>the DateTime of the next execution</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DateTime GetNextExecutionDate(
            int workflowID,
            DateTime now,
            DateTime startDate,
            DateTime? endDate,
            LocalTime startZeit,
            List<string> weekdays,
            List<string> months,
            bool monthLast,
            int dayRepetition,
            int weekRepetition,
            int monthWeek
        )
        {
            List<string> monthsFull = [
                "January", "February", "March", "April", "May", "June",
                "Juli", "August", "September", "October", "November", "December"
            ];

            // check endDate < now?
            if (endDate != DateTime.MaxValue)
            {
                if (endDate < now)
                    throw new ETLException(
                        _processor,
                        $"Workflow has no execution left! ({workflowID})",
                        "GetNextExecutionDate",
                        _dummyTuple
                    );
            }

            DateTime lastOfMonth;
            try
            {
                lastOfMonth = GetLastOfMonth(monthLast, now, startDate, monthsFull, months, weekdays);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetNextExecutionDate",
                    "Retrieving Last day of Month failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            int timeBetweenExecutions;
            List<Tuple<string, LocalTime>> weekRepMapping = [];
            if (dayRepetition > 0)
            {
                try
                {
                    timeBetweenExecutions = CalculateTimeBetweenExecutions(dayRepetition, 1);
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "GetNextExecutionDate",
                        "Daily repetition time between executions could not be calculated!",
                        ref DummySem,
                        _dummyTuple
                    );
                }
            }
            else
            {
                if (weekRepetition > 0)
                {
                    // get time between executions
                    try
                    {
                        timeBetweenExecutions = CalculateTimeBetweenExecutions(weekRepetition, weekdays.Count);
                    }
                    catch (Exception e)
                    {
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "GetNextExecutionDate",
                            "Calculating time between executions failed!",
                            ref DummySem,
                            _dummyTuple
                        );
                    }

                    // maap execution times to week days
                    try
                    {
                        weekRepMapping = GetWeekRepetitionMapping(
                            startDate, weekRepetition, timeBetweenExecutions, weekdays
                        );
                    }
                    catch (Exception e)
                    {
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "GetNextExecutionDate",
                            "Calculating the weekly execution time failed!",
                            ref DummySem,
                            _dummyTuple
                        );
                    }
                }
                else
                {
                    timeBetweenExecutions = 24 * 60;
                }
            }

            // find next month and year to execute in
            int nextExecYear;
            string nextExecMonth;
            try
            {
                Tuple<int, string> yearMonthToExecute = GetNextYearMonthToExecute(now, monthsFull, months);
                nextExecYear = yearMonthToExecute.Item1;
                nextExecMonth = yearMonthToExecute.Item2;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetNextExecutionDate",
                    "Failed detecting next month and year to execute for!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // if we only want to execute in special week of month
            DateTime firstDateOfExecutionInWeek = DateTime.MaxValue;
            DateTime nextTheoreticalStart = DateTime.MaxValue;
            if (monthWeek > 0)
            {
                try
                {
                    firstDateOfExecutionInWeek = GetFirstDateofExecutionInWeek(
                        monthWeek,
                        now,
                        startDate,
                        weekdays,
                        months,
                        monthsFull,
                        nextExecYear,
                        nextExecMonth
                    );
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "GetNextExecutionDate",
                        "Retrieving first day of execution in given week failed!",
                        ref DummySem,
                        _dummyTuple
                    );
                }
            }
            else
            {
                try
                {
                    nextTheoreticalStart = GetNextTheoreticalStart(
                        monthsFull,
                        nextExecMonth,
                        now,
                        startZeit,
                        nextExecYear,
                        weekRepetition,
                        weekRepMapping,
                        months,
                        weekdays,
                        timeBetweenExecutions
                    );
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "GetNextExecutionDate",
                        "Failed retrieving the next theoretical start",
                        ref DummySem,
                        _dummyTuple
                    );
                }
            }

            // list all created next executions (if available), sort and select nearest date
            try
            {
                List<DateTime> nextExecutions = [
                    lastOfMonth, firstDateOfExecutionInWeek, nextTheoreticalStart
                ];
                nextExecutions.Sort();
                DateTime nextExecution = nextExecutions.First();

                return nextExecution;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetNextExecutionDate",
                    "Sorting and determining the next execution from calculated dates failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }
        }

        /// <summary>
        /// calculates the next possible execution date in case of no special use-cases
        /// </summary>
        /// <param name="monthsFull">full list of months in english</param>
        /// <param name="nextExecMonth">name of next month to execute in</param>
        /// <param name="now">DateTime of request to calculate</param>
        /// <param name="startTime">given start time in DB of timeplan</param>
        /// <param name="nextExecYear">the year of the next possible execution</param>
        /// <param name="weekRepetition">number of repetition per week</param>
        /// <param name="weekRepMapping">mapping at what times in week an execution can take place</param>
        /// <param name="months">list of months to execute on in english</param>
        /// <param name="weekdays">list of weekdays to execute on in english</param>
        /// <param name="timeBetweenExecutions">number of minutes between each execution</param>
        /// <returns>next DateTime that fits the given constraints</returns>
        /// <exception cref="ETLException">in case of any errors</exception>
        private DateTime GetNextTheoreticalStart(
            List<string> monthsFull,
            string nextExecMonth,
            DateTime now,
            LocalTime startTime,
            int nextExecYear,
            int weekRepetition,
            List<Tuple<string, LocalTime>> weekRepMapping,
            List<string> months,
            List<string> weekdays,
            int timeBetweenExecutions
        )
        {
            DateTime nextTheoreticalStart = DateTime.MaxValue;

            // get first execution date
            string day;
            try
            {
                day = monthsFull.IndexOf(nextExecMonth) + 1 > now.Month ? "01" : now.Day.ToString();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetNextTheoreticalStart",
                    "Determining the first execution day failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // build the first starting date
            try
            {
                nextTheoreticalStart = DateTime.Parse(
                    $"{nextExecYear}-{monthsFull.IndexOf(nextExecMonth) + 1}-{day}T{startTime}"
                );
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetNextTheoreticalStart",
                    "Parsing date failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            // add days to start if no execution weekday was calculated
            try
            {
                if (weekRepetition > 0)
                {
                    string weekDay = nextTheoreticalStart.DayOfWeek.ToString();
                    while (weekRepMapping.FindIndex(s => s.Item1 == weekDay) == -1)
                    {
                        nextTheoreticalStart = nextTheoreticalStart.AddDays(1);
                        weekDay = nextTheoreticalStart.DayOfWeek.ToString();
                    }

                    nextTheoreticalStart = DateTime.Parse(
                        $"{nextTheoreticalStart.Year}-{nextTheoreticalStart.Month}-{nextTheoreticalStart.Day}T" +
                        $"{weekRepMapping[weekRepMapping.FindIndex(s => s.Item1 == weekDay)].Item2}.000"
                    );
                }

                while (nextTheoreticalStart < now ||
                       !weekdays.Contains(nextTheoreticalStart.DayOfWeek.ToString()) ||
                       !months.Contains(monthsFull[nextTheoreticalStart.Month - 1]))
                {
                    // add minutes based on chosen repetition
                    nextTheoreticalStart = nextTheoreticalStart.AddMinutes(timeBetweenExecutions);
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetNextTheoreticalStart",
                    "Adding days to next theoretical start failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            string debugS = "";
            foreach (var x in weekRepMapping)
            {
                debugS += $"{x.Item1}: {x.Item2}";
            }
            LogMessageLocal(processor, debugS, _dummyTuple);

            return nextTheoreticalStart;
        }

        /// <summary>
        /// Calculate the first possible execution date in a given week and iterate thru months if needed
        /// </summary>
        /// <param name="monthWeek">number of week to execute in month (1-6)</param>
        /// <param name="now">DateTime of request to calculate</param>
        /// <param name="startDate">DateTime of first possible start</param>
        /// <param name="weekdays">list of weekdays allowed to execute on in english</param>
        /// <param name="months">list of months allowed to execute in in english</param>
        /// <param name="monthsFull">full list of months in english</param>
        /// <param name="nextExecYear">next possible year to execute in</param>
        /// <param name="nextExecMonth">next possible month to execute in</param>
        /// <returns>the next possible execution date in given week of month</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DateTime GetFirstDateofExecutionInWeek(
            int monthWeek,
            DateTime now,
            DateTime startDate,
            List<string> weekdays,
            List<string> months,
            List<string> monthsFull,
            int nextExecYear,
            string nextExecMonth
        )
        {
            DateTime firstDateOfExecutionInWeek = DateTime.MaxValue;
            bool success = false;  // set to true if the next execution date was found

            try
            {
                while (firstDateOfExecutionInWeek == DateTime.MaxValue ||
                       firstDateOfExecutionInWeek < now ||
                       !months.Contains(monthsFull[firstDateOfExecutionInWeek.Month - 1])
                )
                {
                    if (months.Contains(monthsFull[monthsFull.IndexOf(nextExecMonth)]))
                    {
                        // get dates of this week in given month
                        List<DateTime> weekDates = GetWeekDates(
                            nextExecYear, monthsFull.IndexOf(nextExecMonth) + 1, monthWeek);
                        success = GetNextFirstDateInWeek(
                            now,
                            weekDates,
                            months,
                            monthsFull,
                            weekdays,
                            startDate,
                            out firstDateOfExecutionInWeek
                        );
                    }

                    if (success)
                    {
                        break;
                    }
                    else
                    {
                        try
                        {
                            firstDateOfExecutionInWeek = DateTime.MaxValue;
                            if (monthsFull.IndexOf(nextExecMonth) + 1 >= 12)
                                nextExecYear++;

                            nextExecMonth = monthsFull[(monthsFull.IndexOf(nextExecMonth) + 1) % 12];
                        }
                        catch (Exception e)
                        {
                            throw HandleErrorCatch(
                                _processor,
                                e,
                                "GetFirstDateofExecutionInWeek",
                                "Setting next observation month and year failed!",
                                ref DummySem,
                                _dummyTuple
                            );
                        }
                    }

                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetFirstDateofExecutionInWeek",
                    "Determining the first date in week failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            return firstDateOfExecutionInWeek;
        }

        /// <summary>
        /// determines the first possible execution date in given week and returns
        /// boolean that signalizes if found or not
        /// </summary>
        /// <param name="now">DateTime of request to calculate</param>
        /// <param name="weekDates">DateTimes in a given week to search in</param>
        /// <param name="months">list of allowed months to execute in</param>
        /// <param name="monthsFull">full list of months</param>
        /// <param name="weekdays">list of allowed weekdays to execute on</param>
        /// <param name="startDate">first time to start the workflow</param>
        /// <param name="firstDateOfExecutionInWeek">output Date of next execution in week</param>
        /// <returns>true for successfull found, false otherwise</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private bool GetNextFirstDateInWeek(
            DateTime now,
            List<DateTime> weekDates,
            List<string> months,
            List<string> monthsFull,
            List<string> weekdays,
            DateTime startDate,
            out DateTime firstDateOfExecutionInWeek
        )
        {
            try
            {
                foreach (DateTime weekDate in weekDates)
                {
                    // find first day in selected week to execute
                    firstDateOfExecutionInWeek = weekDate;

                    if (firstDateOfExecutionInWeek >= now &&
                        months.Contains(monthsFull[firstDateOfExecutionInWeek.Month - 1]) &&
                        weekdays.Contains(firstDateOfExecutionInWeek.DayOfWeek.ToString()))
                    {
                        // set start time from start date
                        TimeSpan startTime = new(
                            0, startDate.Hour, startDate.Minute, startDate.Second, startDate.Millisecond);
                        firstDateOfExecutionInWeek = firstDateOfExecutionInWeek.Add(startTime);
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetFirstDateofExecutionInWeek",
                    "Determining the the next first date in selected week failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            firstDateOfExecutionInWeek = DateTime.MaxValue;
            return false;  // no success
        }

        /// <summary>
        /// determines the year and month to execute based on requested time and allowed months
        /// </summary>
        /// <param name="now">DateTime of request to calculate</param>
        /// <param name="monthsFull">list with all months</param>
        /// <param name="months">list with allowed months to execute in</param>
        /// <returns>year as number and month as string</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private Tuple<int, string> GetNextYearMonthToExecute(DateTime now,List<string> monthsFull,List<string> months)
        {
            bool finished = false;
            string nextExecMonth = "";
            int nextExecYear = now.Year;

            try
            {
                for (int i = now.Month; !finished; i++)
                {
                    nextExecMonth = monthsFull[(i - 1) % 12];
                    if (months.Contains(nextExecMonth))
                    {
                        if ((i - 1) >= 12)
                            nextExecYear = now.Year + 1;

                        finished = true;
                    }
                }
                return new Tuple<int, string>(nextExecYear, nextExecMonth);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetNextYearMonthToExecute",
                    "Determining the next execution month and year failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }
        }

        /// <summary>
        /// define the times of execution in week (day of week : time)
        /// </summary>
        /// <param name="startDate">first time of execution in timeplan</param>
        /// <param name="weekRepetition">number of repetitions in week</param>
        /// <param name="timeBetweenExecutions">time between each repetition</param>
        /// <param name="weekdays">list of allowed weekdays to execute</param>
        /// <returns>mapping of weekdays to times for execution</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private List<Tuple<string, LocalTime>> GetWeekRepetitionMapping(
            DateTime startDate,
            int weekRepetition,
            int timeBetweenExecutions,
            List<string> weekdays
        )
        {
            List<Tuple<string, LocalTime>> weekRepMapping = [];
            LocalTime start = new(startDate.Hour, startDate.Minute, startDate.Second, startDate.Millisecond);

            try
            {
                int day = 0;
                for (int i = 0; i < weekRepetition; i++)
                {
                    weekRepMapping.Add(new Tuple<string, LocalTime>(weekdays[day % weekdays.Count], start));
                    LocalTime next = start.Plus(Period.FromMinutes(timeBetweenExecutions));

                    Period timegap = Period.Between(start, next, PeriodUnits.Minutes);

                    if (timeBetweenExecutions >= 24 * 60)
                    {
                        // determine how many days the minutes are
                        double addDays = Math.Floor((double)(timeBetweenExecutions / 60 / 24));
                        day += (int)addDays;
                    }
                    else
                    {
                        if (timegap.Minutes < 0)
                            day++;
                    }
                    start = next;
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetWeekRepetitionMapping",
                    "Creating repetition mapping for week failed",
                    ref DummySem,
                    _dummyTuple
                );
            }

            return weekRepMapping;
        }

        /// <summary>
        /// determine the next allowed last day of month beginning from startDate
        /// </summary>
        /// <param name="monthLast">true if last day of month shall be calculated</param>
        /// <param name="now">date of request to calculate</param>
        /// <param name="startDate">date of first execution</param>
        /// <param name="monthsFull">full list of all months</param>
        /// <param name="allowedMonths">list of months allowed to execute in</param>
        /// <param name="allowedWeekdays">list of allowed weekdays to execute on</param>
        /// <returns>next last of month date or max. datetime possible</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DateTime GetLastOfMonth(
            bool monthLast,
            DateTime now,
            DateTime startDate,
            List<string> monthsFull,
            List<string> allowedMonths,
            List<string> allowedWeekdays
        )
        {
            DateTime lastOfMonth;
            if (monthLast)
            {
                try
                {
                    do
                    {
                        lastOfMonth = GetLastDayOfMonth(now.Year, now.Month);
                        lastOfMonth = DateTime.Parse(
                            $"{lastOfMonth.Year}-{lastOfMonth.Month}-{lastOfMonth.Day}T" +
                            $"{startDate.ToLongTimeString()}"
                        );
                        now = now.AddMonths(1);  // in case of not satisfying constraints go to next month
                    } while (!allowedMonths.Contains(monthsFull[lastOfMonth.Month - 1]) ||
                             !allowedWeekdays.Contains(lastOfMonth.DayOfWeek.ToString()));

                    LogMessageLocal(processor, $"LastOfMonth: {lastOfMonth}", _dummyTuple);
                    return lastOfMonth;
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "GetLastOfMonth",
                        "Retrieving Last day of Month failed!",
                        ref DummySem,
                        _dummyTuple
                    );
                }
            }
            else
            {
                return DateTime.MaxValue;
            }
        }

        /// <summary>
        /// determines the dates of the week of the given month in given year
        /// </summary>
        /// <param name="year">Year</param>
        /// <param name="month">Month</param>
        /// <param name="week">requested week (< 6)</param>
        /// <returns>list of DateTimes of requested weeks</returns>
        /// <exception cref="Exception">in case of any error</exception>
        private List<DateTime> GetWeekDates(int year,int month,int week = 1)
        {
            if (week > 5)
                throw new ETLException(
                    _processor,
                    $"Weeknumber greater 5 is not allowed! (was {week})",
                    "GetWeekDates",
                    _dummyTuple
                );

            List<DateTime> firstWeekDates = [];

            // Find the first day of the month for the given date
            DateTime firstDayOfMonth = new(year, month, 1);

            // Find the first day of the week for the first day of the month
            DateTime firstDayOfWeek;
            try
            {
                DayOfWeek startOfWeek = DayOfWeek.Monday; // Assuming Sunday is the start of the week
                int timeDiff = (int)firstDayOfMonth.DayOfWeek - (int)startOfWeek;
                int dayOffset = -(timeDiff < 0 ? timeDiff + 7 : timeDiff);
                firstDayOfWeek = firstDayOfMonth.AddDays(dayOffset);

                firstDayOfWeek = firstDayOfWeek.AddDays(7 * (week - 1));
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetWeekDates",
                    $"Calculating the first day of the week {week} for month {month} in {year} failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            if (firstDayOfWeek.Month > month)
                throw new ETLException(
                    _processor,
                    $"Given week does not belong to input date month! ({firstDayOfWeek})",
                    "GetWeekDates",
                    _dummyTuple
                );

            try
            {
                // Add the dates of the first week to the list
                for (int i = 0; i < 7; i++)
                {
                    firstWeekDates.Add(firstDayOfWeek.AddDays(i));
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetWeekDates",
                    "Adding 7 days to first day of week failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }

            return firstWeekDates;
        }

        /// <summary>
        /// easy calculation to get minutes between each repetition
        /// </summary>
        /// <param name="numRepetitions">number of repetitions</param>
        /// <param name="executiondays">number of days in week to execute (<= 7)</param>
        /// <returns>the minutes between each repetition</returns>
        /// <exception cref="Exception">in case of any error</exception>
        private int CalculateTimeBetweenExecutions(int numRepetitions,int executiondays)
        {
            try
            {
                return executiondays * 24 * 60 / numRepetitions;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CalculateTimeBetweenExecutions",
                    "Calculating the time between repetitions failed!",
                    ref DummySem,
                    _dummyTuple
                );
            }
        }

        /// <summary>
        /// Returns the DateTime of the last day of a given month in the given year
        /// </summary>
        /// <param name="year">Year</param>
        /// <param name="month">Month</param>
        /// <returns>DateTime of last day in month of year</returns>
        /// <exception cref="ArgumentOutOfRangeException">when given month < 1 and > 12</exception>
        private DateTime GetLastDayOfMonth(int year, int month)
        {
            try
            {
                // Validate input
                if (month < 1 || month > 12)
                    throw new ArgumentOutOfRangeException(nameof(month), "Month must be between 1 and 12.");

                // Get the last day of the month
                int lastDay = DateTime.DaysInMonth(year, month);

                // Create and return the DateTime object for the last day of the month
                return new DateTime(year, month, lastDay);
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetLastDayOfMonth",
                    $"Failed calculating the last day of month {month} in year {year}",
                    ref DummySem,
                    _dummyTuple
                );
            }
        }
    }
}
