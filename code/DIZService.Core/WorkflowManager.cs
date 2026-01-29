using DocumentFormat.OpenXml.Drawing.Charts;

namespace DIZService.Core
{
    /// <summary>
    /// symbolizes the 4 stages of a workflow
    /// </summary>
    public enum WorkflowStage
    {
        Unknown,
        Scheduled,
        Initializing,
        Executing,
        Failed,
        Finished
    }

    // handles all variables for status workflow status and gives multiple
    // functions to easy manage the state of workflows
    public class WorkflowManager(Processor processor, bool debug) : Helper
    {
        private readonly bool _debug = debug;
        private readonly Processor _processor = processor;

        // use to grant exclusive access on all workflow lists
        private SemaphoreSlim _workflowVarLock = new(1, 1);
        // use to grant exclusive access on variables in mapping functions
        private readonly SemaphoreSlim _mappingVarLock = new(1, 1);

        public SemaphoreSlim WorkflowSteerLock = new(1, 1);  // use to grant init and finish of workflow
        public SemaphoreSlim PackageSteerLock = new(1, 1);  // use to grant init and finish of workflow
        public SemaphoreSlim RealizationSteerLock = new(1, 1);  // use to grant init and finish of workfl.
        public SemaphoreSlim StepSteerLock = new(1, 1);  // use to grant init and finish of workflow

        // lists all workflow IDs that are scheduled and wait to initialized
        private readonly List<Workflow> _scheduledWorkflows = [];
        // use to grant exclusive access on variables in isWorkflow function
        private readonly SemaphoreSlim _schedWFLock = new(1, 1);
        // lists all workflow IDs that are initializing and wait for execution start and locks
        private readonly List<Workflow> _initializingWorkflows = [];
        // use to grant exclusive access on variables in isWorkflow function
        private readonly SemaphoreSlim _initWFLock = new(1, 1);
        // lists all workflow IDs that are executing at the moment
        private readonly List<Workflow> _executingWorkflows = [];
        // use to grant exclusive access on variables in isWorkflow function
        private readonly SemaphoreSlim _execWFLock = new(1, 1);
        // lists all workflow IDs that resulted in error
        private readonly List<Workflow> _failedWorkflows = [];
        // use to grant exclusive access on variables in isWorkflow function
        private readonly SemaphoreSlim _failWFLock = new(1, 1);
        // tracks what workflows were already executed and at what time
        private readonly List<Workflow> _finishedWorkflows = [];
        // use to grant exclusive access on variables in isWorkflow function
        private SemaphoreSlim _finWFLock = new(1, 1);

        // tracks all workflow IDs of workflows that were executed once
        private readonly List<int> _executedWorkflows = [];
        // use to grant exclusive access on variables in isWorkflow function
        private readonly SemaphoreSlim _executedWFLock = new(1, 1);

        // maps the workflow IDs to a ZeitplanAusfuehrungenID
        private readonly Dictionary<int, int> _zeitplanAusfuehrungIDMapping = [];

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        /// <summary>
        /// collects all needed information from workflow manager and prints it to logs
        /// </summary>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void LogWorkflowStates(Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                List<int> _intScheduledWorkflows = [];
                List<Workflow> scheduledWorkflows = [.. _scheduledWorkflows];
                scheduledWorkflows.ForEach((item) => _intScheduledWorkflows.Add(item.GetID()));
                string sched = GetListString(
                    _processor, "Scheduled Workflows", _intScheduledWorkflows, prozesslaeufe);

                List<int> _intInitializingWorkflows = [];
                List<Workflow> initializingWorkflows = [.. _initializingWorkflows];
                initializingWorkflows.ForEach((item) => _intInitializingWorkflows.Add(item.GetID()));
                string init = GetListString(
                    _processor, "Initializing Workflows", _intInitializingWorkflows, prozesslaeufe);

                List<int> _intExecutingWorkflows = [];
                List<Workflow> executingWorkflows = [.. _executingWorkflows];
                executingWorkflows.ForEach((item) => _intExecutingWorkflows.Add(item.GetID()));
                string exec = GetListString(
                    _processor, "Executing Workflows", _intExecutingWorkflows, prozesslaeufe);

                List<int> _intFinishedWorkflows = [];
                List<Workflow> finishedWorkflows = [.. _finishedWorkflows];
                finishedWorkflows.ForEach((item) => _intFinishedWorkflows.Add(item.GetID()));
                string fin = GetListString(
                    _processor, "Finished Workflows", _intFinishedWorkflows, prozesslaeufe);

                List<int> _intFailedWorkflows = [];
                List<Workflow> failedWorkflows = [.. _failedWorkflows];
                failedWorkflows.ForEach((item) => _intFailedWorkflows.Add(item.GetID()));
                string fail = GetListString(
                    _processor, "Failed Workflows", _intFailedWorkflows, prozesslaeufe);

                string executed = GetListString(
                    _processor, "Executed Workflows", _executedWorkflows, prozesslaeufe);

                Task.Run(() => Log(
                    _processor,
                    $"Workflow States: {sched} | {init} | {exec} | {fin} | {fail} | {executed}",
                    prozesslaeufe
                )).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = DummySem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "LogWorkflowStates",
                    "Could not print the workflow manager state",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if a workflow was already created and is listed
        /// </summary>
        /// <param name="workflowID">ID to search for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if workflow is known, false otherwise</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public bool ExistsWorkflow(int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                WorkflowStage stage = GetWorkflowStage(workflowID, prozesslaeufe);

                if (stage == WorkflowStage.Unknown)
                    return false;

                return true;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "ExistsWorkflow",
                    "Could not determine stage of workflow to check existence",
                   ref DummySem,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// returns a list of scheduled workflows (IDs)
        /// </summary>
        /// <returns>List of Workflows</returns>
        public List<Workflow> GetScheduledWorkflows()
        {
            return _scheduledWorkflows;
        }

        /// <summary>
        /// returns a list of initialized workflows (IDs)
        /// </summary>
        /// <returns>List of Workflows</returns>
        public List<Workflow> GetInitializingWorkflows()
        {
            return _initializingWorkflows;
        }

        /// <summary>
        /// returns a list of executing workflows (IDs)
        /// </summary>
        /// <returns>List of Workflows</returns>
        public List<Workflow> GetExecutingWorkflows()
        {
            return _executingWorkflows;
        }

        /// <summary>
        /// returns a list of failed workflows (IDs)
        /// </summary>
        /// <returns>List of Workflows</returns>
        public List<Workflow> GetFailedWorkflows()
        {
            return _failedWorkflows;
        }

        /// <summary>
        /// returns a list of finished workflows (IDs)
        /// </summary>
        /// <returns>List of Workflows</returns>
        public List<Workflow> GetFinishedWorkflows()
        {
            return _finishedWorkflows;
        }

        /// <summary>
        /// returns the mapping of workflow IDs to ZeitplanAusfuehrungenIDs
        /// </summary>
        /// <returns>Mapping of IDs to IDs</returns>
        public Dictionary<int, int> GetZeitplanAusfuehrungenIDMapping()
        {
            return _zeitplanAusfuehrungIDMapping;
        }

        // ------------------ HELPER FUNCTIONS WORKFLOW LISTS ------------------

        /// <summary>
        /// checks all worklow listings for workflow with given ID and returns this workflow. Throws exception if no
        /// list includes a workflow with given ID
        /// </summary>
        /// <param name="workflowID">ID of workflow to look for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>Workflow (if found)</returns>
        /// <exception cref="ETLException">when workflow not known or other error</exception>
        public Workflow GetWorkflow(int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                Workflow workflow;
                _workflowVarLock.Wait();
                _usedSem = _workflowVarLock;

                // check if workflow is scheduled
                if (_scheduledWorkflows.Any(item => item.GetID() == workflowID))
                {
                    workflow = _scheduledWorkflows.Find(item => item.GetID() == workflowID) ??
                                    throw new ETLException("Error for Scheduled Workflows");
                }
                else
                {
                    // check if workflow is executing
                    if (_executingWorkflows.Any(item => item.GetID() == workflowID))
                    {
                        workflow = _executingWorkflows.Find(item => item.GetID() == workflowID) ??
                                        throw new ETLException("Error for Executing Workflows");
                    }
                    else
                    {
                        // check if workflow has finished
                        if (_finishedWorkflows.Any(item => item.GetID() == workflowID))
                        {
                            workflow = _finishedWorkflows.Find(item => item.GetID() == workflowID) ??
                                            throw new ETLException("Error for finished Workflows");
                        }
                        else
                        {
                            // check if workflow has failed
                            if (_failedWorkflows.Any(item => item.GetID() == workflowID))
                            {
                                workflow = _failedWorkflows.Find(item => item.GetID() == workflowID) ??
                                                throw new ETLException("Error for failed Workflows");
                            }
                            else
                            {
                                if (_initializingWorkflows.Any(item => item.GetID() == workflowID))
                                {
                                    workflow = _initializingWorkflows.Find(item => item.GetID() == workflowID) ??
                                                    throw new ETLException("Error for initializing Workflows");
                                }
                                else
                                {
                                    // workflow unknown
                                    throw new ETLException(
                                        _processor,
                                        $"There is no workflow with ID {workflowID} on any level!",
                                        "WorkflowManager.GetWorkflow",
                                        prozesslaeufe
                                    );
                                }
                            }
                        }
                    }
                }
                _workflowVarLock.Release();
                _usedSem = null;

                return workflow;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "WorkflowManager.GetWorkflow",
                   $"Retrieving the workflow with ID {workflowID} failed!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks list of workflow on given level for workflow with given ID and returns this workflow. Throws
        /// exception if no workflow with given ID is not known on given level
        /// </summary>
        /// <param name="stage">stage where to search for workflow</param>
        /// <param name="workflowID">ID of workflow to find</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>Workflow (if found)</returns>
        /// <exception cref="ETLException">workflow not found, stage unknown or other errors</exception>
        public Workflow GetWorkflow(WorkflowStage stage, int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                Workflow? workflow;
                switch (stage)
                {
                    case WorkflowStage.Scheduled:
                        _schedWFLock.Wait();
                        _usedSem = _schedWFLock;
                        if (_scheduledWorkflows.Any(item => item.GetID() == workflowID))
                        {
                            workflow = _scheduledWorkflows.Find(item => item.GetID() == workflowID) ??
                                            throw new ETLException("Error for Scheduled Workflows");
                        }
                        else
                        {
                            workflow = null;
                        }
                        _schedWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Initializing:
                        _initWFLock.Wait();
                        _usedSem = _initWFLock;
                        if (_initializingWorkflows.Any(item => item.GetID() == workflowID))
                        {
                            workflow = _initializingWorkflows.Find(item => item.GetID() == workflowID) ??
                                            throw new ETLException("Error for initializing Workflows");
                        }
                        else
                        {
                            workflow = null;
                        }
                        _initWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Executing:
                        _execWFLock.Wait();
                        _usedSem = _execWFLock;
                        if (_executingWorkflows.Any(item => item.GetID() == workflowID))
                        {
                            workflow = _executingWorkflows.Find(item => item.GetID() == workflowID) ??
                                            throw new ETLException("Error for exeucting Workflows");
                        }
                        else
                        {
                            workflow = null;
                        }
                        _execWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Finished:
                        _finWFLock.Wait();
                        _usedSem = _finWFLock;
                        if (_finishedWorkflows.Any(item => item.GetID() == workflowID))
                        {
                            workflow = _finishedWorkflows.Find(item => item.GetID() == workflowID) ??
                                            throw new ETLException("Error for finished Workflows");
                        }
                        else
                        {
                            workflow = null;
                        }
                        _finWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Failed:
                        _failWFLock.Wait();
                        _usedSem = _failWFLock;
                        if (_failedWorkflows.Any(item => item.GetID() == workflowID))
                        {
                            workflow = _failedWorkflows.Find(item => item.GetID() == workflowID) ??
                                            throw new ETLException("Error for failed Workflows");
                        }
                        else
                        {
                            workflow = null;
                        }
                        _failWFLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is no given stage {stage}!",
                            "WorkflowManager.GetWorkflow",
                            prozesslaeufe
                        );
                }

                if (workflow != null)
                {
                    _usedSem?.Release();
                    return workflow;
                }
                else
                {
                    throw new ETLException(
                        _processor,
                        $"There is no workflow with ID {workflowID} on the given level {stage}!",
                        "WorkflowManager.GetWorkflow",
                        prozesslaeufe
                    );
                }

            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                    "WorkflowManager.GetWorkflow",
                    $"There is no Workflow {workflowID} scheduled!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// sets the given workflow to scheduled. checks if the workflow is not executing and not already scheduled.
        /// if workflow did finish before, removes it from finished list
        /// </summary>
        /// <param name="workflow">Workflow to schedule</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when workflow executing or already scheduled or other error</exception>
        public void SetWorkflowScheduled(Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                WorkflowStage stage = GetWorkflowStage(workflow, prozesslaeufe);
                // check if workflow has not stage finished, failed or unknown -> error
                if (stage == WorkflowStage.Initializing ||
                    stage == WorkflowStage.Executing ||
                    stage == WorkflowStage.Scheduled)
                        throw new ETLException(
                            _processor,
                            $"Workflow with ID {workflow.GetID()} has a bad State ({stage}) to set scheduled",
                            "SetWorkflowScheduled",
                            prozesslaeufe
                        );

                // add workflow to scheduled list
                _schedWFLock.Wait();
                _usedSem = _schedWFLock;

                _scheduledWorkflows.Add(workflow);

                _schedWFLock.Release();
                _usedSem = null;

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Added workflow {workflow.GetID()} to scheduled list!",
                        prozesslaeufe,
                        _debug
                    );
                    LogWorkflowStates(prozesslaeufe);
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                    "SetWorkflowScheduled",
                    $"Failed to set workflow with ID {workflow.GetID()} to scheduled!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// removes the given workflow from scheduled list
        /// </summary>
        /// <param name="workflow">Workflow to neutralize</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">if workflow is in no list or other error</exception>
        public void NeutraliseWorkflow(Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                WorkflowStage wState = GetWorkflowStage(workflow, prozesslaeufe);

                // check if workflow not scheduled -> error
                if (wState != WorkflowStage.Scheduled)
                    throw new ETLException(
                        _processor,
                        $"Workflow {workflow.GetID()} is not scheduled -> Cannot remove scheduled state!",
                        "NeutraliseWorkflow",
                        prozesslaeufe
                    );

                _schedWFLock.Wait();
                _usedSem = _schedWFLock;

                _scheduledWorkflows.Remove(workflow);

                // Remove from failed or finished list
                if (IsWorkflow(WorkflowStage.Finished, workflow.GetID(), prozesslaeufe))
                {
                    try
                    {
                        _finWFLock.Wait();
                        _finishedWorkflows.Remove(workflow);
                        _finWFLock.Release();
                    }
                    catch (Exception e)
                    {
                        throw HandleErrorCatch(
                           _processor,
                           e,
                           "NeutraliseWorkflow",
                           $"Failed removing Workflow with ID {workflow.GetID()} from finished list!",
                           ref _finWFLock,
                           prozesslaeufe
                        );
                    }
                }
                if (IsWorkflow(WorkflowStage.Failed, workflow.GetID(), prozesslaeufe))
                    RemoveErrorState(workflow, prozesslaeufe);

                // Remove Mapping
                RemoveMapping(workflow.GetID(), prozesslaeufe);

                _schedWFLock.Release();
                _usedSem = null;

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Neutralized workflow {workflow.GetID()}!",
                        prozesslaeufe,
                        _debug
                    );
                    LogWorkflowStates(prozesslaeufe);
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "NeutraliseWorkflow",
                    $"Failed setting Workflow with ID {workflow.GetID()} from scheduled to unhandled (unknown, " +
                    $"failed, finished)!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }
        /// <summary>
        /// removes the given workflow from any list and removes it from memory
        /// </summary>
        /// <param name="workflowID">ID of Workflow to neutralize</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when workflow is in no list or other error</exception>
        public void NeutraliseWorkflow(int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                WorkflowStage wState = GetWorkflowStage(workflowID, prozesslaeufe);
                Workflow workflow = GetWorkflow(workflowID, prozesslaeufe);

                if (wState != WorkflowStage.Scheduled)
                    throw new ETLException(
                        _processor,
                        $"Workflow {workflow.GetID()} is not scheduled -> Cannot remove scheduled state!",
                        "NeutraliseWorkflow",
                        prozesslaeufe
                    );

                _schedWFLock.Wait();
                _usedSem = _schedWFLock;

                _scheduledWorkflows.Remove(workflow);

                // Remove from failed or finished list
                if (IsWorkflow(WorkflowStage.Finished, workflowID, prozesslaeufe))
                {
                    try
                    {
                        _finWFLock.Wait();
                        _finishedWorkflows.Remove(workflow);
                        _finWFLock.Release();
                    }
                    catch (Exception e)
                    {
                        throw HandleErrorCatch(
                           _processor,
                           e,
                           "NeutraliseWorkflow",
                           $"Failed removing Workflow with ID {workflowID} from finished list!",
                           ref _finWFLock,
                           prozesslaeufe
                        );
                    }
                }
                if (IsWorkflow(WorkflowStage.Failed, workflowID, prozesslaeufe))
                    RemoveErrorState(workflow, prozesslaeufe);

                // Remove Mapping
                RemoveMapping(workflowID, prozesslaeufe);

                _schedWFLock.Release();
                _usedSem = null;

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Neutralized workflow {workflow.GetID()}!",
                        prozesslaeufe,
                        _debug
                    );
                    LogWorkflowStates(prozesslaeufe);
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "NeutraliseWorkflow",
                   $"Failed set Workflow with ID {workflowID} from scheduled to unhandled (unknown, finished, failed)!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if workflow is really scheduled. if so it remove it from scheduled list and adds it to initializing
        /// list. if initializing list already includes the workflow an exception is thrown
        /// </summary>
        /// <param name="workflow">Workflow to set from scheduled to initializing</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">workflow not scheduled, workflow already initing, any other error</exception>
        public void SetWorkflowScheduledToInitializing(Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _workflowVarLock.Wait();

                // check if workflow is not scheduled -> error
                if (!IsWorkflow(WorkflowStage.Scheduled, workflow, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    throw new ETLException(
                        _processor,
                        $"Workflow with ID {workflow.GetID()} is not scheduled!",
                        "SetWorkflowScheduledToInitializing",
                        prozesslaeufe
                    );
                }

                // check if workflow is alread initializing -> error
                if (IsWorkflow(WorkflowStage.Initializing, workflow, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    throw new ETLException(
                        _processor,
                        $"Workflow with ID {workflow.GetID()} is already initializing!",
                        "SetWorkflowScheduledToInitializing",
                        prozesslaeufe
                    );
                }

                _workflowVarLock.Release();
                _usedSem = null;

                _schedWFLock.Wait();
                _usedSem = _schedWFLock;

                _scheduledWorkflows.Remove(workflow);

                _schedWFLock.Release();
                _usedSem = null;

                _initWFLock.Wait();
                _usedSem = _initWFLock;

                _initializingWorkflows.Add(workflow);

                _initWFLock.Release();
                _usedSem = null;

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Moved workflow {workflow.GetID()} from scheduled to initializing!",
                        prozesslaeufe,
                        _debug
                    );
                    LogWorkflowStates(prozesslaeufe);
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "SetWorkflowScheduledToInitializing",
                   $"Failed to set Workflow with ID {workflow.GetID()} from scheduled to initializing!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if workflow is really initializing. if so it remove it from initializing list and adds it to
        /// executing list. if executing list already includes the workflow an exception is thrown
        /// </summary>
        /// <param name="workflow">Workflow to set from initializing to executing</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">workflow not initing, workflow already executing, any other error</exception>
        public void SetWorkflowInitializingToExecuting(Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _workflowVarLock.Wait();
                _usedSem = _workflowVarLock;

                // check if workflow is not initializing -> error
                if (!IsWorkflow(WorkflowStage.Initializing, workflow, prozesslaeufe))
                    throw HandleErrorCatch(
                        _processor,
                        new ETLException(
                            _processor,
                            $"Workflow with ID {workflow.GetID()} is not initializing!",
                            "SetWorkflowInitializingToExecuting",
                            prozesslaeufe
                        ),
                        "SetWorkflowInitializingToExecuting",
                        $"Workflow with ID {workflow.GetID()} is not initializing!",
                        ref _workflowVarLock,
                        prozesslaeufe
                    );

                // check if workflow is already executing -> error
                if (IsWorkflow(WorkflowStage.Executing, workflow, prozesslaeufe))
                    throw HandleErrorCatch(
                        _processor,
                        new ETLException(
                            _processor,
                            $"Workflow with ID {workflow.GetID()} is already executing!",
                            "SetWorkflowInitializingToExecuting",
                            prozesslaeufe
                        ),
                        "SetWorkflowInitializingToExecuting",
                        $"Workflow with ID {workflow.GetID()} is already executing!",
                        ref _usedSem,
                        prozesslaeufe
                    );

                _workflowVarLock.Release();
                _usedSem = null;

                _initWFLock.Wait();
                _usedSem = _initWFLock;

                _initializingWorkflows.Remove(workflow);

                _initWFLock.Release();
                _usedSem = null;

                _execWFLock.Wait();
                _usedSem = _execWFLock;

                _executingWorkflows.Add(workflow);

                _execWFLock.Release();
                _usedSem = null;

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Moved workflow {workflow.GetID()} from initializing to executing!",
                        prozesslaeufe,
                        _debug
                    );
                    LogWorkflowStates(prozesslaeufe);
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "SetWorkflowScheduledToExecuting",
                   $"Failed to set Workflow with ID {workflow.GetID()} from initialized to executing!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if workflow is really executing. If conditions satisfied
        /// removes the workflow from executing list and adds it to error list.
        /// </summary>
        /// <param name="workflow">Workflow to set from executing to error</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when workflow is not executing or other error</exception>
        public void SetWorkflowFailed(Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                WorkflowStage wStage = GetWorkflowStage(workflow, prozesslaeufe);

                switch (wStage)
                {
                    case WorkflowStage.Scheduled:
                        _schedWFLock.Wait();
                        _usedSem = _schedWFLock;
                        _scheduledWorkflows.Remove(workflow);
                        _schedWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Initializing:
                        _initWFLock.Wait();
                        _usedSem = _initWFLock;
                        _initializingWorkflows.Remove(workflow);
                        _initWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Executing:
                        _execWFLock.Wait();
                        _usedSem = _execWFLock;
                        _executingWorkflows.Remove(workflow);
                        _execWFLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        Task.Run(() =>
                        {
                            Log(
                                _processor,
                                $"Workflow with ID {workflow.GetID()} ({wStage}) has no state to set to failed!",
                                prozesslaeufe,
                                null,
                                LogType.Error
                            );
                        }).Wait();
                        break;
                }

                // if workflow has not failed before add it to failure list
                if (!IsWorkflow(WorkflowStage.Failed, workflow, prozesslaeufe))
                {
                    _failWFLock.Wait();
                    _usedSem = _failWFLock;
                    _failedWorkflows.Add(workflow);
                    _failWFLock.Release();
                    _usedSem = null;
                }

                // if workflow has finished before remove it from finished list
                if (IsWorkflow(WorkflowStage.Finished, workflow, prozesslaeufe))
                {
                    _finWFLock.Wait();
                    _usedSem = _finWFLock;
                    _finishedWorkflows.Remove(workflow);
                    _finWFLock.Release();
                    _usedSem = null;
                }

                // add workflow to executed list if not executed before
                if (!_executedWorkflows.Contains(workflow.GetID()))
                {
                    _executedWFLock.Wait();
                    _usedSem = _executedWFLock;
                    _executedWorkflows.Add(workflow.GetID());
                    _executedWFLock.Release();
                    _usedSem = null;
                }

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Set workflow {workflow.GetID()} to Failed!",
                        prozesslaeufe,
                        _debug
                    );
                    LogWorkflowStates(prozesslaeufe);
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "SetWorkflowFailed",
                   $"Failed setting Workflow with ID {workflow.GetID()} from executing to failed!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// sets a workflow from executing to finished by removing it from executing list and adding to finished list.
        /// To guarantee correct state transmissions checks if workflow is executing at the beginning and also if it
        /// is not already finished.
        /// Also checks if workflows failed before and removes the error state
        /// </summary>
        /// <param name="workflow">workflow to set to finished</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when workflow was not executing or other error</exception>
        public void SetWorkflowExecutingToFinished(Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                // check if workflow is really executing
                if (!IsWorkflow(WorkflowStage.Executing, workflow, prozesslaeufe))
                    throw new ETLException(
                        _processor,
                        $"There is no workflow executing with ID {workflow.GetID()}",
                        "SetWorkflowExecutingToFinished",
                        prozesslaeufe
                    );

                // if workflow has not finished before add it to list -> otherwise skip
                if (!IsWorkflow(WorkflowStage.Finished, workflow, prozesslaeufe))
                {
                    _finWFLock.Wait();
                    _usedSem = _finWFLock;
                    _finishedWorkflows.Add(workflow);
                    _finWFLock.Release();
                    _usedSem = null;
                }

                // remove error state if set before
                if (IsWorkflow(WorkflowStage.Failed, workflow, prozesslaeufe))
                    RemoveErrorState(workflow, prozesslaeufe);

                // add workflow to executed list if not executed before
                _executedWFLock.Wait();
                _usedSem = _executedWFLock;
                if (!_executedWorkflows.Contains(workflow.GetID()))
                {
                    _executedWorkflows.Add(workflow.GetID());
                    _executedWFLock.Release();
                    _usedSem = null;
                } else
                {
                    _executedWFLock.Release();
                    _usedSem = null;
                }

                // remove executing state for workflow
                _execWFLock.Wait();
                _usedSem = _execWFLock;
                _executingWorkflows.Remove(workflow);
                _execWFLock.Release();
                _usedSem = null;

                RemoveMapping(workflow.GetID(), prozesslaeufe);
                workflow.SignalizeRemovedMapping();

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Moved workflow {workflow.GetID()} from executing to finished!",
                        prozesslaeufe,
                        _debug
                    );
                    LogWorkflowStates(prozesslaeufe);
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "SetWorkflowExecutingToFinished",
                   $"Failed to set workflow with ID {workflow.GetID()} from executing to finished!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if workflow exists in error list and removes it.
        /// Important: Does not get semaphores -> surround function with it
        /// </summary>
        /// <param name="workflow">Workflow to remove</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when workflow not set as failed or other error</exception>
        private void RemoveErrorState(Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                if (IsWorkflow(WorkflowStage.Failed, workflow, prozesslaeufe))
                {
                    _failWFLock.Wait();
                    _usedSem = _failWFLock;
                    _failedWorkflows.Remove(workflow);
                    _failWFLock.Release();
                    _usedSem = null;
                }
                else
                {
                    throw new ETLException(
                        _processor,
                        $"Workflow with ID {workflow.GetID()} is not set as failed!",
                        "RemoveErrorState",
                        prozesslaeufe
                    );
                }
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "RemoveErrorState",
                   $"Could not remove the error state from workflow with ID {workflow.GetID()}",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks in what list the given workflow can be found and returns the corresponding stage
        /// </summary>
        /// <param name="workflow">Workflow to get stage for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>Stage of workflow</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public WorkflowStage GetWorkflowStage(Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _workflowVarLock.Wait();
                _usedSem = _workflowVarLock;

                if (IsWorkflow(WorkflowStage.Scheduled, workflow, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Scheduled;
                }
                if (IsWorkflow(WorkflowStage.Initializing, workflow, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Initializing;
                }
                if (IsWorkflow(WorkflowStage.Executing, workflow, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Executing;
                }
                if (IsWorkflow(WorkflowStage.Failed, workflow, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Failed;
                }
                if (IsWorkflow(WorkflowStage.Finished, workflow, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Finished;
                }
                else
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Unknown;
                }
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflowStage",
                   $"Failed retrieving state of Workflow with ID {workflow.GetID()}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }
        /// <summary>
        /// checks in what list the given workflow can be found and returns the corresponding stage
        /// </summary>
        /// <param name="workflowID">ID of Workflow to get stage for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>Stage of Workflow</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public WorkflowStage GetWorkflowStage(int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _workflowVarLock.Wait();
                _usedSem = _workflowVarLock;

                if (IsWorkflow(WorkflowStage.Scheduled, workflowID, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Scheduled;
                }
                if (IsWorkflow(WorkflowStage.Initializing, workflowID, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Initializing;
                }
                if (IsWorkflow(WorkflowStage.Executing, workflowID, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Executing;
                }
                if (IsWorkflow(WorkflowStage.Failed, workflowID, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Failed;
                }
                if (IsWorkflow(WorkflowStage.Finished, workflowID, prozesslaeufe))
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Finished;
                }
                else
                {
                    _workflowVarLock.Release();
                    _usedSem = null;
                    return WorkflowStage.Unknown;
                }
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "GetWorkflowStage",
                   $"Failed retrieving state of Workflow with ID {workflowID}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if the given workflow has the given state
        /// </summary>
        /// <param name="stage">stage to search in</param>
        /// <param name="workflow">workflow to search for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if workflow in stage, false otherwise</returns>
        /// <exception cref="ETLException">when stage unknown or other error</exception>
        public bool IsWorkflow(WorkflowStage stage, Workflow workflow, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                bool contains = false;
                switch (stage)
                {
                    case WorkflowStage.Scheduled:
                        _schedWFLock.Wait();
                        _usedSem = _schedWFLock;
                        contains = _scheduledWorkflows.Contains(workflow);
                        _schedWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Initializing:
                        _initWFLock.Wait();
                        _usedSem = _initWFLock;
                        contains = _initializingWorkflows.Contains(workflow);
                        _initWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Executing:
                        _execWFLock.Wait();
                        _usedSem = _execWFLock;
                        contains = _executingWorkflows.Contains(workflow);
                        _execWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Failed:
                        _failWFLock.Wait();
                        _usedSem = _failWFLock;
                        contains = _failedWorkflows.Contains(workflow);
                        _failWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Finished:
                        _finWFLock.Wait();
                        _usedSem = _finWFLock;
                        contains = _finishedWorkflows.Contains(workflow);
                        _finWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Unknown:
                        contains = true;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"Workflow stage is not known! ({stage})",
                            "IsWorkflow",
                            prozesslaeufe
                        );
                }
                return contains;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "IsWorkflow",
                   $"Failed checking the state of Workflow with ID {workflow.GetID()}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }
        /// <summary>
        /// checks if a workflow with given ID has the given state
        /// </summary>
        /// <param name="stage">stage to search in</param>
        /// <param name="workflowID">ID of workflow to search for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if workflow in stage, false otherwise</returns>
        /// <exception cref="ETLException">when stage unknown or other error</exception>
        public bool IsWorkflow(WorkflowStage stage, int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                bool contains = true;
                switch (stage)
                {
                    case WorkflowStage.Scheduled:
                        _schedWFLock.Wait();
                        _usedSem = _schedWFLock;
                        if (_scheduledWorkflows.Find(w => w.GetID() == workflowID) == null)
                            contains = false;
                        _schedWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Initializing:
                        _initWFLock.Wait();
                        _usedSem = _initWFLock;
                        if (_initializingWorkflows.Find(w => w.GetID() == workflowID) == null)
                            contains = false;
                        _initWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Executing:
                        _execWFLock.Wait();
                        _usedSem = _execWFLock;
                        if (_executingWorkflows.Find(w => w.GetID() == workflowID) == null)
                            contains = false;
                        _execWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Failed:
                        _failWFLock.Wait();
                        _usedSem = _failWFLock;
                        if (_failedWorkflows.Find(w => w.GetID() == workflowID) == null)
                            contains = false;
                        _failWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Finished:
                        _finWFLock.Wait();
                        _usedSem = _finWFLock;
                        if (_finishedWorkflows.Find(w => w.GetID() == workflowID) == null)
                            contains = false;
                        _finWFLock.Release();
                        _usedSem = null;
                        break;
                    case WorkflowStage.Unknown:
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"Workflow stage is not known! ({stage})",
                            "IsWorkflow",
                            prozesslaeufe
                        );
                }

                return contains;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "IsWorkflow",
                   $"Failed checking the state of Workflow with ID {workflowID}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if the given workflow ID is listed in executed list
        /// </summary>
        /// <param name="workflowID">ID of workflow to check</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if workflow with given ID was executed before, otherwise false</returns>
        public bool WasExecutedOnce(int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _executedWFLock.Wait();
                _usedSem = _executedWFLock;
                bool contains = _executedWorkflows.Contains(workflowID);
                _executedWFLock.Release();
                _usedSem = null;

                return contains;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "WasExecutedOnce",
                    $"Failed checking if workflow {workflowID} was executed before!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        // --------------------- HELPER FUNCTIONS MAPPING ----------------------

        /// <summary>
        /// adds a mapping for given workflow ID to zeitplanAusfuehrungenID
        /// </summary>
        /// <param name="workflowID">key</param>
        /// <param name="zeitplanAusfuehrungenID">value</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void AddMapping(int workflowID, int zeitplanAusfuehrungenID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                // TODO: Check if ZeitplanIDMapping is already asigned

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Adding mapping for workflow {workflowID} and ZeitplanAusfuehrungenID " +
                        $"{zeitplanAusfuehrungenID} ({GetMappingString(_zeitplanAusfuehrungIDMapping, "Mapping")})",
                        prozesslaeufe,
                        _debug
                    );
                }).Wait();
                _mappingVarLock.Wait();
                _usedSem = _mappingVarLock;
                _zeitplanAusfuehrungIDMapping.Add(workflowID, zeitplanAusfuehrungenID);
                _mappingVarLock.Release();
                _usedSem = null;
                Task.Run(() => {
                    Log(
                        _processor,
                        $"Added mapping of workflow {workflowID} to ZeitplanAusfuehrungenID " +
                        $"{zeitplanAusfuehrungenID} ({GetMappingString(_zeitplanAusfuehrungIDMapping, "Mapping")})",
                        prozesslaeufe,
                        _debug
                    );
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "AddMapping",
                   $"Failed mapping zeitplanAusfuehrungenID {zeitplanAusfuehrungenID} to Workflow with ID " +
                   $"{workflowID}! ({GetMappingString(_zeitplanAusfuehrungIDMapping, "Mapping")})",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// removes the mapping for given workflow ID
        /// </summary>
        /// <param name="workflowID">key</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when workflow not mapped or other error</exception>
        public void RemoveMapping(int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _mappingVarLock.Wait();
                _usedSem = _mappingVarLock;
                if (!_zeitplanAusfuehrungIDMapping.ContainsKey(workflowID))
                    throw new ETLException(
                        _processor,
                        $"No mapping for Workflow with ID {workflowID} found!",
                        "RemoveMapping",
                        prozesslaeufe
                    );
                _zeitplanAusfuehrungIDMapping.Remove(workflowID);
                _mappingVarLock.Release();
                _usedSem = null;
                Task.Run(() => {
                    Log(
                        _processor,
                        $"Removed mapping of workflow {workflowID} (" +
                        $"{GetMappingString(_zeitplanAusfuehrungIDMapping, "Mapping")})",
                        prozesslaeufe,
                        null
                    );
                }).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "RemoveMapping",
                   $"Failed removing mapping zeitplanAusfuehrungenID to Workflow with ID {workflowID}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if there exists a mapping for a given workflow
        /// </summary>
        /// <param name="workflowID">key</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if the mapping includes the given workflowID, false otherwise</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public bool ExistsMapping(int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _mappingVarLock.Wait();
                _usedSem = _mappingVarLock;
                bool contains = _zeitplanAusfuehrungIDMapping.ContainsKey(workflowID);
                _mappingVarLock.Release();
                _usedSem = null;
                return contains;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                    "ExistsMapping",
                    $"Failed checking the mapping of zeitplanAusfuehrungenID to Workflow with ID {workflowID}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// retrieves the mapped zeitplanAusfuehrungenID for a given workflow ID
        /// </summary>
        /// <param name="workflowID">key</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>zeitplanAusfuehrungenID</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public int GetZeitplanAusfuehrungenID(int workflowID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _mappingVarLock.Wait();
                _usedSem = _mappingVarLock;
                int zeitplanAusfuehrungenID = _zeitplanAusfuehrungIDMapping[workflowID];
                _mappingVarLock.Release();
                _usedSem = null;
                return zeitplanAusfuehrungenID;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "ExistsMapping",
                   $"Failed retrieving the mapping of zeitplanAusfuehrungenID to Workflow with ID {workflowID}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }
    }
}
