using System.Data;
using System.Diagnostics;
using System.Data.Common;
using System.Runtime.InteropServices;

namespace DIZService.Core
{
    public class Step : Helper
    {
        private readonly int _id;  // StepID

        private readonly Processor _processor;      // global processor
        private readonly Workflow _workflow;        // parent workflow
        private readonly Package _package;          // parent workflow
        private readonly Realization _realization;  // parent realization

        // use to execute the wanted task
        private readonly CommandExecutor _executor;

        private int _paketschrittProzesslaeufeID = -1;

        // one of SQL, EXCEL, ... (not used at the moment)
        private readonly string _taskType;

        private string _command;                        // command to execute
        private readonly string _commandType;           // type of command to execute (one of COPY, EXEC, ...)
        private readonly List<string> _targetTables;    // table to execute on
        private readonly List<string> _srcTables;       // table to execute on

        private bool _startedExec = false;  // set to true if execution has started

        private readonly int _order;       // number that represents the rang of execution of this step
        private readonly bool _locked;     // true if this step shall be executed in stand-alone
        private readonly bool _timeslice;  // true if this step shall execute command in time slices

        private readonly int? _parameterID;  // ETL_Paketschritt_Parameter_ID if one is given

        private bool _increasedNumSteps = false;    // if true this step has inc. num. of steps for realization
        public bool IncreasedNumExecSteps = false;  // if true this step has inc. num. of executing steps in processor
        private bool _increasedNumThreads = false;  // if true this step has inc. num. of threads in processor

        private bool _addedTableWorkflow = false;     // if true this steps added tar. tab. to acc. list in workflow
        private bool _addedTableProcessor = false;    // if true this steps added tar. tab. to acc. list in processor

        private bool _announced = false;              // if true this step has announced locking

        private bool _addedToQueue = false;           // true if this step was added to step queue
        private bool _addedToTableQueue = false;      // true if this step was added to a table queue

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        // tuple that includes all prozesslaeufeIDs (prozess, paket, paketumsetzung, paketschritt)
        private Tuple<int?, int?, int?, int?> _prozesslaeufe = new(
            null, null, null, null
        );

        // read configuration ID of realization (set when reading connection infos)
        private string? _realizationConfigID;

        public Step(
            int id,
            Processor processor,
            Workflow workflow,
            Package package,
            Realization realization,
            string command,
            string commandType,
            string taskType,
            List<string> srcTables,
            List<string> targetTables,
            int order,
            bool locked,
            bool timeslice,
            int? parameterID
        ) {
            try
            {
                _id = id;

                _processor = processor;
                _workflow = workflow;
                _package = package;
                _realization = realization;

                _command = command;
                _commandType = commandType;
                _taskType = taskType;

                _order = order;
                _locked = locked;
                _timeslice = timeslice;

                if (parameterID != null)
                    _parameterID = parameterID;

                _prozesslaeufe = new Tuple<int?, int?, int?, int?>(
                    _workflow.GetProzesslaeufeID(),
                    _package.GetPaketProzesslaeufeID(),
                    _realization.GetPaketumsetzungProzesslaeufeID(),
                    null
                );

                _executor = new CommandExecutor(_processor);

                _targetTables = [];
                foreach (string table in targetTables)
                {
                    _targetTables.Add(
                        ReplacePlaceholder(
                            _processor,
                            _workflow,
                            table,
                            _prozesslaeufe,
                            _processor.Debug
                        )
                    );
                }
                _srcTables = [];
                foreach (string table in srcTables)
                {
                    if (table != "")
                        _srcTables.Add(
                            ReplacePlaceholder(
                                _processor,
                                _workflow,
                                table,
                                _prozesslaeufe,
                                _processor.Debug
                            )
                        );
                }

            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    processor,
                    e,
                    "Step",
                    "Setting step attributes!",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }

        // ---------- HELPER FUNCTIONS ----------

        /// <summary>
        /// returns the StepID of this step
        /// </summary>
        /// <returns>int</returns>
        public int GetID()
        {
            return _id;
        }

        /// <summary>
        /// returns the PaketschrittProzesslaeufeID of this step
        /// </summary>
        /// <returns>int</returns>
        public int GetPaketschrittProzesslaeufeID()
        {
            return _paketschrittProzesslaeufeID;
        }

        /// <summary>
        /// Wraps the logging command specified to this step
        /// </summary>
        /// <param name="message">message to log</param>
        /// <param name="debug">log depending on processor debug flag</param>
        /// <param name="type">EventType to log (Default=Information)</param>
        /// [SupportedOSPlatform("windows")]
        private void StepLog(string message, bool debug = false, LogType type = LogType.Info)
        {
            if (_paketschrittProzesslaeufeID == -1)
            {
                Task.Run(() =>
                {
                    Log(
                        _processor,
                        message,
                        _prozesslaeufe,
                        !debug || _processor.Debug,
                        type: type
                    );
                }).Wait();
            }
            else
            {
                Task.Run(() => Log(
                    _processor,
                    message,
                    _prozesslaeufe,
                    !debug || _processor.Debug,
                    type: type
                )).Wait();
            }
        }

        // ---------- STEP FUNCTIONS ----------

        /// <summary>
        /// prepares the step, by initializing logging , adding to queue (if lock is set) and waiting for first spot in
        /// this queue (step and table). Will then announce lock for this step if needed and wait for start (free treads
        /// , free steps, not-accessed table, no lock). When step can be started, the number of steps (processor and
        /// realization) and threads are increased, add table to accessed list (workflow and processor). If step was
        /// added to queue the step gets removed and the step is started
        /// </summary>
        /// <param name="inited">will be set to true if the steps was successfully initialized</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void Init(out bool inited)
        {
            try
            {
                // initialize logging
                Task.Run(() =>
                {
                    _paketschrittProzesslaeufeID = InitializeLogging(
                        _processor,
                        "Logging.ETL_Paketschritt_Prozesslaeufe",
                        [
                            new Tuple<string, int>("ETL_Prozesslaeufe_ID", _workflow.GetProzesslaeufeID()),
                            new Tuple<string, int>("ETL_Paket_Prozesslaeufe_ID", _package.GetPaketProzesslaeufeID()),
                            new Tuple<string, int>(
                                "ETL_Paketumsetzung_Prozesslaeufe_ID",
                                _realization.GetPaketumsetzungProzesslaeufeID()
                            ),
                            new Tuple<string, int>("ETL_Paketschritte_ID", _id)
                        ],
                        DateTime.Now,
                        _prozesslaeufe
                    );
                }).Wait();

                _prozesslaeufe = new Tuple<int?, int?, int?, int?>(
                    _workflow.GetProzesslaeufeID(),
                    _package.GetPaketProzesslaeufeID(),
                    _realization.GetPaketumsetzungProzesslaeufeID(),
                    _paketschrittProzesslaeufeID
                );

                Task.Run(() => StepLog($"Initialize step {_order} (ID: {_id})")).Wait();

                // lock announced?
                if (_processor.LockManager.IsAnnounced(Level.Step, _prozesslaeufe))
                {
                    // add to step queue (process)
                    _processor.QueueManager.AddToQueue(Level.Step, _id, _prozesslaeufe);
                    _addedToQueue = true;

                    // add step to table queue
                    foreach (string table in _targetTables)
                    {
                        _processor.QueueManager.AddToTableQueue(_id, table, _prozesslaeufe);
                    }
                    foreach (string table in _srcTables)
                    {
                        _processor.QueueManager.AddToTableQueue(_id, table, _prozesslaeufe);
                    }
                    _addedToTableQueue = true;
                }

                // was step added to queue? -> wait for 1st position in table and step queue
                try
                {
                    if (_addedToQueue)
                    {
                        bool waited = false;

                        _processor.WorkflowManager.StepSteerLock.Wait();  // guarantee single check
                        _usedSem = _processor.WorkflowManager.StepSteerLock;

                        bool first = _processor.QueueManager.CheckQueueFirst(Level.Step, _id, _prozesslaeufe);

                        bool targetTablesFirst = true;
                        foreach (string table in _targetTables)
                        {
                            bool tableFirst = _processor.QueueManager.CheckTableQueueFirst(
                                _id,
                                table,
                                _prozesslaeufe
                            );
                            if (!tableFirst)
                            {
                                targetTablesFirst = false;
                                break;
                            }
                        }
                        bool srcTablesFirst = true;
                        foreach (string table in _srcTables)
                        {
                            bool tableFirst = _processor.QueueManager.CheckTableQueueFirst(
                                _id,
                                table,
                                _prozesslaeufe
                            );
                            if (!tableFirst)
                            {
                                srcTablesFirst = false;
                                break;
                            }
                        }

                        int maxWait = 5;
                        DateTime jetzt = DateTime.Now;
                        bool onceSend = false;

                        while (!first || !targetTablesFirst || !srcTablesFirst)
                        {
                            _processor.WorkflowManager.StepSteerLock.Release();
                            _usedSem = null;

                            waited = true;
                            string stepQueue = GetListString(
                                _processor,
                                "Steps Queue",
                                _processor.QueueManager.WaitingSteps,
                                _prozesslaeufe
                            );
                            string tablesQueue = "";
                            foreach (string table in _targetTables)
                            {
                                tablesQueue += GetListString(
                                    _processor,
                                    "Table Queue",
                                    _processor.QueueManager.WaitingTableQueues[table],
                                    _prozesslaeufe
                                );
                            }

                            if (_processor.Debug || (DateTime.Now - jetzt).TotalMinutes >= maxWait || !onceSend)
                            {
                                Task.Run(() => StepLog(
                                    $"Wait for first spot in queue! ({first} | {targetTablesFirst}) (" +
                                    $"{stepQueue} | {tablesQueue})"
                                ), _workflow.GetCancelSource().Token).Wait();
                                jetzt = DateTime.Now;
                                onceSend = true;
                            }

                            // Task.Delay(2 * 1000, _workflow.GetCancelSource().Token).Wait();
                            Task.Delay(_processor.WaitingTime, _workflow.GetCancelSource().Token).Wait();

                            _processor.WorkflowManager.StepSteerLock.Wait();  // guarantee single check
                            _usedSem = _processor.WorkflowManager.StepSteerLock;

                            first = _processor.QueueManager.CheckQueueFirst(Level.Step, _id, _prozesslaeufe);
                            targetTablesFirst = true;
                            foreach (string table in _targetTables)
                            {
                                bool tableFirst = _processor.QueueManager.CheckTableQueueFirst(
                                    _id,
                                    table,
                                    _prozesslaeufe
                                );
                                if (!tableFirst)
                                {
                                    targetTablesFirst = false;
                                    break;
                                }
                            }
                            srcTablesFirst = true;
                            foreach (string table in _srcTables)
                            {
                                bool tableFirst = _processor.QueueManager.CheckTableQueueFirst(
                                    _id,
                                    table,
                                    _prozesslaeufe
                                );
                                if (!tableFirst)
                                {
                                    srcTablesFirst = false;
                                    break;
                                }
                            }
                        }
                        _processor.WorkflowManager.StepSteerLock.Release();
                        _usedSem = null;

                        if (waited)
                            Task.Run(() => StepLog(
                                $"Step {_id} ({_paketschrittProzesslaeufeID}) first in queue! " +
                                $"({first} | {targetTablesFirst})"
                            )).Wait();
                    }
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Step.Init",
                        "Failed waiting for first spot in queue",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // lock process for this step?
                if (_locked)
                {
                    Task.Run(() => StepLog($"Handle Lock for step {_order} (ID: {_id})")).Wait();

                    try
                    {
                        //    announce lock
                        _processor.LockManager.AnnounceLock(Level.Step, _prozesslaeufe);
                        _announced = true;
                    }
                    catch (Exception e)
                    {
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "Step.Init",
                            $"Failed announcing Lock for Step with ID {_id}!",
                            ref DummySem,
                            _prozesslaeufe
                        );
                    }
                }

                // wait until all conditions to start step are satified
                SemaphoreSlim? usedSem = null;
                try
                {
                    bool waited = false;

                    _processor.WorkflowManager.StepSteerLock.Wait();  // guarantee single check
                    _usedSem = _processor.WorkflowManager.StepSteerLock;

                    // process not locked
                    bool announced;
                    if (_locked)  // want to lock for this step?
                    {
                        announced = false;  // false (can execute)
                    }
                    else  // Is locked?
                    {
                        // false (can execute) | true  (wait)
                        announced = _processor.LockManager.IsAnnounced(Level.Step, _prozesslaeufe);
                    }
                    // step limit not reached (realization) - numSteps > maxParallel
                    bool stepLimit = !_realization.CheckFreeSteps();
                    // thread limit not reached (processor) - numThreads > maxThreads
                    bool threadLimit = !_processor.CheckFreeThreads();
                    // table not used (processor) - table accessed
                    bool targetTablesAccessed = false;
                    foreach (string table in _targetTables)
                    {
                        bool singleTableAccessed = _processor.CheckAccessedTable(table);
                        if (singleTableAccessed)
                        {
                            targetTablesAccessed = true;
                            break;
                        }
                    }
                    bool srcTablesAccessed = false;
                    foreach (string table in _srcTables)
                    {
                        bool singleTableAccessed = _processor.CheckAccessedTable(table);
                        if (singleTableAccessed)
                        {
                            srcTablesAccessed = true;
                            break;
                        }
                    }

                    int maxWait = 5;
                    DateTime jetzt = DateTime.Now;
                    bool onceSend = false;

                    while (announced || stepLimit || threadLimit || targetTablesAccessed || srcTablesAccessed)
                    {
                        _processor.WorkflowManager.StepSteerLock.Release();
                        _usedSem = null;

                        if (_workflow.GetCancelSource().Token.IsCancellationRequested)
                        {
                            inited = false;
                            Task.Run(() => StepLog("Catched requested cancellation")).Wait();

                            return;
                        }

                        waited = true;
                        if (_processor.Debug || (DateTime.Now - jetzt).TotalMinutes >= maxWait || !onceSend)
                        {
                            Task.Run(() => StepLog(
                                $"Wait until step can be started! ({announced} | {stepLimit} | {threadLimit} | " +
                                $"{targetTablesAccessed} | {srcTablesAccessed})"
                            )).Wait();
                            jetzt = DateTime.Now;
                            onceSend = true;
                        }

                        // Task.Delay(2 * 1000).Wait();
                        Task.Delay(_processor.WaitingTime).Wait();

                        _processor.WorkflowManager.StepSteerLock.Wait();
                        _usedSem = _processor.WorkflowManager.StepSteerLock;

                        // process not locked
                        if (_locked)
                        {
                            announced = false;
                        }
                        else
                        {
                            announced = _processor.LockManager.IsAnnounced(Level.Step, _prozesslaeufe);
                        }
                        // numSteps > maxParallel
                        stepLimit = !_realization.CheckFreeSteps();
                        // numThreads > maxThreads
                        threadLimit = !_processor.CheckFreeThreads();
                        // table accessed
                        targetTablesAccessed = false;
                        foreach (string table in _targetTables)
                        {
                            bool singleTableAccessed = _processor.CheckAccessedTable(table);
                            if (singleTableAccessed)
                            {
                                targetTablesAccessed = true;
                                break;
                            }
                        }
                        srcTablesAccessed = false;
                        foreach (string table in _srcTables)
                        {
                            bool singleTableAccessed = _processor.CheckAccessedTable(table);
                            if (singleTableAccessed)
                            {
                                srcTablesAccessed = true;
                                break;
                            }
                        }
                    }

                    // when satisifed
                    //    increase thread counter (processor)
                    _processor.LockManager.NumberIncDecSemaphore.Wait();
                    usedSem = _processor.LockManager.NumberIncDecSemaphore;
                    _processor.IncreaseThreadNumber(_prozesslaeufe);
                    _increasedNumThreads = true;
                    //    increase step counter (realization)
                    _realization.IncreaseExecutingSteps();
                    _increasedNumSteps = true;
                    //    increase step counter (processor)
                    Task.Run(() => _processor.IncreaseNumExecuting(
                        Level.Step,
                        _id,
                        _locked,
                        this,
                        _prozesslaeufe,
                        _workflow.GetCancelSource()
                    ), _workflow.GetCancelSource().Token).Wait();
                    _processor.LockManager.NumberIncDecSemaphore.Release();
                    usedSem = null;

                    //    add table to used list (processor)
                    foreach (string table in _targetTables)
                    {
                        _processor.AddAccessedTable(table, _workflow, _prozesslaeufe);
                    }
                    foreach (string table in _srcTables)
                    {
                        _processor.AddAccessedTable(table, _workflow, _prozesslaeufe);
                    }
                    _addedTableProcessor = true;
                    //    add table to used list (workflow)
                    foreach (string table in _targetTables)
                    {
                        _workflow.AddAccessedTable(table, _prozesslaeufe);
                    }
                    foreach (string table in _srcTables)
                    {
                        _workflow.AddAccessedTable(table, _prozesslaeufe);
                    }
                    _addedTableWorkflow = true;
                    //    increase executing step number (processor)
                    //        want to lock for this step?
                    //            Y: get semaphore -> execute
                    //            N: Is another step executing?
                    //                Y: execute
                    //                N: get semaphore -> execute

                    if (waited)
                        Task.Run(() => StepLog(
                            $"Step can be started! ({announced} | {stepLimit} | {threadLimit} | " +
                            $"{targetTablesAccessed} | {srcTablesAccessed})"
                        )).Wait();
                }
                catch (Exception e)
                {
                    SemaphoreSlim tmp = usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Step.Init",
                        "Failed waiting for satisfied conditions to start!",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

                // removing from queues
                try
                {
                    if (_addedToQueue)
                    {
                        // remove from step queue
                        _processor.QueueManager.RemoveFromQueue(Level.Step, _id, _prozesslaeufe);
                        _addedToQueue = false;
                        // remove from table queue
                        foreach (string table in _targetTables)
                        {
                            _processor.QueueManager.RemoveFromTableQueue(_id, table, _prozesslaeufe);
                        }
                        foreach (string table in _srcTables)
                        {
                            _processor.QueueManager.RemoveFromTableQueue(_id, table, _prozesslaeufe);
                        }
                        _addedToTableQueue = false;
                    }
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Step.Start",
                        "Failed removing step from process and/or table queue!",
                        ref DummySem,
                        _prozesslaeufe,
                        _workflow.GetCancelSource()
                    );
                }

                inited = true;
                Task.Run(() =>
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                        StepLog(
                            $"Step {_order} (ID: {_id}) initialized");
                }).Wait();

                // execute
                Task.Run(() => Start(), _workflow.GetCancelSource().Token).Wait();
            } catch (Exception e)
            {
                Task.Run(() =>
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                        StepLog(
                            $"Step failed!");
                }).Wait();
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Step.Init",
                    $"Failed initializing Step with ID {_id}!",
                    ref tmp,
                    _prozesslaeufe,
                    _workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// starts the step by logging the start and executing the steps task. After execution, the step is finished
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        public void Start()
        {
            bool announced = _processor.LockManager.IsAnnounced(Level.Step, _prozesslaeufe);
            Task.Run(() => StepLog(
                    $"Execute Step {_order} (ID: {_id})! (Steplock = " +
                    $"{announced})"
                ), _workflow.GetCancelSource().Token
            ).Wait();

            // log start of step (paketschrittProzesslauf)
            try
            {
                DateTime start = DateTime.Now;
                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketschritt_Prozesslaeufe",
                    _paketschrittProzesslaeufeID,
                    "ETL_Paketschritt_Prozesslaeufe_ID",
                    [
                            new Tuple<string, object>("Startzeitpunkt", start),
                            new Tuple<string, object>("Parallelsperre", _locked),
                            new Tuple<string, object>("Ist_gestartet", true)
                    ],
                    _prozesslaeufe
                )).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Step.Start",
                    "Failed logging start of step",
                    ref tmp,
                    _prozesslaeufe,
                    _workflow.GetCancelSource()
                );
            }

            // execute the step task
            try
            {
                // src_connection, src_type, dst_connection, dst_type
                Tuple<DbConnection?, string, DbConnection, string, string> connections = GetRealizationConnections();

                Task.Run(() => Execute(connections), _workflow.GetCancelSource().Token).Wait();

                // Task.Delay(100).Wait();
                Task.Run(() =>
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                        StepLog(
                            $"Finished Execution of Step {_order} (ID: {_id})",
                            true
                        );
                }).Wait();

                Task.Run(() => Finish(), _workflow.GetCancelSource().Token).Wait();
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Step.Start",
                    "Failed executing step!",
                    ref tmp,
                    _prozesslaeufe,
                    _workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// when the step has finished, the accessed table gets removed from tracking tables (workflow and processor),
        /// the lock flag gets removed (if set), and the counters for threads and steps are decreased (with potential
        /// release of semaphore)
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        private void Finish()
        {
            Task.Run(() => StepLog($"Finished step {_order} (ID: {_id})")).Wait();

            // remove table from accessed tables
            try
            {
                // processor
                foreach (string table in _targetTables)
                {
                    _processor.RemoveAccessedTable(table, _workflow, _prozesslaeufe);
                }
                foreach (string table in _srcTables)
                {
                    _processor.RemoveAccessedTable(table, _workflow, _prozesslaeufe);
                }
                _addedTableProcessor = false;
                // workflow
                foreach (string table in _targetTables)
                {
                    _workflow.RemoveAccessedTable(table, _prozesslaeufe);
                }
                foreach (string table in _srcTables)
                {
                    _workflow.RemoveAccessedTable(table, _prozesslaeufe);
                }
                _addedTableWorkflow = false;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Step.Finish",
                    "Failed removing the accessed table from lists!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // set lock flag to false if lock for this step was active
            try
            {
                if (_locked)
                {
                    // remove the flag to enable parallel step execution
                    _processor.LockManager.RemoveLockFlag(Level.Step, _prozesslaeufe);
                    _announced = false;
                }
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Step.Finish",
                    "Failed Removing the Step lock flag!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // decrease counter -> will automatically release the semaphore
            try
            {
                // decrease number of executing step, number of threads
                _processor.DecreaseNumExecuting(Level.Step, _locked, this, _prozesslaeufe);

                // decrease the number of executing threads
                _processor.DecreaseThreadNumber(_prozesslaeufe);
                _increasedNumThreads = false;

                // decrease number of executing steps within realization
                _realization.DecreaseExecutingSteps();
                _increasedNumSteps = false;

                // remove this step from executing list
                //_realization.RemoveStartedStep(this);
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Step.Finish",
                    $"Failed decreasing the counter for step with ID {_id}",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // log end of step
            try
            {
                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketschritt_Prozesslaeufe",
                    _paketschrittProzesslaeufeID,
                    "ETL_Paketschritt_Prozesslaeufe_ID",
                    [
                        new Tuple<string, object>("Endzeitpunkt", DateTime.Now),
                        new Tuple<string, object>("Ist_abgeschlossen", true),
                        new Tuple<string, object>("Erfolgreich", true)
                    ],
                    _prozesslaeufe
                ));

                Task.Run(() =>
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                        StepLog(
                            $"Ended Step {_order} (ID: {_id})");
                }).Wait();
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Step.Finish",
                    $"Failed logging end of step",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// aborts this step by setting log, removing accessed table, releasing lock and decreasing counters
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        public void Abort()
        {
            Task.Run(() => StepLog($"Aborting Step with ID {_id}")).Wait();

            try
            {
                // log process end
                if (_paketschrittProzesslaeufeID != -1 && _startedExec)
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paketschritt_Prozesslaeufe",
                        _paketschrittProzesslaeufeID,
                        "ETL_Paketschritt_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Ausfuehrungsendzeitpunkt", DateTime.Now)
                        ],
                        _prozesslaeufe
                    )).Wait();

                _processor.WorkflowManager.StepSteerLock.Wait();
                _usedSem = _processor.WorkflowManager.StepSteerLock;

                // remove target tabel from accessed tables lists
                if (_addedTableProcessor)
                {
                    foreach (string table in _targetTables)
                    {
                        _processor.RemoveAccessedTable(table, _workflow, _prozesslaeufe);
                    }
                    foreach (string table in _srcTables)
                    {
                        _processor.RemoveAccessedTable(table, _workflow, _prozesslaeufe);
                    }
                    _addedTableProcessor = false;
                }
                if (_addedTableWorkflow)
                {
                    foreach (string table in _targetTables)
                    {
                        _workflow.RemoveAccessedTable(table, _prozesslaeufe);
                    }
                    foreach (string table in _srcTables)
                    {
                        _workflow.RemoveAccessedTable(table, _prozesslaeufe);
                    }
                    _addedTableWorkflow = false;
                }

                // handle lock
                if (_locked && _announced)
                    _processor.LockManager.RemoveLockFlag(Level.Step, _prozesslaeufe);

                // remove from step queue
                if (_addedToQueue)
                    _processor.QueueManager.RemoveFromQueue(Level.Step, _id, _prozesslaeufe);

                // remove from table queue
                if (_addedToTableQueue)
                {
                    foreach (string table in _targetTables)
                    {
                        _processor.QueueManager.RemoveFromTableQueue(_id, table, _prozesslaeufe);
                    }
                    foreach (string table in _srcTables)
                    {
                        _processor.QueueManager.RemoveFromTableQueue(_id, table, _prozesslaeufe);
                    }
                    _addedToTableQueue = false;
                }

                SemaphoreSlim? usedSem = new(1, 1);
                try
                {
                    _processor.LockManager.NumberIncDecSemaphore.Wait();
                    usedSem = _processor.LockManager.NumberIncDecSemaphore;
                    // decrease executing step num (processor) -> will automatically release semaphore if needed
                    if (IncreasedNumExecSteps)
                        _processor.DecreaseNumExecuting(
                            Level.Step, _locked, this, _prozesslaeufe);

                    // decrease number executing steps (realization)
                    if (_increasedNumSteps)
                        _realization.DecreaseExecutingSteps();

                    // decrease thread counter
                    if (_increasedNumThreads)
                        _processor.DecreaseThreadNumber(_prozesslaeufe);

                    _processor.LockManager.NumberIncDecSemaphore.Release();
                    usedSem = null;
                } catch (Exception e)
                {
                    usedSem?.Release();

                    throw new ETLException(
                        _processor,
                        "Failed Decreasing the counters!", "Step.Abort",
                        e,
                        _prozesslaeufe
                    );
                }

                // log failure for step
                try
                {
                    if (_paketschrittProzesslaeufeID != -1)
                        Task.Run(() => UpdateLog(
                            _processor,
                            "Logging.ETL_Paketschritt_Prozesslaeufe",
                            _paketschrittProzesslaeufeID,
                            "ETL_Paketschritt_Prozesslaeufe_ID",
                            [
                                new Tuple<string, object>("Endzeitpunkt", DateTime.Now),
                                new Tuple<string, object>("Ist_abgeschlossen", true)
                            ],
                            _prozesslaeufe
                        )).Wait();
                }
                catch (Exception e)
                {
                    if (e is not TaskCanceledException)
                    {
                        if (e is AggregateException exception)
                        {
                            HandleAggregateException(_processor, exception);
                        }
                        else
                        {
                            Task.Run(() => Log(
                                _processor,
                                $"Catched unexpected error ({e.GetType()})",
                                _prozesslaeufe
                            )).Wait();
                            throw;
                        }
                    }
                }

                try
                {
                    _processor.WorkflowManager.StepSteerLock.Release();
                    _usedSem = null;
                }
                catch
                {
                    Task.Run(() => StepLog("Could not release lock! No error!", true)).Wait();
                }
            }
            catch (Exception e)
            {
                try
                {
                    _processor.WorkflowManager.StepSteerLock.Release();
                    _usedSem = null;
                }
                catch
                {
                    Task.Run(() => StepLog("Could not release lock! No error!", true)).Wait();
                }
                Task.Run(() => SafeExit(
                    _processor,
                    e,
                    $"Step.Abort (ID: {_id})",
                    _prozesslaeufe
                ));
                return;
            }

            Task.Run(() =>
            {
                StepLog($"Step with ID {_id} aborted");
            }).Wait();
        }

        /// <summary>
        /// checks the type of execution for this step and calls the corresponding function in command Executor
        /// </summary>
        /// <param name="connections">Tuple containing all necessary information to open connection to target and
        /// source</param>
        /// <exception cref="ETLException">in case of any error</exception>
        private void Execute(Tuple<DbConnection?, string, DbConnection, string, string> connections)
        {
            try
            {
                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketschritt_Prozesslaeufe",
                    _paketschrittProzesslaeufeID,
                    "ETL_Paketschritt_Prozesslaeufe_ID",
                    [new Tuple<string, object>("Ausfuehrungsstartzeitpunkt", DateTime.Now)],
                    _prozesslaeufe
                )).Wait();

                _startedExec = true;

                switch (_commandType)
                {
                    case "COPY":
                        if (_processor.Debug)
                        {
                            Task.Run(() => Log(
                                _processor,
                                $"Running COPY (SQL) (timeslice = {_timeslice})",
                                _prozesslaeufe
                            )).Wait();
                        }
                        if (_timeslice)
                        {
                            Task.Run(() => _executor.CopyDataTimesliced(
                                connections.Item1,
                                connections.Item3,
                                _realizationConfigID ?? throw new ETLException("Konfiguration_ID not set yet"),
                                _command,
                                _targetTables[0],  // we expect for copy only one table
                                connections.Item2,
                                connections.Item4,
                                connections.Item5,
                                _prozesslaeufe,
                                _workflow,
                                _processor.Debug
                            ), _workflow.GetCancelSource().Token).Wait();
                        } else
                        {
                            Task.Run(() => _executor.CopyData(
                                connections.Item1,
                                connections.Item3,
                                _realizationConfigID ?? throw new ETLException("Konfiguration_ID not set yet"),
                                _command,
                                _targetTables[0],  // we expect for copy only one table
                                connections.Item2,
                                connections.Item4,
                                connections.Item5,
                                _prozesslaeufe,
                                _workflow,
                                _processor.Debug
                            ), _workflow.GetCancelSource().Token).Wait();
                        }
                        if (_processor.Debug)
                        {
                            Task.Run(() => Log(
                                _processor,
                                "FINISHED COPY",
                                _prozesslaeufe
                            )).Wait();
                        }
                        break;
                    case "TRANSFER":
                        Task.Run(() => Log(
                            _processor,
                            $"Running TRANSFER ({_taskType})",
                            _prozesslaeufe,
                            _processor.Debug
                        )).Wait();

                        switch (_taskType)
                        {
                            case "EXCEL":
                                Task.Run(() => _executor.TransferDataFromExcelToDB(
                                    _command,
                                    _targetTables[0],  // we expect for TRANSFER only one table
                                    connections.Item3,
                                    connections.Item4,  // destination connection and destination type
                                    _prozesslaeufe,
                                    _workflow,
                                    _processor.Debug
                                ), _workflow.GetCancelSource().Token).Wait();
                                break;
                            case "CSV":
                                _command = ReplacePlaceholder(
                                    _processor,
                                    _workflow,
                                    _command,
                                    _prozesslaeufe,
                                    _processor.Debug
                                );
                                Task.Run(() => StepLog(_command)).Wait();
                                Task.Run(() => _executor.TransferDBToCSV(
                                    _command,
                                    _realizationConfigID ?? throw new ETLException("Konfiguration_ID not set yet"),
                                    _targetTables[0],
                                    _parameterID,
                                    connections.Item3,
                                    _prozesslaeufe,
                                    _workflow
                                ), _workflow.GetCancelSource().Token).Wait();
                                break;
                            default:
                                throw new ETLException(
                                    _processor,
                                    $"Unknown Tasktype {_taskType}!",
                                    "Execute",
                                    _prozesslaeufe
                                );
                        }

                        Task.Run(() => Log(
                            _processor,
                            $"FINISHED TRANSFER ({_taskType})",
                            _prozesslaeufe,
                            _processor.Debug
                        )).Wait();

                        break;
                    case "EXEC":
                        throw new NYIException(
                            _processor,
                            "Running EXEC NYI",
                            "Execute",
                            _prozesslaeufe
                        );  // TODO
                    case "SQL_TARGET":
                        if (_processor.Debug)
                        {
                            Task.Run(() => Log(
                                _processor,
                                "Running SQL_TARGET",
                                _prozesslaeufe
                            )).Wait();
                        }
                        _command = ReplacePlaceholder(
                            _processor,
                            _workflow,
                            _command,
                            _prozesslaeufe,
                            _processor.Debug
                        );
                        Task.Run(() => _executor.ExecuteCommand(
                            connections.Item4,
                            connections.Item3,
                            _realizationConfigID ?? throw new ETLException("Konfiguration_ID not set yet"),
                            _command,
                            _prozesslaeufe,
                            _workflow,
                            _processor.Debug
                        ), _workflow.GetCancelSource().Token).Wait();
                        if (_processor.Debug)
                        {
                            Task.Run(() => Log(
                                _processor,
                                "FINISHED SQL_TARGET",
                                _prozesslaeufe
                            )).Wait();
                        }
                        break;
                    case "SQL_SOURCE":
                        throw new NYIException(
                            _processor,
                            "Running SQL_SOURCE NYI",
                            "Execute",
                            _prozesslaeufe
                        );  // TODO
                    case "TEST":
                        Task.Run(() => _executor.RunDummy(
                            int.Parse(_command),
                            _prozesslaeufe
                        ), _workflow.GetCancelSource().Token).Wait();
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"Unknown Task Type ({_taskType})",
                            "Execute",
                            _prozesslaeufe
                        );
                }

                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketschritt_Prozesslaeufe",
                    _paketschrittProzesslaeufeID,
                    "ETL_Paketschritt_Prozesslaeufe_ID",
                    [
                        new Tuple<string, object>("Ausfuehrungsendzeitpunkt", DateTime.Now)
                    ],
                    _prozesslaeufe
                )).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Execute",
                    $"Failed executing task for step with ID {_id}!",
                    ref tmp,
                    _prozesslaeufe,
                    _workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// determine the connection information of source and destination for package
        /// </summary>
        /// <returns>Tuple including connection to source (+type) and destination (+type)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private Tuple<DbConnection?, string, DbConnection, string, string> GetRealizationConnections()
        {
            // read paket
            DataTable realizations;
            try
            {
                realizations = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    "SELECT * FROM pc.ETL_Paket_Umsetzungen",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    $"Retrieving realization information failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // select right package
            DataRow selectedRealizations;
            try
            {
                selectedRealizations = DBHelper.GetDataRowFromDataTable(
                    _processor, realizations,
                    $"ETL_Paket_Umsetzungen_ID = {_realization.GetID()}",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    $"Selecting package failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get configuration
            try
            {
                _realizationConfigID = selectedRealizations["ETL_Konfigurationen_ID"].ToString();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Extracting konfiguration ID failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get all configurations
            DataTable configs;
            try
            {
                configs = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    "SELECT * FROM pc.ETL_Konfigurationen",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Retrieving configurations failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // select right configuration
            DataRow selectedConfigurations;
            try
            {
                selectedConfigurations = DBHelper.GetDataRowFromDataTable(
                    _processor, configs, $"ETL_Konfigurationen_ID = {_realizationConfigID}",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Selecting configuration failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get src and dst connection ID
            string srcConnectionID;
            string dstConnectionID;
            try
            {
                srcConnectionID = selectedConfigurations["Quell_ETL_Verbindungen_ID"].ToString() ??
                                    throw new ETLException("No Quell_ETL_Verbindungen_ID");
                dstConnectionID = selectedConfigurations["Ziel_ETL_Verbindungen_ID"].ToString() ??
                                    throw new ETLException("No Ziel_ETL_Verbindungen_ID");
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Extracting connection ID for source and/or destination failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // add configurations and connection IDs to paket prozesslaeufe
            try
            {
                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketumsetzung_Prozesslaeufe",
                    _realization.GetPaketumsetzungProzesslaeufeID(),  // paketumsetzungProzesslaeufeID
                    "ETL_Paketumsetzung_Prozesslaeufe_ID",
                    [
                        new Tuple<string, object>("ETL_Konfigurationen_ID", _realizationConfigID ??
                            throw new ETLException("No ETL_Konfigurationen_ID")),
                        new Tuple<string, object>("Quell_ETL_Verbindungen_ID", srcConnectionID),
                        new Tuple<string, object>("Ziel_ETL_Verbindungen_ID", dstConnectionID),
                        new Tuple<string, object>("Mandanten_ID", _realization.GetMandantenID())
                    ],
                    _prozesslaeufe
                )).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Adding Configuration ID and connection IDs to Paket_Prozesslaufe failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get mandanten connection from IDs
            DataTable mandantenConnections;
            try
            {
                mandantenConnections = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    "SELECT * FROM pc.ETL_Mandanten_Verbindungen",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Retrieving mandanten failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get data connection ID for source
            DataRow selectedSrcConnectionID;
            try
            {
                selectedSrcConnectionID = DBHelper.GetDataRowFromDataTable(
                    _processor, mandantenConnections,
                    $"ETL_Verbindungen_ID = {srcConnectionID} AND Mandanten_ID = {_realization.GetMandantenID()}",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Selecting source mandentenverbindung failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get data connection ID for destination
            DataRow selectedDstConnectionID;
            try
            {
                selectedDstConnectionID = DBHelper.GetDataRowFromDataTable(
                    _processor, mandantenConnections,
                    $"ETL_Verbindungen_ID = {dstConnectionID} AND Mandanten_ID = {_realization.GetMandantenID()}",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Selecting destination mandentenverbindung failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get data conncetions
            DataTable connections;
            try
            {
                connections = _processor.DbHelper.GetDataTableFromQuery(
                    _processor, "SELECT * FROM conf.Datenverbindungen",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Retrieving data connection failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get data connection src
            DataRow selectedSrcConnection;
            try
            {
                selectedSrcConnection = DBHelper.GetDataRowFromDataTable(
                    _processor, connections,
                    $"Datenverbindungen_ID = {selectedSrcConnectionID["Datenverbindungen_ID"]}",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Selecting source data connection failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // extract the data source type
            string srcType;
            try
            {
                srcType = selectedSrcConnection["Datenquellentypen_ID"].ToString() ??
                            throw new ETLException("No Datenquellentypen_ID");
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Extracting source communication type failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get data connection dst
            DataRow selectedDstConnection;
            try
            {
                selectedDstConnection = DBHelper.GetDataRowFromDataTable(
                    _processor, connections,
                    $"Datenverbindungen_ID = {selectedDstConnectionID["Datenverbindungen_ID"]}",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Extracting source communication type failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // extract destination data source type and user
            string dstType;
            string dstUser;
            try
            {
                dstType = selectedDstConnection["Datenquellentypen_ID"].ToString() ??
                            throw new ETLException("No Datenquellentypen_ID");
                dstUser = selectedDstConnection["Benutzer"].ToString() ??
                            throw new ETLException("No Benutzer");
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Extracting destination communication type and user failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get communication type for correct connection establishment
            DataTable communications;
            try
            {
                communications = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    "SELECT * FROM conf.Datenquellentypen",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Retrieving data source type failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get communication type for correct connection establishment (source)
            DataRow srcCommunication;
            try
            {
                srcCommunication = DBHelper.GetDataRowFromDataTable(
                    _processor, communications,
                    $"Datenquellen_Typ_ID = {srcType}",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Selecting data source type failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // get communication type for correct connection establishment (destination)
            DataRow dstCommunication;
            try
            {
                dstCommunication = DBHelper.GetDataRowFromDataTable(
                    _processor, communications,
                    $"Datenquellen_Typ_ID = {dstType}",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Selecting data destination type failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // extract communication name
            string srcCommunicationType;
            string dstCommunicationType;
            try
            {
                srcCommunicationType = srcCommunication["Datenquellentyp"].ToString() ?? throw new ETLException("No Datenquellentyp");
                dstCommunicationType = dstCommunication["Datenquellentyp"].ToString() ?? throw new ETLException("No Datenquellentyp");
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Extracting data source type for source and/or destination failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // create source connection
            DbConnection? srcConnection;
            try
            {
                srcConnection = DBHelper.GetConnection(
                    _processor, srcCommunicationType, selectedSrcConnection,
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Retrieving source connection failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // create destination connection
            DbConnection dstConnection;
            try
            {
                dstConnection = DBHelper.GetConnection(
                    _processor, dstCommunicationType, selectedDstConnection,
                    _prozesslaeufe
                ) ?? throw new ETLException("No Queryresult");
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetRealizationConnections",
                    "Retrieving destination connection failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            return new Tuple<DbConnection?, string, DbConnection, string, string>(
                srcConnection, srcCommunicationType, dstConnection, dstCommunicationType, dstUser
            );
        }
    }
}
