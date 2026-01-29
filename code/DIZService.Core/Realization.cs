using System.Data;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DIZService.Core
{
    public class Realization : Helper
    {
        private readonly Package _package;      // parent package
        private readonly Processor _processor;  // global processor
        private readonly Workflow _workflow;    // parent workflow

        private readonly int _id;                // UmsetzungenID
        private readonly int _maxParallelSteps;  // maximum number of parallel executable steps
        private readonly int _priority;          // priority of this realization
        private readonly int _mandantenID;       // mandantenID to identify correct connection

        private int _paketumsetzungProzesslaeufeID = -1;  // needs to be set when initializing

        private readonly bool _locked;   // true if this realization shall be executed in stand-alone

        private int _executingParallelSteps;        // number of parallel executing steps
        private readonly SemaphoreSlim _execParallelStepsLock = new(1, 1); // inc or dec counter at a time

        private readonly List<Step> _createdSteps = [];  // includes the step objects of this realization
        private readonly SemaphoreSlim _createdStepsLock = new(1, 1);  // inc or dec counter at a time

        private bool _startedExec = false;  // set to true if execution has started

        private bool _announced = false;  // true if realization has announced a lock
        public bool IncreasedNumExecRealizations = false;  // true if the #realizations was increased in processor
        private bool _addedToQueue = false;  // true if realization was added to realization queue

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        // tuple that includes all prozesslaeufeIDs (prozess, paket, paketumsetzung, paketschritt)
        private Tuple<int?, int?, int?, int?> _prozesslaeufe = new(
            null, null, null, null
        );

        public Realization(
            int id,
            Processor processor,
            Workflow workflow,
            Package package,
            int maxParallelSteps,
            int priority,
            bool locked,
            int mandantenID
        ) {
            // set values
            try
            {
                _id = id;
                _processor = processor;
                _workflow = workflow;
                _package = package;
                _maxParallelSteps = maxParallelSteps;
                _priority = priority;
                _locked = locked;
                _mandantenID = mandantenID;

                _prozesslaeufe = new Tuple<int?, int?, int?, int?>(
                    _workflow.GetProzesslaeufeID(),
                    _package.GetPaketProzesslaeufeID(),
                    null,
                    null
                );
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    processor,
                    e,
                    "Realization",
                    $"Failed setting realization attributes",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }

        // ---------- HELPER FUNCTIONS ----------

        /// <summary>
        /// returns the realizationID of this realization
        /// </summary>
        /// <returns>ID</returns>
        public int GetID()
        {
            return _id;
        }

        /// <summary>
        /// returns the MandantenID for this realization
        /// </summary>
        /// <returns></returns>
        public int GetMandantenID()
        {
            return _mandantenID;
        }

        /// <summary>
        /// increases the number of parallel executing steps
        /// </summary>
        /// <exception cref="ETLException">if limit of parallel steps are exceeded</exception>
        public void IncreaseExecutingSteps()
        {
            try
            {
                _execParallelStepsLock.Wait();
                _usedSem = _execParallelStepsLock;

                if (_executingParallelSteps + 1 > _maxParallelSteps)
                    throw new ETLException(
                        _processor,
                        "Increasing the number of executing steps would exceed the allowed parallel steps!",
                        "IncreaseExecutingSteps",
                        _prozesslaeufe
                    );

                _executingParallelSteps++;

                _execParallelStepsLock.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization",
                    $"Failed increasing the number of executing steps for realization!",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// decreases the number of parallel executing steps
        /// </summary>
        /// <exception cref="ETLException">in case of negative number of steps after decrease</exception>
        public void DecreaseExecutingSteps()
        {
            try
            {
                _execParallelStepsLock.Wait();
                _usedSem = _execParallelStepsLock;

                if (_executingParallelSteps - 1 < 0)
                    throw new ETLException(
                        _processor,
                        "Decreasing the executing step number would be negativ -> Impossible!",
                        "DecreaseExecutingSteps",
                        _prozesslaeufe
                    );

                _executingParallelSteps--;
                _execParallelStepsLock.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization",
                    $"Failed decreasing the number of executing steps for realization!",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// returns the priorization of this realization
        /// </summary>
        /// <returns>int</returns>
        public int GetPriorization()
        {
            return _priority;
        }

        /// <summary>
        /// returns the PaketProzesslaeufeID of this realization
        /// </summary>
        /// <returns>int</returns>
        public int GetPaketumsetzungProzesslaeufeID()
        {
            return _paketumsetzungProzesslaeufeID;
        }

        /// <summary>
        /// checks if another step can be executed in parallel
        /// </summary>
        /// <returns>true if another step can be executed</returns>
        public bool CheckFreeSteps()
        {
            return _executingParallelSteps < _maxParallelSteps;
        }

        /// <summary>
        /// removes the given step from started list
        /// </summary>
        /// <param name="step">step to remove</param>
        /// <exception cref="ETLException">when step cannot be removed</exception>
        public void RemoveStartedStep(Step step)
        {
            try
            {
                _createdStepsLock.Wait();
                _usedSem = _createdStepsLock;

                _createdSteps.RemoveAll(item => item == step);
                _createdStepsLock.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "RemoveStartedStep",
                    "Failed removing step from realization list",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// removes the given step to started list
        /// </summary>
        /// <param name="step">step to add</param>
        /// <exception cref="ETLException">when step cannot be added</exception>
        private void AddStartedStep(Step step)
        {
            try
            {
                _createdStepsLock.Wait();
                _usedSem = _createdStepsLock;

                _createdSteps.Add(step);

                _createdStepsLock.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "AddStartedStep",
                    "Failed adding step to realization list",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// wraps the logging process to automatically add given information
        /// </summary>
        /// <param name="message">message to log</param>
        /// <param name="debug">true to log when processor debug flag is set</param>
        /// <param name="type">EventType to log (Default=Information)</param>
        private void RealizationLog(
            string message,
            bool debug = false,
            LogType type = LogType.Info
        )
        {
            if (_paketumsetzungProzesslaeufeID == -1)
            {
                Task.Run(() => Log(
                    _processor,
                    message,
                    _prozesslaeufe,
                    !debug || _processor.Debug,
                    type: type
                )).Wait();
            } else
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

        // ---------- REALIZATION FUNCTIONS ----------

        /// <summary>
        /// initializes the logging, adds the realization to queue (if a lock is announced) and waits until realization
        /// first in queue. Will then announce lock for this realization if needed and will then wait to get started
        /// (released lock). When realization can be started it increases the counter (optionally waits for semaphore),
        /// remove from queue (if added) and start this realization
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        public void Init()
        {
            // init logging
            Task.Run(() =>
            {
                _paketumsetzungProzesslaeufeID = InitializeLogging(
                    _processor,
                    "Logging.ETL_Paketumsetzung_Prozesslaeufe",
                    [
                        new Tuple<string, int>("ETL_Prozesslaeufe_ID", _workflow.GetProzesslaeufeID()),
                        new Tuple<string, int>("ETL_Paket_Prozesslaeufe_ID", _package.GetPaketProzesslaeufeID()),
                        new Tuple<string, int>("ETL_Paket_Umsetzungen_ID", _id),
                    ],
                    DateTime.Now,
                    _prozesslaeufe
                );
            }, _workflow.GetCancelSource().Token).Wait();

            _prozesslaeufe = new Tuple<int?, int?, int?, int?>(
                _workflow.GetProzesslaeufeID(),
                _package.GetPaketProzesslaeufeID(),
                _paketumsetzungProzesslaeufeID,
                null
            );

            Task.Run(() => RealizationLog($"Initialize realization {_id}"), _workflow.GetCancelSource().Token).Wait();

            // lock announced?
            try
            {
                if (_processor.LockManager.IsAnnounced(Level.Realization, _prozesslaeufe))
                {
                    _processor.QueueManager.AddToQueue(Level.Realization, _id, _prozesslaeufe);
                    _addedToQueue = true;
                }
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Init",
                    "Adding realization to queue failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // was realization added to queue? -> wait for 1st position in realization queue
            try
            {
                if (_addedToQueue)
                {
                    bool waited = false;

                    _processor.WorkflowManager.RealizationSteerLock.Wait(_workflow.GetCancelSource().Token);
                    _usedSem = _processor.WorkflowManager.RealizationSteerLock;

                    bool first = _processor.QueueManager.CheckQueueFirst(Level.Realization, _id, _prozesslaeufe);
                    while (!first)
                    {
                        _processor.WorkflowManager.RealizationSteerLock.Release();
                        _usedSem = null;

                        waited = true;

                        int maxWait = 5;
                        DateTime jetzt = DateTime.Now;
                        bool onceSend = false;

                        if (_processor.Debug || (DateTime.Now - jetzt).TotalMinutes >= maxWait || !onceSend)
                        {
                            Task.Run(() => RealizationLog(
                                $"Wait for first spot in realization queue ({first})!",
                                true
                            ), _workflow.GetCancelSource().Token).Wait();
                            jetzt = DateTime.Now;
                            onceSend = true;
                        }

                        // Task.Delay(2 * 1000).Wait();
                        Task.Delay(_processor.WaitingTime).Wait(_workflow.GetCancelSource().Token);

                        _processor.WorkflowManager.RealizationSteerLock.Wait(_workflow.GetCancelSource().Token);
                        _usedSem = _processor.WorkflowManager.RealizationSteerLock;

                        first = _processor.QueueManager.CheckQueueFirst(Level.Realization, _id, _prozesslaeufe);
                    }

                    _processor.WorkflowManager.RealizationSteerLock.Release();
                    _usedSem = null;

                    if (waited)
                        Task.Run(() => RealizationLog(
                            $"Realization {_id} first in queue",
                            true
                        ), _workflow.GetCancelSource().Token).Wait();
                }
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Init",
                    "Waiting for first position in queue failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // lock process for this realization?
            if (_locked)
            {
                Task.Run(() => RealizationLog(
                    $"Handle Lock for realization {_id}"
                ), _workflow.GetCancelSource().Token).Wait();

                try
                {
                    //    announce lock
                    _processor.LockManager.AnnounceLock(Level.Realization, _prozesslaeufe);
                    _announced = true;
                }
                catch (Exception e)
                {
                    SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Realization.Init",
                        $"Failed locking process for realization",
                        ref tmp,
                        _prozesslaeufe
                    );
                }
            }

            // wait until possible lock has been released
            try
            {
                bool waited = false;
                bool announced = false;
                _processor.WorkflowManager.RealizationSteerLock.Wait(_workflow.GetCancelSource().Token);
                _usedSem = _processor.WorkflowManager.RealizationSteerLock;

                if (!_locked)
                {
                    announced = _processor.LockManager.IsAnnounced(Level.Realization, _prozesslaeufe);

                    if (announced)
                    {
                        Task.Run(() => RealizationLog(
                            $"Wait until realization {_id} can be started! (Lock announced: {announced})",
                            true
                        ), _workflow.GetCancelSource().Token).Wait();
                        while (announced)
                        {
                            _processor.WorkflowManager.RealizationSteerLock.Release();
                            _usedSem = null;

                            waited = true;
                            // Task.Delay(2 * 1000).Wait();
                            Task.Delay(_processor.WaitingTime).Wait(_workflow.GetCancelSource().Token);

                            _processor.WorkflowManager.RealizationSteerLock.Wait(_workflow.GetCancelSource().Token);
                            _usedSem = _processor.WorkflowManager.RealizationSteerLock;

                            announced = _processor.LockManager.IsAnnounced(Level.Realization, _prozesslaeufe);
                        }
                    }
                }

                // increase number of executing realizations
                try
                {
                    Task.Run(() => _processor.IncreaseNumExecuting(
                        Level.Realization,
                        _id,
                        _locked,
                        this,
                        _prozesslaeufe,
                        _workflow.GetCancelSource()
                    ), _workflow.GetCancelSource().Token).Wait();
                    _usedSem = null;
                }
                catch (Exception e)
                {
                    SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Realization.Init",
                        "Failed increasing the number of executing realizations!",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

                if (waited)
                    Task.Run(() => RealizationLog(
                        $"Realization can be started! ({announced})"
                    ), _workflow.GetCancelSource().Token).Wait();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Init",
                    "Failed Waiting for released Lock",
                    ref DummySem,
                    _prozesslaeufe
                );
            }

            // remove realization from queue (if added)
            try
            {
                if (_addedToQueue)
                {
                    _processor.QueueManager.RemoveFromQueue(Level.Realization, _id, _prozesslaeufe);
                    _addedToQueue = false;
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Init",
                    $"Failed removing realization with ID {_id} from queue!",
                    ref DummySem,
                    _prozesslaeufe
                );
            }

            Task.Run(() =>
            {
                RealizationLog($"Realization {_id} initialized", true);
            }, _workflow.GetCancelSource().Token).Wait();

            // execute
            try
            {
                Task.Run(() => Start(), _workflow.GetCancelSource().Token).Wait();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Init",
                    $"Failed running the realization with ID {_id} ({e.GetType()})",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// logs start of realization, reads the steps to execute (sorted), create and initialize steps (with adding to
        /// tracking list in workflow). After finshing all steps the realization will finish
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        private void Start()
        {
            bool announced = _processor.LockManager.IsAnnounced(Level.Realization, _prozesslaeufe);
            Task.Run(() => RealizationLog(
                    $"Execute Realization {_id}! (Realizationlock = " +
                    $"{announced})"
                ), _workflow.GetCancelSource().Token
            ).Wait();

            // log start of realization (paket_prozesslauf)
            try
            {
                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketumsetzung_Prozesslaeufe",
                    _paketumsetzungProzesslaeufeID,
                    "ETL_Paketumsetzung_Prozesslaeufe_ID",
                    [
                        new Tuple<string, object>("Startzeitpunkt", DateTime.Now),
                        new Tuple<string, object>("Parallelsperre", _locked),
                        new Tuple<string, object>("Ist_gestartet", true)
                    ],
                    _prozesslaeufe
                ), _workflow.GetCancelSource().Token).Wait();
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Start",
                    $"Failed logging start of realization {_id}",
                    ref DummySem,
                    _prozesslaeufe
                );
            }

            // execute
            // read the steps for realization
            DataTable realizationSteps = new();
            try
            {
                DataTable steps = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * " +
                    $"FROM pc.ETL_PAKETUMSETZUNGEN_PAKETSCHRITTE " +
                    $"WHERE ETL_Paket_Umsetzungen_ID = {_id} AND ETL_Workflow_ID = {_workflow.GetID()} AND " +
                    $"      Ist_aktiv = 1",
                    _prozesslaeufe
                );

                // read steps if steps applied to realization
                if (steps.Rows.Count > 0)
                {
                    List<int> stepIDs = [];
                    foreach (DataRow step in steps.Rows)
                    {
                        stepIDs.Add(Convert.ToInt32(step["ETL_PAKETSCHRITTE_ID"].ToString()));
                    }

                    string stepIDsList = ConvertListToString(
                        _processor,
                        stepIDs,
                        _prozesslaeufe
                    );

                    // TODO: join abhängigkeit and get reihenfolge from abhängigkeit
                    realizationSteps = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT A.*, " +
                        $"       ab.Schritt_Reihenfolge " +
                        $"FROM pc.ETL_PAKETSCHRITTE AS A " +
                        $"LEFT JOIN pc.ETL_Paketumsetzungen_Paketschritte AS ab " +
                        $"ON A.ETL_Paketschritte_ID = ab.ETL_Paketschritte_ID AND ab.ETL_Paket_Umsetzungen_ID = {_id}" +
                        $"   AND ab.ETL_Workflow_ID = {_workflow.GetID()} " +
                        $"WHERE A.ETL_Paketschritte_ID IN ({stepIDsList}) AND A.IST_AKTIV = 1 AND ab.Ist_Aktiv = 1",
                        _prozesslaeufe
                    );

                    // sorting the steps
                    try
                    {
                        realizationSteps.DefaultView.Sort = "Schritt_Reihenfolge ASC";
                        realizationSteps = realizationSteps.DefaultView.ToTable();
                    }
                    catch (Exception e)
                    {
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "Realization.Start",
                            "Failed sorting the realization steps!",
                            ref DummySem,
                            _prozesslaeufe
                        );
                    }
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Start",
                    "Retrieving package steps failed!",
                    ref DummySem,
                    _prozesslaeufe
                );
            }

            Task.Run(() => RealizationLog(
                $"Number of Steps: {realizationSteps.Rows.Count}"), _workflow.GetCancelSource().Token).Wait();

            // create steps
            foreach (DataRow stepRow in realizationSteps.Rows)
            {
                // extract needed information
                string command, commandType, taskType;
                bool stepLocked, timeslice;
                int stepID, order;
                int? parameterID;
                // lists all tables accessed by step
                List<string> targetTables = [];
                List<string> srcTables = [];
                try
                {
                    command = stepRow["BEFEHL"].ToString() ?? throw new ETLException("No Befehl");  // SQL command
                    // type of command (COPY, EXEC, SQL_TARGET, SQL_SOURCE)
                    commandType = stepRow["BEFEHLSTYP"].ToString() ?? throw new ETLException("No Befehl");
                    // == SQL, EXCEL, CSV  --> TODO in future when more tasks exist
                    taskType = stepRow["AUFGABENTYP"].ToString() ?? throw new ETLException("No Befehl");
                    // tables to insert data into (list of string)
                    targetTables = [.. (stepRow["ZIELTABELLE"].ToString() ??
                                    throw new ETLException("No ZIELTABELLE")).Split(',')];
                    srcTables = [.. (stepRow["QUELLTABELLE"].ToString() ??
                                    throw new ETLException("No QUELLTABELLE")).Split(',')];
                    timeslice = bool.Parse(stepRow["ZEITSCHEIBE"].ToString() ??
                                    throw new ETLException("No ZEITSCHEIBE"));  // run command in time slices
                    order = int.Parse(stepRow["Schritt_Reihenfolge"].ToString() ??
                                    throw new ETLException("No Schritt_Reihenfolge"));  // order of step
                    stepID = int.Parse(stepRow["ETL_Paketschritte_ID"].ToString() ??
                                    throw new ETLException("No ETL_Paketschritte_ID"));

                    try
                    {
                        DataRow parameterRow = _processor.DbHelper.GetDataTableFromQuery(
                            _processor,
                            $"SELECT ETL_Paketschritt_Parameter_ID " +
                            $"FROM pc.ETL_Paketschritte_Paketschritt_Parameter " +
                            $"WHERE Ist_Aktiv = 1 AND ETL_Paketschritte_ID = {stepID} AND " +
                            $"      ETL_Workflow_ID = {_workflow.GetID()}",
                            _prozesslaeufe
                        ).Rows[0];
                        parameterID = Convert.ToInt32(parameterRow["ETL_Paketschritt_Parameter_ID"]);
                    } catch
                    {
                        parameterID = null;
                    }

                    // signalizes that this step cannot run in parallel
                    stepLocked = bool.Parse(stepRow["Parallelsperre"].ToString() ??
                                    throw new ETLException("No Parallelsperre"));
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Realization.Start",
                        "Failed extracting the step information",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // create new step and add it to tracking list
                try
                {
                    Step step = new(
                        stepID,
                        _processor,
                        _workflow,
                        _package,
                        this,
                        command,
                        commandType,
                        taskType,
                        srcTables,
                        targetTables,
                        order,
                        stepLocked,
                        timeslice,
                        parameterID
                    );
                    AddStartedStep(step);
                    _workflow.AddPaketschrittProzesslaeufeID(step.GetPaketschrittProzesslaeufeID(), _prozesslaeufe);
                } catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Realization.Start",
                        "Failed creating the new Step and adding it to tracking list!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }
            }

            // log start of realization (paket_prozesslauf)
            try
            {
                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketumsetzung_Prozesslaeufe",
                    _paketumsetzungProzesslaeufeID,
                    "ETL_Paketumsetzung_Prozesslaeufe_ID",
                    [
                        new Tuple<string, object>("Ausfuehrungsstartzeitpunkt", DateTime.Now)
                    ],
                    _prozesslaeufe
                ), _workflow.GetCancelSource().Token).Wait();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Start",
                    $"Failed logging execution start of realization {_id}",
                    ref DummySem,
                    _prozesslaeufe
                );
            }

            _startedExec = true;

            List<Task> startedStepTasks = [];
            // wait for finished steps and finish realization
            try
            {
                startedStepTasks = [];
                // needed since original list could be changed while working on it
                Task.Run(() =>
                {
                    for (int i = 0; i < _createdSteps.Count; i++)
                    {
                        // start step execution including counter increasing etc.
                        bool inited = false;
                        Task stepTask = Task.Run(
                            () => _createdSteps[i].Init(out inited),
                            _workflow.GetCancelSource().Token);

                        while (!inited)
                        {
                            Task.Delay(5).Wait();

                            if (_workflow.GetCancelSource().Token.IsCancellationRequested)
                                break;
                        }

                        startedStepTasks.Add(stepTask);
                        _workflow.AddExecutingTask($"Step_{_createdSteps[i].GetID()}", stepTask, _prozesslaeufe);

                        if (_workflow.GetCancelSource().Token.IsCancellationRequested)
                            break;
                    }
                }, _workflow.GetCancelSource().Token).Wait();

                Task.Run(() => Log(
                    _processor, "Wait until steps finished", _prozesslaeufe)).Wait();
                Task.WaitAll([.. startedStepTasks], _workflow.GetCancelSource().Token);
                Task.Run(() => Log(
                    _processor, "Steps finished", _prozesslaeufe)).Wait();

                try
                {
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paketumsetzung_Prozesslaeufe",
                        _paketumsetzungProzesslaeufeID,
                        "ETL_Paketumsetzung_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Ausfuehrungsendzeitpunkt", DateTime.Now)
                        ],
                        _prozesslaeufe
                    ), _workflow.GetCancelSource().Token).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Realization.Start",
                        $"Failed logging execution start of realization {_id}",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                Task.Run(() => Finish(), _workflow.GetCancelSource().Token).Wait();
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Start",
                    $"Failed waiting for end of all steps and finishing this realization with ID {_id} " +
                    $"(ExceptionType: {e.GetType()})",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// finalizes execution of realization.This includes removing lock (if it was set), decreasing the number of
        /// executing realizations, removing the realization from package tracking list
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        private void Finish()
        {
            Task.Run(() => RealizationLog($"Start finishing realization {_id}")).Wait();
            _processor.WorkflowManager.RealizationSteerLock.Wait();
            _usedSem = _processor.WorkflowManager.RealizationSteerLock;

            // set lock flag to false if lock for this realization was active
            try
            {
                if (_locked)
                {
                    // remove flag to enable parallel realization execution
                    _processor.LockManager.RemoveLockFlag(Level.Realization, _prozesslaeufe);
                    _announced = false;
                }
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Finish",
                    "Failed removing the lock flag on realization level",
                    ref _usedSem,
                    _prozesslaeufe
                );
            }

            // decrease number of executing realizations -> will automatically release semaphore
            try
            {
                _processor.DecreaseNumExecuting(
                    Level.Realization,
                    _locked,
                    this,
                    _prozesslaeufe
                );
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Finish",
                    "Failed decreasing the number of executing realizations",
                    ref _usedSem,
                    _prozesslaeufe
                );
            }

            // remove realization from package realization list
            try
            {
                _package.RemoveRealization(this);
                _processor.WorkflowManager.RealizationSteerLock.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Finish",
                    "Failed removing realization from package list",
                    ref tmp,
                    _prozesslaeufe
                );
            }

            // log end of realization
            try
            {
                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketumsetzung_Prozesslaeufe",
                    _paketumsetzungProzesslaeufeID,
                    "ETL_Paketumsetzung_Prozesslaeufe_ID",
                    [
                        new Tuple<string, object>("Endzeitpunkt", DateTime.Now),
                        new Tuple<string, object>("Ist_abgeschlossen", true),
                        new Tuple<string, object>("Erfolgreich", true)
                    ],
                    _prozesslaeufe
                ), _workflow.GetCancelSource().Token);

                Task.Run(() =>
                {
                    RealizationLog($"Finished Realization {_id}");
                }, _workflow.GetCancelSource().Token).Wait();
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Realization.Finish",
                    "Failed logging end of realization",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// aborts this realization by setting log, aborting child steps, releasing lock, decreasing counter and
        /// removing from queue
        /// </summary>
        public void Abort()
        {
            Task.Run(() => RealizationLog($"Aborting Realization with ID {_id}")).Wait();

            try
            {
                // log process end
                if (_paketumsetzungProzesslaeufeID != -1 && _startedExec)
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paketumsetzung_Prozesslaeufe",
                        _paketumsetzungProzesslaeufeID,
                        "ETL_Paketumsetzung_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Ausfuehrungsendzeitpunkt", DateTime.Now)
                        ],
                        _prozesslaeufe
                    )).Wait();

                // initialize abort for all running steps
                List<Task> abortions = [];
                foreach (Step step in _createdSteps)
                {
                    abortions.Add(Task.Run(() => step.Abort()));
                }

                // wait for all finished abortions
                Task.WaitAll([.. abortions]);
                Task.Run(() => RealizationLog($"All Steps of Realization with ID {_id} aborted!")).Wait();

                // handle lock
                if (_locked && _announced)
                    _processor.LockManager.RemoveLockFlag(Level.Realization, _prozesslaeufe);

                // remove from queue
                if (_addedToQueue)
                    _processor.QueueManager.RemoveFromQueue(Level.Realization, _id, _prozesslaeufe);

                // decrease executing realization num -> will automatically release semaphore if needed
                if (IncreasedNumExecRealizations)
                    _processor.DecreaseNumExecuting(
                        Level.Realization,
                        _locked,
                        this,
                        _prozesslaeufe
                    );

                // log error
                if (_paketumsetzungProzesslaeufeID != -1)
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paketumsetzung_Prozesslaeufe",
                        _paketumsetzungProzesslaeufeID,
                        "ETL_Paketumsetzung_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Endzeitpunkt", DateTime.Now),
                            new Tuple<string, object>("Ist_abgeschlossen", true)
                        ],
                        _prozesslaeufe
                    )).Wait();
            } catch (Exception e)
            {
                Task.Run(() => SafeExit(
                    _processor,
                    e,
                    $"Realization.Abort (ID: {_id})",
                    _prozesslaeufe
                ));
                return;
            }

            Task.Run(() =>
            {
                RealizationLog($"Realization with ID {_id} aborted");
            }).Wait();
        }
    }
}
