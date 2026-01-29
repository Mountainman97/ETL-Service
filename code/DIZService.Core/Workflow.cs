using System.Data;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DIZService.Core
{
    public class Workflow : Helper
    {
        private readonly int _id = -1;  // workflowID

        private int _prozesslaeufeID = -1;
        private int _zeitplanAusfuehrungenID = -1;
        private int _masterPackageID = -1;  // ID of master package
        private int _fallbackPackageID = -1;  // ID of package to execute on failure
        private bool _locked;  // true if this workflow shall lock execution

        // lists all email addresses to send result mail in cc to
        private readonly List<string> _emailReceiver = [];  // will include email adress of workflow owner
        private readonly List<string> _ccReceiver = [];

        private DateTime _takeoverFrom;
        private DateTime _takeoverTo;

        private bool _startedExec = false;  // set to true if execution has started

        private readonly Processor _processor;  // processor controlling this workflow
        private CancellationTokenSource _cancleSource;  // use to controllable stop the workflow

        // list of packages running in this workflow with semaphore to grant unique access
        private readonly List<Package> _packages = [];
        private readonly SemaphoreSlim _lockPackagesList = new(1, 1);

        // lists all IDs of started realizations
        private readonly List<int> _startedPaketProzesslaeufeIDs = [];
        private readonly List<int> _startedPaketschrittProzesslaeufeIDs = [];

        // lists the tables that are accessed by steps of this workflow at the moment -> use to remove in case of error
        private readonly List<string> _accessedTables = [];
        // list of all tasks that are started by this workflow and its child items
        private readonly Dictionary<string, Task> _executingTasks = [];

        private bool _announced = false;  // true if this workflow announced lock
        public bool IncreasedNumExecWorkflows = false; // true if workflow increased workflow counter in processor
        private bool _addedToQueue = false; // true if this workflow was added to workflow queue in processor
        // true if some process removed zeitplanausfuehrungen mapping -> default = true
        private bool _removedMapping = true;

        // grants single access on executing task list
        private readonly SemaphoreSlim _execTaskListLock = new(1, 1);
        // grants single access on accessed tables list
        private readonly SemaphoreSlim _accessedTablesLock = new(1, 1);
        // grants single access on paketProzesslaeufe list
        private readonly SemaphoreSlim _paketProzesslaeufeListLock = new(1, 1);

        // grant single access to functions -> guarantees that function has finished before another can start
        // important when workflow is already accessed to restart when not already finished
        private readonly SemaphoreSlim _functionControl = new(1, 1);

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        // tuple that includes all prozesslaeufeIDs (prozess, paket, paketumsetzung, paketschritt)
        private Tuple<int?, int?, int?, int?> _prozesslaeufe = new(
            null, null, null, null
        );

        // use to visualize the execution of this workflow
        private Vizualiser? _vizualiser = null;

        public Workflow(
            int id,
            int zeitplanAusfuehrungenID,
            int masterPackageID,
            Processor processor,
            int? fallbackPackageID = null
        )
        {
            try
            {
                _id = id;
                _processor = processor;
                _cancleSource = new CancellationTokenSource();

                Create(zeitplanAusfuehrungenID, masterPackageID, fallbackPackageID);
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    processor,
                    e,
                    "Workflow",
                    "Failed setting workflow attributes",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        // ---------- HELPER FUNCTIONS ----------

        /// <summary>
        /// returns the set master package ID
        /// </summary>
        /// <returns>ID of master package</returns>
        public int GetMasterPackage()
        {
            return _masterPackageID;
        }

        public Processor GetProcessor()
        {
            return _processor;
        }

        /// <summary>
        /// returns the ID of fallback package of workflow
        /// </summary>
        /// <returns>ID of this workflows fallback package</returns>
        public int GetFallbackPackage()
        {
            return _fallbackPackageID;
        }

        /// <summary>
        /// returns the cancellation source
        /// </summary>
        /// <returns>cancellation source</returns>
        public CancellationTokenSource GetCancelSource()
        {
            return _cancleSource;
        }

        /// <summary>
        /// returns the given ZeitplanAusfuherungenID
        /// </summary>
        /// <returns>_zeitplanAusfuehrungenID</returns>
        public int GetZeitplanAusfuehrungenID()
        {
            return _zeitplanAusfuehrungenID;
        }

        /// <summary>
        /// adds the given task to the executing list
        /// </summary>
        /// <param name="name">name of task to add</param>
        /// <param name="task">Task to add</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of adding error</exception>
        public void AddExecutingTask(string name,Task task,Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _execTaskListLock.Wait();
                _usedSem = _execTaskListLock;

                _executingTasks[name] = task;

                _execTaskListLock.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Workflow.AddExecutingTask",
                    "Workflow: Failed adding Task to executing List!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// adds the given table to accessed list of this workflow
        /// </summary>
        /// <param name="table">name of table to add</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">any error</exception>
        public void AddAccessedTable(string table, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                table = ReplacePlaceholder(
                    _processor, this, table, prozesslaeufe, _processor.Debug);

                _accessedTablesLock.Wait();
                _usedSem = _accessedTablesLock;

                _accessedTables.Add(table);

                string output = $"ZugriffTabellen: ";
                foreach (string wf in _accessedTables)
                {
                    output += $"{wf}, ";
                }

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Workflow: Added table {table} to accessed tables in workflow! ({output})",
                        prozesslaeufe,
                        _processor.Debug
                    );
                }).Wait();

                _accessedTablesLock.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Workflow.AddAccessedTable",
                    $"Workflow: Adding table {table} to list failed!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }
        /// <summary>
        /// removes the given table from this accessed table list
        /// </summary>
        /// <param name="table">name of table to remove</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">any error</exception>
        public void RemoveAccessedTable(string table, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                table = ReplacePlaceholder(
                    _processor, this, table, prozesslaeufe, _processor.Debug);

                _accessedTablesLock.Wait();
                _usedSem = _accessedTablesLock;

                _accessedTables.Remove(table);

                string output = $"ZugriffTabellen: ";
                foreach (string wf in _accessedTables)
                {
                    output += $"{wf}, ";
                }

                _accessedTablesLock.Release();
                _usedSem = null;
                Task.Run(() => {
                    Log(
                        _processor,
                        $"Workflow: Removed table {table} from accessed tables in workflow! ({output})",
                        prozesslaeufe,
                        _processor.Debug
                    );
                }).Wait();
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Workflow.RemoveAccessedTable",
                    $"Workflow: Removing table {table} from list failed",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// gives the WorkflowID of this workflow
        /// </summary>
        /// <returns>ID of this workflow</returns>
        public int GetID()
        {
            // TODO: Throw error when _id == -1
            return _id;
        }

        /// <summary>
        /// gives the ProzesslaeufeID of this workflow
        /// </summary>
        /// <returns>ProzesslaeufeID</returns>
        public int GetProzesslaeufeID()
        {
            return _prozesslaeufeID;
        }

        /// <summary>
        /// sets flag to remvoed mapping to true
        /// </summary>
        public void SignalizeRemovedMapping()
        {
            _removedMapping = true;
        }

        /// <summary>
        /// adds the given package to the list of packages that where started by this workflow
        /// </summary>
        /// <param name="package">package to add</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">any error</exception>
        public void AddExecutingpackage(Package package, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _lockPackagesList.Wait();
                _usedSem = _lockPackagesList;

                _packages.Add(package);

                _lockPackagesList.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "AddExecutingpackage",
                    "Failed adding a package to executing list!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }
        /// <summary>
        /// removes the given package from list of executing packages
        /// </summary>
        /// <param name="package">package to remove</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void RemoveExecutingpackage(Package package, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _lockPackagesList.Wait();
                _usedSem = _lockPackagesList;

                _packages.RemoveAll(item => item == package);

                _lockPackagesList.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "_lockPackagesList",
                    "Failed removing a package from executing list!",
                    ref tmp,
                    prozesslaeufe
                );
            }

        }

        /// <summary>
        /// adds the paketProzesslaeufeID to list to track started realizations
        /// </summary>
        /// <param name="paketProzesslaeufeID">ID of paketProzesslaeufe to add to tracking list</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void AddPaketProzesslaeufeID(int paketProzesslaeufeID, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _paketProzesslaeufeListLock.Wait();
                _usedSem = _paketProzesslaeufeListLock;

                _startedPaketProzesslaeufeIDs.Add(paketProzesslaeufeID);

                _paketProzesslaeufeListLock.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "AddPaketProzesslaeufeID",
                    "Adding the PaketProzesslaeufeID to list failed!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }
        /// <summary>
        /// adds the given ID to the list of started paketschrittProzesslaeufeIDs for later usage
        /// </summary>
        /// <param name="id">paketschrittProzesslaeufeID</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void AddPaketschrittProzesslaeufeID(int id, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _startedPaketschrittProzesslaeufeIDs.Add(id);
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "AddPaketschrittProzesslaeufeID",
                    "Failed adding a new paketschrittProzesslaeufeID to started list!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// returns the calculated/given takeover times as a tuple
        /// </summary>
        /// <returns>takeover times (from and to) as tuple</returns>
        public Tuple<DateTime, DateTime> GetTakeoverTime()
        {
            return new Tuple<DateTime, DateTime>(_takeoverFrom, _takeoverTo);
        }

        /// <summary>
        /// Wraps the logging command specified to this workflow
        /// </summary>
        /// <param name="message">message to log</param>
        /// <param name="debug">log depending on processor debug flag</param>
        /// <param name="type">log depending on processor debug flag</param>
        private void WorkflowLog(
            string message,
            bool debug = false,
            LogType type = LogType.Info
        )
        {
            if (_prozesslaeufeID == -1)
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

        // ---------- WORKFLOW FUNCTIONS ----------

        /// <summary>
        /// prepares the workflow by setting zeitplanAusfuehrungenID and initializing log and setting workflow to
        /// scheduled -> workflow ready to be initialized
        /// if error occurs -> workflow gets aborted
        /// </summary>
        /// <param name="zeitplanAusfuehrungenID">new ZeitplanAusfuehrungenID</param>
        /// <param name="masterPackageID">if needed give a new masterPackageID</param>
        /// <param name="fallbackPackageID">if needed give a new fallback package ID</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void Create(int zeitplanAusfuehrungenID, int? masterPackageID = null, int? fallbackPackageID = null)
        {
            bool setScheduled = false;
            try
            {
                _functionControl.Wait();
                _usedSem = _functionControl;

                Task.Run(() => Log(
                    _processor, $"(Re)Create workflow {_id}", _prozesslaeufe)).Wait();

                _zeitplanAusfuehrungenID = zeitplanAusfuehrungenID;

                if (masterPackageID != null)
                    _masterPackageID = (int) masterPackageID;

                if (fallbackPackageID != null)
                    _fallbackPackageID = (int) fallbackPackageID;

                // add mapping for zeitplanausfuehrungen
                try
                {
                    _processor.WorkflowManager.AddMapping(_id, zeitplanAusfuehrungenID, _prozesslaeufe);
                    _removedMapping = false;
                } catch (Exception e)
                {
                    SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Create",
                        "Failed adding mapping for workflow and zeitplanausfuehrung!",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

                // set workflow to scheduled
                _processor.WorkflowManager.SetWorkflowScheduled(this, _prozesslaeufe);
                setScheduled = true;

                Task.Run(() =>
                {
                    WorkflowLog($"Workflow {_id} scheduled!");
                }).Wait();

                _functionControl.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                if (!_removedMapping)
                {
                    _processor.WorkflowManager.RemoveMapping(_id, _prozesslaeufe);
                    _removedMapping = true;
                }
                if (setScheduled)
                    _processor.WorkflowManager.SetWorkflowFailed(this, _prozesslaeufe);

                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Workflow.Create",
                    "Failed initializing workflow",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// initializes the workflow (when created or rescheduled), extracts the locking information, sets the workflow
        /// from scheduled to initializing and waits for start (based on request date). Will then be added to queue (if
        /// a lock is announced) and will wait for first spot in this queue. If this workflow shall run in locked mode,
        /// a lock is announced and will finally wait for execution (released lock). When workflow can be started the
        /// number of workflows will be increased (gets semaphore if needed) and workflow is removed from queue (if
        /// added). The workflow then gets started.
        /// In case of any error the workflow gets aborted
        /// </summary>
        /// <param name="requestDate">date when the workflow shall start</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void Init(DateTime requestDate)
        {
            try
            {
                _functionControl.Wait();
                _usedSem = _functionControl;

                // check if workflow was already created
                if (_zeitplanAusfuehrungenID == -1)
                    throw new ETLException(
                        _processor,
                        "ZeitplamnAusfuehrungenID was not set! -> Create Workflow first!",
                        "Workflow.Init",
                        _prozesslaeufe
                    );

                // init logging
                Task.Run(() =>
                {
                    _prozesslaeufeID = InitializeLogging(
                        _processor,
                        "Logging.ETL_Prozesslaeufe",
                        [
                            new Tuple<string, int>("ETL_Zeitplan_Ausfuehrungen_ID", _zeitplanAusfuehrungenID),
                            new Tuple<string, int>("ETL_Workflow_ID", _id)
                        ],
                        DateTime.Now,
                        _prozesslaeufe
                    );
                }).Wait();

                _prozesslaeufe = new Tuple<int?, int?, int?, int?>(GetProzesslaeufeID(), null, null, null);
                _vizualiser = new Vizualiser(_processor, GetProzesslaeufeID(), this);

                Task.Run(() => Log(
                    _processor, $"Initialize workflow {_id}", _prozesslaeufe, null
                )).Wait();

                // read the time periods to retrieve data for
                try
                {
                    DataTable workflowInfo = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT" +
                        $"   ETL_Pakete_ID, " +
                        $"   ETL_Fallback_Pakete_ID, " +
                        $"   Uebernahme_von, " +
                        $"   Uebernahme_bis, " +
                        $"   Uebernahme_Tage_Rueckwirkend " +
                        $"FROM pc.ETL_Workflow " +
                        $"WHERE ETL_Workflow_ID = {_id}",
                        _prozesslaeufe
                    );

                    DataRow info = workflowInfo.Rows[0];
                    _masterPackageID = Convert.ToInt32(info["ETL_Pakete_ID"].ToString());

                    if (info["ETL_Fallback_Pakete_ID"].ToString() != "")
                        _fallbackPackageID = Convert.ToInt32(info["ETL_Fallback_Pakete_ID"].ToString());

                    if (info["Uebernahme_von"].ToString() == "")  // Uebernahme_Tage_Rueckwirkend
                    {
                        _takeoverTo = DateTime.Now;
                        _takeoverTo = DateTime.Parse($"{_takeoverTo:dd.MM.yyyy} 23:59:59");

                        int preDays = Convert.ToInt32(info["Uebernahme_Tage_Rueckwirkend"].ToString());
                        _takeoverFrom = _takeoverTo.AddDays(-preDays);
                        _takeoverFrom = DateTime.Parse($"{_takeoverFrom:dd.MM.yyyy} 00:00:00");
                    }
                    else  // Uebernahme_{von,bis}
                    {
                        _takeoverFrom = DateTime.Parse(info["Uebernahme_von"].ToString() ??
                                                throw new ETLException("No Uebernahme_von"));

                        if (info["Uebernahme_bis"].ToString() == "")
                        {
                            // TODO not now, take next expected execution time
                            DataTable zeitplanAus = _processor.DbHelper.GetDataTableFromQuery(
                                _processor,
                                $"SELECT" +
                                $"   Anforderungszeitpunkt " +
                                $"FROM pc.ETL_Zeitplan_Ausfuehrungen " +
                                $"WHERE ETL_Zeitplan_Ausfuehrungen_ID = {_zeitplanAusfuehrungenID}",
                                _prozesslaeufe
                            );
                            _takeoverTo = DateTime.Parse(zeitplanAus.Rows[0]["Anforderungszeitpunkt"].ToString() ??
                                                throw new ETLException("No Anforderungszeitpunkt"));
                        }
                        else
                        {
                            _takeoverTo = DateTime.Parse(info["Uebernahme_bis"].ToString() ??
                                                throw new ETLException("No Uebernahme_bis"));
                        }
                    }
                    Task.Run(() => WorkflowLog($"Takeover data from {_takeoverFrom} to {_takeoverTo}")).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Create",
                        "Failed reading data periods!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // extract locking info for workflow
                try
                {
                    DataTable workflow = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT * FROM pc.ETL_Workflow WHERE ETL_Workflow_ID = {_id}",
                        _prozesslaeufe
                    );
                    _locked = bool.Parse(workflow.Rows[0]["Parallelsperre"].ToString() ??
                                                throw new ETLException("No Parallelsperre"));
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Create",
                        "Failed extracting the locking information from workflow!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // set workflow to executing for prozesslaeufe
                try
                {
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Prozesslaeufe",
                        _prozesslaeufeID,
                        "ETL_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Startzeitpunkt", DateTime.Now)
                        ],
                        _prozesslaeufe
                    ));
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Start",
                        $"Logging workflow start failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // set workflow from scheduled to initializing
                try
                {
                    _processor.WorkflowManager.SetWorkflowScheduledToInitializing(this, _prozesslaeufe);
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Init",
                        "Failed Setting Workflow initializing",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // wait for start
                int millisecondsToStartWorkflows = (int)(requestDate - DateTime.Now).TotalMilliseconds;
                if (millisecondsToStartWorkflows > 0)
                {
                    // Task.Delay(50).Wait();
                    Task.Run(() => WorkflowLog(
                        $"Wait for Start of Workflow {_id} ({millisecondsToStartWorkflows / 1000} seconds (+1 Second))",
                        true
                    ), _cancleSource.Token).Wait();
                    Task.Delay(millisecondsToStartWorkflows).Wait();  // wait before workflow shall be started
                    Task.Run(() => WorkflowLog($"Waiting finished", true), _cancleSource.Token).Wait();
                }

                // log start
                Task.Run(() => UpdateZeitplanAusfuehrung(
                    _processor,
                    _zeitplanAusfuehrungenID,
                    [new Tuple<string, object>("Startzeitpunkt", DateTime.Now)],
                    _prozesslaeufe
                )).Wait();

                // lock announced?
                try
                {
                    if (_processor.LockManager.IsAnnounced(Level.Workflow, _prozesslaeufe))
                    {
                        _processor.QueueManager.AddToQueue(Level.Workflow, _id, _prozesslaeufe);
                        _addedToQueue = true;
                    }
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Init",
                        "Adding workflow to queue failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // was workflow added to queue? -> wait for 1st position in workflow queue
                SemaphoreSlim? usedSem = null;
                try
                {
                    if (_addedToQueue)
                    {
                        bool waited = false;

                        _processor.WorkflowManager.WorkflowSteerLock.Wait();
                        usedSem = _processor.WorkflowManager.WorkflowSteerLock;
                        bool first = _processor.QueueManager.CheckQueueFirst(Level.Workflow, _id, _prozesslaeufe);
                        while (!first)
                        {
                            _processor.WorkflowManager.WorkflowSteerLock.Release();
                            usedSem = null;
                            waited = true;

                            int maxWait = 5;
                            DateTime jetzt = DateTime.Now;
                            bool onceSend = false;

                            if (_processor.Debug || (DateTime.Now - jetzt).TotalMinutes >= maxWait || !onceSend)
                            {
                                Task.Run(() => WorkflowLog(
                                    $"Wait for first spot in workflow queue ({first})!",
                                    true
                                ), GetCancelSource().Token).Wait();
                                jetzt = DateTime.Now;
                                onceSend = true;
                            }

                            // Task.Delay(2 * 1000).Wait();
                            Task.Delay(_processor.WaitingTime).Wait();

                            _processor.WorkflowManager.WorkflowSteerLock.Wait();
                            usedSem = _processor.WorkflowManager.WorkflowSteerLock;
                            first = _processor.QueueManager.CheckQueueFirst(Level.Workflow, _id, _prozesslaeufe);
                        }
                        _processor.WorkflowManager.WorkflowSteerLock.Release();
                        usedSem = null;

                        if (waited)
                            Task.Run(() => WorkflowLog(
                                $"Workflow {_id} first in queue", true
                            ), GetCancelSource().Token).Wait();
                    }
                } catch (Exception e)
                {
                    SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Init",
                        $"Waiting for first position in workflow queue failed!",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

                // lock process for this workflow?
                if (_locked)
                {
                    Task.Run(() => WorkflowLog($"Handle Lock for workflow {_id}"), _cancleSource.Token).Wait();

                    try
                    {
                        //    announce lock
                        _processor.LockManager.AnnounceLock(Level.Workflow, _prozesslaeufe);
                        _announced = true;
                    }
                    catch (Exception e)
                    {
                        // abort this workflow
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "Workflow.Init",
                            "Failed announcing lock for workfow!",
                            ref DummySem,
                            _prozesslaeufe
                        );
                    }
                }

                // wait until possible workflow lock has been released
                try
                {
                    bool waited = false;
                    bool announced = false;
                    _processor.WorkflowManager.WorkflowSteerLock.Wait();
                    usedSem = _processor.WorkflowManager.WorkflowSteerLock;

                    if (!_locked)
                    {
                        announced = _processor.LockManager.IsAnnounced(Level.Workflow, _prozesslaeufe);

                        if (announced)
                        {
                            Task.Run(() => WorkflowLog(
                                $"Wait until workflow {_id} can be started! (Lock announced: {announced})",
                                true
                            ), GetCancelSource().Token).Wait();

                            while (announced)
                            {
                                _processor.WorkflowManager.WorkflowSteerLock.Release();
                                usedSem = null;

                                waited = true;
                                // Task.Delay(2 * 1000).Wait();
                                Task.Delay(_processor.WaitingTime).Wait();

                                _processor.WorkflowManager.WorkflowSteerLock.Wait();
                                usedSem = _processor.WorkflowManager.WorkflowSteerLock;

                                announced = _processor.LockManager.IsAnnounced(Level.Workflow, _prozesslaeufe);
                            }

                        }
                    }

                    // increase number of executing realizations
                    try
                    {
                        Task.Run(() => _processor.IncreaseNumExecuting(
                            Level.Workflow,
                            _id,
                            _locked,
                            this,
                            _prozesslaeufe,
                            GetCancelSource()
                        ), _cancleSource.Token).Wait();
                        usedSem = null;
                    } catch (Exception e)
                    {
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "Workflow.Init",
                            "Failed increasing the number of executing workflows!",
                            ref DummySem,
                            _prozesslaeufe
                        );
                    }

                    if (waited)
                        Task.Run(() => WorkflowLog(
                            $"Workflow can be started! ({announced})"), GetCancelSource().Token).Wait();
                } catch (Exception e)
                {
                    SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Init",
                        "Failed Waiting for released Lock",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

                // remove workflow from queue (if added)
                try
                {
                    if (_addedToQueue)
                    {
                        _processor.QueueManager.RemoveFromQueue(Level.Workflow, _id, _prozesslaeufe);
                        _addedToQueue = false;
                    }
                }
                catch (Exception e)
                {
                    // abort this workflow
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Init",
                        "Failed removing workflow from queue!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // execute
                Task.Run(() =>
                {
                    WorkflowLog($"Workflow {_id} initialized!");
                }).Wait();

                _functionControl.Release();
                _usedSem = null;

                Task.Run(() => Start(_masterPackageID)).Wait();
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                HandleErrorCatch(
                    _processor,
                    e,
                    "Workflow.Init",
                    "Failed initializing Workflow",
                    ref tmp,
                    _prozesslaeufe
                );
                Task.Run(() => Abort()).Wait();
            }
        }

        /// <summary>
        /// Sets workflow from initializing to executing. Logs the start, initializes the master package and adds it to
        /// package list. The workflow then waits until the master package has finished and finishes then.
        /// In case of any error the workflow gets aborted
        /// </summary>
        /// <param name="masterpackageID">ID of the master package</param>
        /// <exception cref="ETLException">in case of any error</exception>
        private void Start(int masterpackageID)
        {
            Task? masterTask = null;
            try
            {
                _functionControl.Wait();
                _usedSem = _functionControl;

                // set workflow initializing to executing
                try
                {
                    _processor.WorkflowManager.SetWorkflowInitializingToExecuting(this, _prozesslaeufe);
                } catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Start",
                        "Failed setting workflow executing!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // log execution start
                Task.Run(() => UpdateZeitplanAusfuehrung(
                    _processor,
                    _zeitplanAusfuehrungenID,
                    [
                        new Tuple<string, object>("Ausfuehrungsstartzeitpunkt", DateTime.Now)
                    ],
                    _prozesslaeufe
                )).Wait();

                _startedExec = true;

                Task.Run(() => WorkflowLog(
                    $"Execute Workflow {_id}! (Workflowlock = " +
                    $"{_processor.LockManager.IsAnnounced(Level.Workflow, _prozesslaeufe)})"
                ), _cancleSource.Token).Wait();

                // set workflow to executing for prozesslaeufe
                try
                {
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Prozesslaeufe",
                        _prozesslaeufeID,
                        "ETL_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Ausfuehrungsstartzeitpunkt", DateTime.Now),
                            new Tuple<string, object>("Parallelsperre", _locked),
                            new Tuple<string, object>("Ist_gestartet", true)
                        ],
                        _prozesslaeufe
                    ));
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Start",
                        $"Logging workflow start failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // create master package
                Package master;
                try
                {
                    master = new Package(masterpackageID, _processor, this);
                    AddExecutingpackage(master, _prozesslaeufe);
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Start",
                        $"Creating the master package ({masterpackageID}) failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // initialize master package (includes executing)
                masterTask = Task.Run(() => master.Init(), _cancleSource.Token) ?? throw new ETLException(
                    _processor,
                    "Creating the Master package failed!",
                    "Workflow.Start",
                    _prozesslaeufe
                );

                // add package to executing list (including task) in processor
                try
                {
                    _processor.AddExecutingPackage(master, masterTask, _prozesslaeufe);
                    master.SetFlagForAddingToExecutingList();
                    AddExecutingTask($"Masterpacket_{_id}", masterTask, _prozesslaeufe);
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Start",
                        "Adding package to executing list of processor failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                _functionControl.Release();
                _usedSem = null;

                // finish this workflow
                Task.Run(() => Log(
                    _processor,
                    "Wait until master package has finished!",
                    _prozesslaeufe
                )).Wait();
                masterTask.Wait();

                Task.Run(() => Finish(), _cancleSource.Token).Wait();

            } catch (Exception e)
            {
                string status = masterTask == null ? "" : masterTask.Status.ToString();
                Task.Run(() => Log(
                    _processor,
                    $"Failed starting/running Workflow! ({status})",
                    _prozesslaeufe
                )).Wait();
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                HandleErrorCatch(
                    _processor,
                    e,
                    "Workflow.Start",
                    $"Failed starting Workflow ({e.GetType()})",
                    ref tmp,
                    _prozesslaeufe
                );

                Task.Run(() => Abort()).Wait();
            }
        }

        /// <summary>
        /// When execution has finished, the end is logged, the tracking lists of started packages and accessed tables
        /// get cleaned and the Workflow is set to finished. If the workflow announced a lock the lock is removed,
        /// the number of executing workflow gets decreased (potentially frees semaphore) and finally resets the
        /// zeitplanausfuehrungenID and ProzesslauefeID and the cancelation source is recreated. After this process, the
        /// workflow can be recreated and started again.
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        private void Finish()
        {
            try
            {
                _functionControl.Wait();
                _usedSem = _functionControl;

                // update process log to finished
                // log the end of workflow
                try
                {
                    DateTime end = DateTime.Now;
                    Task.Run(() => UpdateZeitplanAusfuehrung(
                        _processor,
                        _zeitplanAusfuehrungenID,
                        [
                            new Tuple<string, object>("Endzeitpunkt", end),
                            new Tuple<string, object>("Ausgefuehrt", true),
                            new Tuple<string, object>("Erfolgreich", true)
                        ],
                        _prozesslaeufe
                    ), _cancleSource.Token).Wait();

                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Prozesslaeufe",
                        _prozesslaeufeID,
                        "ETL_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Ausfuehrungsendzeitpunkt", end)
                        ],
                        _prozesslaeufe
                    ), _cancleSource.Token).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.End",
                        $"Updating the endtime of workflow failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // clean tracking variables
                try
                {
                    _packages.Clear();
                    _startedPaketProzesslaeufeIDs.Clear();
                    _accessedTables.Clear();
                    Task.Run(() => WorkflowLog("Cleared tracking lists!", true), _cancleSource.Token).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.End",
                        "Clearing the tracking lists failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // set workflow to finished and release lock if needed
                try
                {
                    // lock? -> remove stand alone flag
                    if (_locked)
                    {
                        _processor.LockManager.RemoveLockFlag(Level.Workflow, _prozesslaeufe);
                        _announced = false;
                    }

                    // decreaase number executing workflows -> will automatically release semaphore if needed
                    _processor.DecreaseNumExecuting(
                        Level.Workflow,
                        _locked,
                        this,
                        _prozesslaeufe
                    );
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.End",
                        "Setting the workflow to finished failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                Task.Run(() =>
                {
                    WorkflowLog($"Finished Workflow {_id}");
                }, _cancleSource.Token).Wait();

                // update process log to finished
                // log the end of workflow
                try
                {
                    DateTime end = DateTime.Now;

                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Prozesslaeufe",
                        _prozesslaeufeID,
                        "ETL_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Endzeitpunkt", end),
                            new Tuple<string, object>("Ist_abgeschlossen", true),
                            new Tuple<string, object>("Erfolgreich", true)
                        ],
                        _prozesslaeufe
                    ), _cancleSource.Token).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.End",
                        $"Updating the endtime of workflow failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // extract email info for workflow
                try
                {
                    // vizualize workflow
                    try
                    {
                        _vizualiser = new Vizualiser(
                            _processor, (int)(_prozesslaeufe.Item1 ??
                                                throw new ETLException("No ETL_Prozesslauefe_ID")), this);
                        _vizualiser.Vizualize(_prozesslaeufe);
                    }
                    catch (Exception e)
                    {
                        Exception ex = HandleErrorCatch(
                            _processor,
                            e,
                            "Vizualize",
                            $"Failed vizualizing process!",
                            ref DummySem,
                            _prozesslaeufe
                        );
                    }

                    DataTable workflow = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT * " +
                        $"FROM [conf].[Email_Verteiler_Workflow] " +
                        $"WHERE ETL_Workflow_ID = {_id} AND Ist_Aktiv = 1",
                        _prozesslaeufe
                    );

                    foreach (DataRow verteiler in workflow.Rows)
                    {
                        if (bool.Parse(verteiler["Nur_Fehler"].ToString() ?? throw new ETLException("No Nur_Fehler")))
                            continue;

                        if (bool.Parse(verteiler["CC"].ToString() ?? throw new ETLException("No CC")))
                        {
                            _ccReceiver.Add(verteiler["Empfaenger"].ToString() ?? throw new ETLException("No Empfaenger"));
                        }
                        else
                        {
                            _emailReceiver.Add(verteiler["Empfaenger"].ToString() ?? throw new ETLException("No Empfaenger"));
                        }
                    }

                    Task.Run(() => Log(
                        _processor,
                        $"{_emailReceiver.Count}",
                        _prozesslaeufe
                    )).Wait();

                    if (_emailReceiver.Count > 0)
                        Task.Run(() => SendResultMail(
                            _processor,
                            true,
                            _emailReceiver,
                            _ccReceiver,
                            this,
                            _prozesslaeufe
                        )).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Finish",
                        "Failed extracting the email sending information for workflow!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // set workflow to finished
                _processor.WorkflowManager.SetWorkflowExecutingToFinished(this, _prozesslaeufe);

                // reset zeitplanAUsfuehrungenID & prozesslaeufeID
                _zeitplanAusfuehrungenID = -1;
                _prozesslaeufeID = -1;
                _prozesslaeufe = new Tuple<int?, int?, int?, int?>(null, null, null, null);

                _emailReceiver.Clear();
                _ccReceiver.Clear();

                Task.Run(() => WorkflowLog(
                    "Reseted ZeitplanAusfuehrungenID and ProzesslaeufeID!", true), _cancleSource.Token).Wait();

                _cancleSource = new CancellationTokenSource();

                _functionControl.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                HandleErrorCatch(
                    _processor,
                    e,
                    "Workflow.End",
                    "Failed finishing Workflow",
                    ref tmp,
                    _prozesslaeufe
                );
                Task.Run(() => Abort()).Wait();
            }
        }

        /// <summary>
        /// Aborts this workflow by signalizing abortion over cancelation source and aborting all child items. After
        /// successfull abortion of all items, we log the error and wait until all tasks that where started have
        /// finished. We then remove the workflow from queue (if still in queue), remove the lock flag (if set), remove
        /// the mapping of workflow to zeitplanausfuehrungenID, decreasing the number of executing workflows
        /// (potentially frees semaphore) and sets workflow to failed.
        /// </summary>
        public void Abort()
        {
            try
            {
                _functionControl.Wait();
                _usedSem = _functionControl;

                Task.Run(() => WorkflowLog($"Aborting Workfow with ID {_id}")).Wait();
                Task.Run(() => _processor.WorkflowManager.LogWorkflowStates(_prozesslaeufe)).Wait();
                if (_prozesslaeufeID != -1)
                    Task.Run(() => _processor.PrintStatus(_prozesslaeufe)).Wait();

                // signalize cancellation for this workflow to all subtasks
                try
                {
                    _cancleSource.Cancel();
                }
                catch
                {
                    Task.Run(() => Log(
                        _processor, "Cancel Src was not created yet", _prozesslaeufe
                    )).Wait();
                }

                // set endtime of execution of process
                try
                {
                    if (_startedExec)
                        Task.Run(() => UpdateLog(
                            _processor,
                            "Logging.ETL_Prozesslaeufe",
                            _prozesslaeufeID,
                            "ETL_Prozesslaeufe_ID",
                            [
                                new Tuple<string, object>("Ausfuehrungsendzeitpunkt", DateTime.Now),
                            ],
                            _prozesslaeufe
                        )).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Abort",
                        "Logging failure of workflow in ETL_Prozesslaeufe failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // start abort for all packages
                List<Task> abortions = [];
                foreach (Package package in _packages)
                {
                    abortions.Add(Task.Run(() => package.Abort()));
                }

                // wait until all packages have been aborted
                Task.Run(() => WorkflowLog($"Wait until all packages of workflow have been aborted!")).Wait();
                Task.WaitAll([.. abortions]);
                Task.Run(() => WorkflowLog($"All packages  of workflow with ID {_id} aborted!")).Wait();

                // signalize cancellation for this workflow to all subtasks
                try
                {
                    _cancleSource.Cancel();
                }
                catch
                {
                    Task.Run(() => Log(
                        _processor, "Cancel Src was not created yet", _prozesslaeufe
                    )).Wait();
                }

                // wait until all processes for this workflow have ended
                Task.Run(() => WaitForTaskEnd()).Wait();

                if (_fallbackPackageID != -1)
                {
                    try
                    {
                        _packages.Clear();
                        _cancleSource = new CancellationTokenSource();
                        Task.Run(() => Log(
                            _processor, "Creating Fallback Package", _prozesslaeufe
                        )).Wait();
                        Package fallbackPackage = new(_fallbackPackageID, _processor, this);
                        AddExecutingpackage(fallbackPackage, _prozesslaeufe);

                        Task.Run(() => Log(
                            _processor,
                            "Initializing and starting Fallback Package",
                            _prozesslaeufe
                        )).Wait();
                        Task fallbackTask = Task.Run(fallbackPackage.Init) ?? throw new ETLException(
                            _processor,
                            "Creating the fallback package failed!",
                            "Workflow.Start",
                            _prozesslaeufe
                        );

                        _processor.AddExecutingPackage(fallbackPackage, fallbackTask, _prozesslaeufe);

                        fallbackTask.Wait();
                        Task.Run(() => Log(
                            _processor, "Finished Fallback Package", _prozesslaeufe
                        )).Wait();
                    }
                    catch (Exception ex)
                    {
                        // TODO: how to handle errors in fallback package?
                        Task.Run(() => Log(
                            _processor, "Fallback Package Failed!", _prozesslaeufe
                        )).Wait();

                        Task.Run(() => ErrorLog(
                            _processor,
                            "Dienst",
                            "Fallback Package failed!",
                            "major",
                            ex,
                            "Workflow.Abort",
                            _prozesslaeufe
                        )).Wait();

                        // start abort for all packages
                        abortions = [];
                        foreach (Package package in _packages)
                        {
                            abortions.Add(Task.Run(() => package.Abort()));
                        }

                        // wait until all packages have been aborted
                        Task.Run(() => WorkflowLog(
                            $"Wait until all fallback packages of workflow have been aborted!")).Wait();
                        Task.WaitAll([.. abortions]);
                        Task.Run(() => WorkflowLog($"All falback packages of workflow with ID {_id} aborted!")).Wait();

                        // signalize cancellation for this workflow to all subtasks
                        try
                        {
                            _cancleSource.Cancel();
                        }
                        catch
                        {
                            Task.Run(() => Log(
                                _processor, "Cancel Src was not created yet", _prozesslaeufe
                            )).Wait();
                        }
                    }
                }

                try
                {
                    _vizualiser = new Vizualiser(
                        _processor, (_prozesslaeufe.Item1 ?? throw new ETLException("No ETL_Prozesslaeufe_ID")), this);
                    _vizualiser.Vizualize(_prozesslaeufe);
                }
                catch (Exception e)
                {
                    Exception ex = HandleErrorCatch(
                        _processor,
                        e,
                        "Vizualize",
                        $"Failed vizualizing process!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // extract email info for workflow
                try
                {
                    DataTable workflow = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT * " +
                        $"FROM conf.Email_Verteiler_Workflow " +
                        $"WHERE ETL_Workflow_ID = {_id} AND Ist_Aktiv = 1",
                        _prozesslaeufe
                    );

                    foreach (DataRow verteiler in workflow.Rows)
                    {
                        if (bool.Parse(verteiler["CC"].ToString() ?? throw new ETLException("No CC")))
                        {
                            _ccReceiver.Add(verteiler["Empfaenger"].ToString() ??
                                                throw new ETLException("No Empfaenger"));
                        }
                        else
                        {
                            _emailReceiver.Add(verteiler["Empfaenger"].ToString() ??
                                                    throw new ETLException("No Empfaenger"));
                        }
                    }
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Abort",
                        "Failed extracting the email sending information for workflow!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                Task.Run(() => SendResultMail(
                    _processor,
                    false,
                    _emailReceiver,
                    _ccReceiver,
                    this,
                    _prozesslaeufe
                )).Wait();

                //throw new Exception("Fail in Abort");

                // decrease the executing workflow counter -> will automatically release semaphore if needed
                if (IncreasedNumExecWorkflows)
                    _processor.DecreaseNumExecuting(
                        Level.Workflow,
                        _locked,
                        this,
                        _prozesslaeufe
                    );

                // clean tracking variables
                _packages.Clear();
                _startedPaketProzesslaeufeIDs.Clear();
                _accessedTables.Clear();
                Task.Run(() => WorkflowLog("Cleared tracking lists!", true)).Wait();

                // remove mapping for zeitplanausfuehrungenID
                // _processor.WorkflowManager.WorkflowSteerLock.Wait();
                Task.Run(() => WorkflowLog($"Is Mapping Removed? {_removedMapping}", false)).Wait();
                if (!_removedMapping)
                {
                    _processor.WorkflowManager.RemoveMapping(_id, _prozesslaeufe);
                    _removedMapping = true;
                }

                // remove lock flag if this workflow ran in stand alone
                Task.Run(() => WorkflowLog($"Is locked? {_locked && _announced}")).Wait();
                if (_locked && _announced)
                    _processor.LockManager.RemoveLockFlag(Level.Workflow, _prozesslaeufe);

                // remove workflow from queue
                Task.Run(() => WorkflowLog($"Is Added to Queue? {_addedToQueue}")).Wait();
                if (_addedToQueue)
                    _processor.QueueManager.RemoveFromQueue(Level.Workflow, _id, _prozesslaeufe);

                Task.Run(() => _processor.PrintStatus(_prozesslaeufe)).Wait();
                Task.Run(() => _processor.WorkflowManager.LogWorkflowStates(_prozesslaeufe)).Wait();

                // set workflow to failed (from scheduled, init or executing)
                _processor.WorkflowManager.SetWorkflowFailed(this, _prozesslaeufe);

                // set workflow ended in DB (ZeitplanAusfuehrungen)
                Task.Run(() => UpdateZeitplanAusfuehrung(
                    _processor,
                    _zeitplanAusfuehrungenID,
                    [
                        new Tuple<string, object>("Endzeitpunkt", DateTime.Now),
                        new Tuple<string, object>("Ausgefuehrt", true),
                        new Tuple<string, object>("Erfolgreich", false)
                    ],
                    _prozesslaeufe
                )).Wait();

                // set prozesslaeufe to finished but not successful
                try
                {
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Prozesslaeufe",
                        _prozesslaeufeID,
                        "ETL_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Endzeitpunkt", DateTime.Now),
                            new Tuple<string, object>("Ist_abgeschlossen", true),
                            new Tuple<string, object>("Erfolgreich", false)
                        ],
                        _prozesslaeufe
                    )).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.Abort",
                        "Logging failure of workflow in ETL_Prozesslaeufe failed!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                Task.Run(() =>
                {
                    WorkflowLog($"Workfow with ID {_id} aborted!");
                }).Wait();

                // reset zeitplanAUsfuehrungenID & prozesslaeufeID
                _zeitplanAusfuehrungenID = -1;
                _prozesslaeufeID = -1;

                _emailReceiver.Clear();
                _ccReceiver.Clear();

                _prozesslaeufe = new Tuple<int?, int?, int?, int?>(null, null, null, null);

                _cancleSource = new CancellationTokenSource();

                _functionControl.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    _processor,
                    "Dienst",
                    "Aborting Workflow failed!",
                    "major",
                    e,
                    $"Workflow.Abort (ID: {_id})",
                    _prozesslaeufe
                )).Wait();

                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                HandleErrorCatch(
                    _processor,
                    e,
                    "Workflow.Abort",
                    "Aborting Workflow failed!",
                    ref tmp,
                    _prozesslaeufe
                );

                Task.Run(() => SafeExit(
                    _processor,
                    e,
                    $"Workflow.Abort (ID: {_id})",
                    _prozesslaeufe
                ));
                return;
            }
        }

        /// <summary>
        /// waits until all tasks that were started by this workflow have finished. If a TaskCancelationException is
        /// thrown, all faulted and cancled tasks are removed from list and waiting starts again. If another error
        /// occurs a safe exit of the service is initialized!
        /// </summary>
        /// <exception cref="ETLException">in case unexpected error</exception>
        private void WaitForTaskEnd()
        {
            try
            {
                _execTaskListLock.Wait();
                _usedSem = _execTaskListLock;
                if (_executingTasks.Count > 0)
                {
                    Dictionary<string, Task> copyTasks2 = new(_executingTasks);
                    foreach (var task in copyTasks2)
                    {
                        if (task.Value == null)
                        {
                            _executingTasks.Remove(task.Key);
                        } else
                        {
                            if (task.Value.Status == TaskStatus.Faulted || task.Value.Status == TaskStatus.Canceled)
                                _executingTasks.Remove(task.Key);
                        }
                    }
                }
                _execTaskListLock.Release();
                _usedSem = null;

                _execTaskListLock.Wait();
                _usedSem = _execTaskListLock;
                Dictionary<string, Task> copyTasks = new(_executingTasks);
                _execTaskListLock.Release();
                _usedSem = null;

                while (true)
                {
                    try
                    {
                        Task.Run(() => WorkflowLog(
                            $"Wait until all started tasks of workflow have been aborted! (" +
                            $"{_executingTasks.Count} tasks)"
                        )).Wait();

                        _execTaskListLock.Wait();
                        _usedSem = _execTaskListLock;
                        List<Task> tasks = [];
                        copyTasks = new Dictionary<string, Task>(_executingTasks);
                        foreach (var execTask in copyTasks)
                        {
                            tasks.Add(execTask.Value);
                        }
                        _execTaskListLock.Release();
                        _usedSem = null;

                        bool finished = Task.WaitAll([.. tasks], timeout: TimeSpan.FromSeconds(20));
                        if (finished) { break; }
                        _execTaskListLock.Wait();
                        _usedSem = _execTaskListLock;
                        copyTasks = new Dictionary<string, Task>(_executingTasks);
                        foreach (var task in copyTasks)
                        {
                            Task.Run(() => WorkflowLog(
                                $"Task {task.Key}: {task.Value.Status}", _processor.Debug)).Wait();
                            if (task.Value.Status == TaskStatus.Faulted || task.Value.Status == TaskStatus.Canceled ||
                                task.Value.Status == TaskStatus.RanToCompletion)
                                    _executingTasks.Remove(task.Key);
                        }
                        _execTaskListLock.Release();
                        _usedSem = null;

                        _cancleSource.Cancel();
                    } catch (Exception e)
                    {
                        Task.Run(() => WorkflowLog(e.ToString(), _processor.Debug)).Wait();

                        _execTaskListLock.Wait();
                        _usedSem = _execTaskListLock;
                        // log all tasks and their state
                        copyTasks = new Dictionary<string, Task>(_executingTasks);
                        foreach (var task in copyTasks)
                        {
                            Task.Run(() => WorkflowLog(
                                $"Task {task.Key}: {task.Value.Status}", _processor.Debug)).Wait();
                            if (task.Value.Status == TaskStatus.Faulted || task.Value.Status == TaskStatus.Canceled ||
                                task.Value.Status == TaskStatus.RanToCompletion)
                                    _executingTasks.Remove(task.Key);
                        }
                        _execTaskListLock.Release();
                        _usedSem = null;
                    }
                }

                Task.Run(() =>
                {
                    WorkflowLog($"All started tasks have been finished!");
                }).Wait();

                _execTaskListLock.Wait();
                _usedSem = _execTaskListLock;
                // log all tasks and their state
                copyTasks = new Dictionary<string, Task>(_executingTasks);
                foreach (var task in copyTasks)
                {
                    Task.Run(() => WorkflowLog($"Task {task.Key}: {task.Value.Status}", _processor.Debug)).Wait();
                    if (task.Value.Status == TaskStatus.Faulted || task.Value.Status == TaskStatus.Canceled ||
                        task.Value.Status == TaskStatus.RanToCompletion)
                            _executingTasks.Remove(task.Key);
                }
                _execTaskListLock.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                Task.Run(() => Log(
                    _processor, $"Failed Waiting finishing tasks! ({e})", _prozesslaeufe
                )).Wait();

                if (e is AggregateException agg)
                {
                    HandleAggregateException(_processor, agg);
                    Task.Run(() => WaitForTaskEnd()).Wait();
                } else
                {
                    SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Workflow.WaitForTaskEnd",
                        "Waiting for finished tasks failed!",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

            }
        }
    }
}
