using System.Data;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DIZService.Core
{
    public class Package : Helper
    {
        private readonly int _id;                // PackageID
        private readonly Processor _processor;   // global processor
        private readonly Workflow _workflow;     // parent workflow

        private int _paketProzesslaeufeID = -1;  // needs to by set by inititializing log

        // includes the realization objects created by this package
        private readonly List<Realization> _startedRealizations = [];

        private bool _locked;        // true if package shall run in stand alone
        //private bool _mandantenRel;  // true if package has a relation to mandant TODO: not directly used yet

        // true if this package increased number of executing in processor
        public bool IncreasedNumExecPackages = false;  // true if the counters where increased for this package
        private bool _announced = false;               // true if this pacakge announced lock
        private bool _addedToQueue = false;            // true if this package was added to queue in processor
        private bool _addedExecutingTask = false;      // true if the package (task) was added to processor list

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        private bool _startedExec = false;  // set to true if execution has started

        // tuple that includes all prozesslaeufeIDs (prozess, paket, paketumsetzung, paketschritt)
        private Tuple<int?, int?, int?, int?> _prozesslaeufe = new(
            null, null, null, null
        );

        public Package(
            int id,
            Processor processor,
            Workflow workflow
        )
        {
            try
            {
                _id = id;
                _processor = processor;
                _workflow = workflow;

                _prozesslaeufe = new Tuple<int?, int?, int?, int?>(_workflow.GetProzesslaeufeID(), null, null, null);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    processor,
                    e,
                    "Package",
                    "Failed setting package attributes",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }

        // ---------- HELPER FUNCTIONS ----------

        /// <summary>
        /// returns the package ID of this package
        /// </summary>
        /// <returns>package ID</returns>
        public int GetID()
        {
            return _id;
        }

        /// <summary>
        /// returns the PaketProzesslaeufeID
        /// </summary>
        /// <returns>_paketProzesslaeufeID</returns>
        public int GetPaketProzesslaeufeID()
        {
            return _paketProzesslaeufeID;
        }

        /// <summary>
        /// Wraps the logging command specified to this package
        /// </summary>
        /// <param name="message">message to log</param>
        /// <param name="debug">log depending on processor debug flag</param>
        /// <param name="type">EventType to log (Default=Information)</param>
        private void PackageLog(
            string message,
            bool debug = false,
            LogType type = LogType.Info
        )
        {
            if (_paketProzesslaeufeID == -1)
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

        /// <summary>
        /// removes the given realization from started list
        /// </summary>
        /// <param name="realization">realization to remove</param>
        /// <exception cref="ETLException">when realization cannot be removed</exception>
        public void RemoveRealization(Realization realization)
        {
            try
            {
                // TODO: is a semaphore needed?
                _startedRealizations.RemoveAll(item => item == realization);
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "RemoveRealization",
                    "Removing realization from list failed!",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// sets the flag for adding package to executing list to true
        /// </summary>
        public void SetFlagForAddingToExecutingList()
        {
            _addedExecutingTask = true;
        }

        // ---------- WORKFLOW FUNCTIONS ----------

        /// <summary>
        /// initializes logging, add prozessID to workflow, reads locking state, adds package to queue if a lock is
        /// announced and waits for first spot in this queue and announces lock if this package shall run in stand-alone
        /// mode. Will then wait for start (released lock) and increase number of executing packages. Finally removes
        /// package from queue and starts depending packages. After executing the this package gets started.
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        public void Init()
        {
            try
            {
                // init logging
                Task.Run(() =>
                {
                    _paketProzesslaeufeID = InitializeLogging(
                        _processor,
                        "Logging.ETL_Paket_Prozesslaeufe",
                        [
                                new Tuple<string, int>("ETL_Prozesslaeufe_ID", _workflow.GetProzesslaeufeID()),
                                new Tuple<string, int>(
                                    "ETL_Pakete_ID",
                                    _id
                                )
                        ],
                        DateTime.Now,
                        _prozesslaeufe
                    );

                }, _workflow.GetCancelSource().Token).Wait();

                _workflow.AddPaketProzesslaeufeID(_paketProzesslaeufeID, _prozesslaeufe);
                _prozesslaeufe = new Tuple<int?, int?, int?, int?>(
                    _prozesslaeufe.Item1,
                    _paketProzesslaeufeID,
                    null,
                    null
                );

                Task.Run(() => PackageLog($"Initialize package {_id}"), _workflow.GetCancelSource().Token).Wait();

                DataTable package = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * FROM pc.ETL_Pakete WHERE ETL_Pakete_ID = {_id} AND Ist_Aktiv = 1",
                    _prozesslaeufe
                );

                // extract information
                try
                {
                    _locked = bool.Parse(package.Rows[0]["Parallelsperre"]?.ToString() ?? "true");
                    //_mandantenRel = bool.Parse(package.Rows[0]["Mandantenbezug"]?.ToString() ?? "true");
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Package.Init",
                        "Failed extracting the locking and mandanten information from package!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // handle dependencies
                Task.Run(() => HandleDependencies(), _workflow.GetCancelSource().Token).Wait();

                // lock announced?
                try
                {
                    // add to package queue if a lock was announced
                    if (_processor.LockManager.IsAnnounced(Level.Package,_prozesslaeufe))
                    {
                        _processor.QueueManager.AddToQueue(Level.Package, _id, _prozesslaeufe);
                        _addedToQueue = true;
                    }
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Package.Init",
                        "Failed extracting the locking information from package!",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // was package added to queue? -> wait for 1st position in package queue
                try
                {
                    if (_addedToQueue)
                    {
                        bool waited = false;

                        _processor.WorkflowManager.PackageSteerLock.Wait();
                        _usedSem = _processor.WorkflowManager.PackageSteerLock;

                        bool first = _processor.QueueManager.CheckQueueFirst(Level.Package, _id, _prozesslaeufe);
                        while (!first)
                        {
                            _processor.WorkflowManager.PackageSteerLock.Release();
                            _usedSem = null;

                            waited = true;
                            // Task.Delay(2 * 1000).Wait();
                            Task.Delay(_processor.WaitingTime).Wait();

                            _processor.WorkflowManager.PackageSteerLock.Wait();
                            _usedSem = _processor.WorkflowManager.PackageSteerLock;

                            first = _processor.QueueManager.CheckQueueFirst(Level.Package, _id, _prozesslaeufe);
                        }
                        _processor.WorkflowManager.PackageSteerLock.Release();
                        _usedSem = null;

                        if (waited)
                            Task.Run(() => PackageLog(
                                $"Package {_id} first in queue",
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
                        "Package.Init",
                        "Failed waiting for first position of package queue!",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

                // lock process for this package?
                if (_locked)
                {
                    try
                    {
                        Task.Run(() => PackageLog(
                            $"Handle Lock for package {_id}",
                            true
                        ), _workflow.GetCancelSource().Token).Wait();

                        // set flag to get stand-alone mode
                        _processor.LockManager.AnnounceLock(Level.Package, _prozesslaeufe);
                        _announced = true;
                    }
                    catch (Exception e)
                    {
                        SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "Package.Init",
                            "Failed announcing lock for package",
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
                    _processor.WorkflowManager.PackageSteerLock.Wait();
                    _usedSem = _processor.WorkflowManager.PackageSteerLock;
                    if (!_locked)
                    {
                        announced = _processor.LockManager.IsAnnounced(Level.Package, _prozesslaeufe);

                        if (announced)
                        {
                            Task.Run(() => PackageLog(
                                $"Wait until package {_id} can be started! ({announced})",
                                true
                            ), _workflow.GetCancelSource().Token).Wait();
                            while (announced)
                            {
                                _processor.WorkflowManager.PackageSteerLock.Release();
                                _usedSem = null;

                                waited = true;
                                // Task.Delay(2 * 1000).Wait();
                                Task.Delay(_processor.WaitingTime).Wait();

                                _processor.WorkflowManager.PackageSteerLock.Wait();
                                _usedSem = _processor.WorkflowManager.PackageSteerLock;

                                announced = _processor.LockManager.IsAnnounced(Level.Package, _prozesslaeufe);
                            }
                        }
                    }

                    // increase number of executing packages
                    try
                    {
                        Task.Run(() => _processor.IncreaseNumExecuting(
                            Level.Package,
                            _id,
                            _locked,
                            this,
                            _prozesslaeufe,
                            _workflow.GetCancelSource()
                        ), _workflow.GetCancelSource().Token).Wait();
                        _usedSem = null;
                    } catch (Exception e)
                    {
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "Package.Init",
                            "Failed increasing number of executing packages",
                            ref DummySem,
                            _prozesslaeufe
                        );
                    }

                    if (waited)
                        Task.Run(() => PackageLog(
                            $"Package can be started! ({announced})"
                        ),_workflow.GetCancelSource().Token).Wait();
                }
                catch (Exception e)
                {
                    SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Package.Init",
                        "Failed Waiting for released Lock",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

                // remove package from queue (if added)
                try
                {
                    if (_addedToQueue)
                    {
                        _processor.QueueManager.RemoveFromQueue(Level.Package, _id, _prozesslaeufe);
                        _addedToQueue = false;
                    }
                }
                catch (Exception e)
                {
                    SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Package.Init",
                        "Failed removing Package from queue and releasing Steering lock",
                        ref tmp,
                        _prozesslaeufe
                    );
                }

                Task.Run(() => {
                    PackageLog($"Package {_id} initialized");
                }, _workflow.GetCancelSource().Token).Wait();

                Task.Run(() => Start(), _workflow.GetCancelSource().Token).Wait();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Package.Init",
                    $"Failed initializing the Package with ID {_id}",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// updates the log (start), reads needed realizations and initializes them. After that the initialized
        /// realizations are started (controlled in correct order) and after finished execution the package gets
        /// finished
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        private void Start()
        {
            try
            {
                bool announced = _processor.LockManager.IsAnnounced(Level.Package, _prozesslaeufe);
                Task.Run(() => PackageLog(
                    $"Execute Package {_id}! (Packagelock = " +
                    $"{announced})"
                ), _workflow.GetCancelSource().Token).Wait();

                // log start of package (paket_prozesslauf)
                try
                {
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paket_Prozesslaeufe",
                        _paketProzesslaeufeID,
                        "ETL_Paket_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Startzeitpunkt", DateTime.Now),
                            new Tuple<string, object>("Parallelsperre", _locked),
                            new Tuple<string, object>("Ist_gestartet", true)
                        ],
                        _prozesslaeufe
                    ), _workflow.GetCancelSource().Token).Wait();
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Package.Start",
                        $"Failed logging start of package {_id}",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }


                // read the realizations correlated to package
                DataTable realizations = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT * " +
                    $"FROM pc.ETL_PAKETE_PAKETUMSETZUNGEN " +
                    $"WHERE ETL_Pakete_ID = {_id} AND ETL_Workflow_ID = {_workflow.GetID()} AND Ist_aktiv = 1",
                    _prozesslaeufe
                );

                // read realizations of package if realizations are applied
                if (realizations.Rows.Count > 0)
                {
                    List<int> umsetzungenIDs = [];
                    foreach (DataRow realization in realizations.Rows)
                    {
                        umsetzungenIDs.Add(Convert.ToInt32(realization["ETL_PAKET_UMSETZUNGEN_ID"].ToString()));
                    }

                    string umsetzungenIDsList = ConvertListToString(
                        _processor,
                        umsetzungenIDs,
                        _prozesslaeufe
                    );

                    // read realizations
                    DataTable packageRealizations = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT paket.*, ab.Paket_Priorisierung, ab.Mandanten_ID " +
                        $"FROM pc.ETL_PAKET_UMSETZUNGEN AS paket " +
                        $"JOIN pc.ETL_Pakete_Paketumsetzungen AS ab " +
                        $"     ON paket.ETL_Paket_Umsetzungen_ID = ab.ETL_Paket_Umsetzungen_ID AND " +
                        $"        ab.ETL_Pakete_ID = {_id} AND ab.ETL_Workflow_ID = {_workflow.GetID()} " +
                        $"WHERE paket.ETL_Paket_Umsetzungen_ID IN ({umsetzungenIDsList}) AND " +
                        $"      ab.Ist_Aktiv = 1 AND paket.Ist_aktiv = 1 " +
                        $"ORDER BY ab.Paket_Priorisierung",
                        _prozesslaeufe
                    );

                    // create realizations
                    foreach (DataRow realizationRow in packageRealizations.Rows)
                    {
                        // extract needed information
                        int packagePriority, realizationID;
                        int parallelSteps;
                        int mandantenID;
                        bool realizationLocked;
                        try
                        {
                            packagePriority = int.Parse(
                                realizationRow["Paket_Priorisierung"].ToString() ??
                                    throw new ETLException("No Paket_Priorisierung"));
                            realizationID = int.Parse(
                                realizationRow["ETL_Paket_Umsetzungen_ID"].ToString() ??
                                    throw new ETLException("No ETL_Paket_Umsetzungen_ID"));
                            parallelSteps = int.Parse(
                                realizationRow["Anzahl_Parallele_Schritte"].ToString() ??
                                    throw new ETLException("No Anzahl_Parallele_Schritte"));
                            realizationLocked = bool.Parse(
                                realizationRow["Parallelsperre"].ToString() ??
                                    throw new ETLException("No Parallelsperre"));
                            mandantenID = int.Parse(
                                realizationRow["Mandanten_ID"].ToString() ??
                                    throw new ETLException("No Mandanten_ID"));
                        }
                        catch (Exception e)
                        {
                            throw HandleErrorCatch(
                                _processor,
                                e,
                                "Package.Start",
                                "Failed extracting the realization information!",
                                ref DummySem,
                                _prozesslaeufe
                            );
                        }

                        // initialize new realization
                        try
                        {
                            _startedRealizations.Add(
                                new Realization(
                                    realizationID,
                                    _processor,
                                    _workflow,
                                    this,
                                    parallelSteps,
                                    packagePriority,
                                    realizationLocked,
                                    mandantenID
                                )
                            );
                        }
                        catch (Exception e)
                        {
                            throw HandleErrorCatch(
                                _processor,
                                e,
                                "Package.Start",
                                "Failed adding a new realization to tracking list!",
                                ref DummySem,
                                _prozesslaeufe
                            );
                        }
                    }
                }

                // log start of package (paket_prozesslauf)
                try
                {
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paket_Prozesslaeufe",
                        _paketProzesslaeufeID,
                        "ETL_Paket_Prozesslaeufe_ID",
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
                        "Package.Start",
                        $"Failed logging execution start of package {_id}",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                _startedExec = true;

                // run realizations
                Task.Run(() => HandleRealizations(), _workflow.GetCancelSource().Token).Wait();

                try
                {
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paket_Prozesslaeufe",
                        _paketProzesslaeufeID,
                        "ETL_Paket_Prozesslaeufe_ID",
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
                        "Package.Start",
                        $"Failed logging execution start of package {_id}",
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
                    "Package.Start",
                    $"Failed starting the package with ID {_id}",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// remmoves the lock flag (if set before), decreases the number of executing packages, removes the package
        /// from tracking lists in processor and workflow and logs the end
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        private void Finish()
        {
            try
            {
                _processor.WorkflowManager.PackageSteerLock.Wait();
                _usedSem = _processor.WorkflowManager.PackageSteerLock;

                // set lock flag to false if lock for this pacakge was active
                try
                {
                    if (_locked)
                    {
                        // remove flag to enable parallel realization execution
                        _processor.LockManager.RemoveLockFlag(Level.Package, _prozesslaeufe);
                        _announced = false;
                    }
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Package.Finish",
                        "Failed removing the lock flag on package level",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }

                // decrease number of executing packages
                _processor.DecreaseNumExecuting(Level.Package, _locked, this, _prozesslaeufe);

                // remove package from executing list (globally and in workflow)
                _processor.RemoveExecutingPackage(this, _prozesslaeufe);
                _workflow.RemoveExecutingpackage(this, _prozesslaeufe);
                _addedExecutingTask = false;

                // log end of package
                try
                {
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paket_Prozesslaeufe",
                        _paketProzesslaeufeID,
                        "ETL_Paket_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Endzeitpunkt", DateTime.Now),
                            new Tuple<string, object>("Ist_abgeschlossen", true),
                            new Tuple<string, object>("Erfolgreich", true)
                        ],
                        _prozesslaeufe
                    ), _workflow.GetCancelSource().Token);

                    Task.Run(() => {
                        PackageLog($"Finished Package {_id}");
                    }, _workflow.GetCancelSource().Token).Wait();
                    _processor.WorkflowManager.PackageSteerLock.Release();
                    _usedSem = null;
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "Package.Finish",
                        "Failed logging end of package",
                        ref DummySem,
                        _prozesslaeufe
                    );
                }
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "Package.Finish",
                    $"Ending the package with ID {_id} failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// aborts all started realizations. After abortion, removes lock flag (if set before), removes package from
        /// queue (if added before), decreases the number of executing packages (if increased before) and removes
        /// package from tracking list in workflow and processor. Finally the End of execution is logged.
        /// </summary>
        public void Abort()
        {
            Task.Run(() => PackageLog($"Aborting Package with ID {_id}")).Wait();

            try
            {
                // log process end
                if (_paketProzesslaeufeID != -1 && _startedExec)
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paket_Prozesslaeufe",
                        _paketProzesslaeufeID,
                        "ETL_Paket_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Ausfuehrungsendzeitpunkt", DateTime.Now)
                        ],
                        _prozesslaeufe
                    )).Wait();

                // initialize abort for all running realizations
                List<Task> abortions = [];
                foreach (Realization realization in _startedRealizations)
                {
                    Task realizationAbortTask = Task.Run(() => realization.Abort());
                    abortions.Add(realizationAbortTask);
                }

                // wait for all finished abortions
                Task.WaitAll([.. abortions]);
                Task.Run(() => PackageLog($"All Realization of Package with ID {_id} aborted!")).Wait();

                // handle lock
                if (_locked && _announced)
                {
                    _processor.LockManager.RemoveLockFlag(Level.Package, _prozesslaeufe);
                    _announced = false;
                }

                // remove from queue if needed
                if (_addedToQueue)
                {
                    _processor.QueueManager.RemoveFromQueue(Level.Package, _id, _prozesslaeufe);
                    _addedToQueue = false;
                }

                // decrease executing package num -> will automatically release semaphore if needed
                if (IncreasedNumExecPackages)
                    _processor.DecreaseNumExecuting(
                        Level.Package,
                        _locked,
                        this,
                        _prozesslaeufe
                    );

                // remove package from globally executing packages list
                if (_addedExecutingTask)
                {
                    _processor.RemoveExecutingPackage(this, _prozesslaeufe);
                    _addedExecutingTask = false;
                }

                // log error
                if (_paketProzesslaeufeID != -1)
                    Task.Run(() => UpdateLog(
                        _processor,
                        "Logging.ETL_Paket_Prozesslaeufe",
                        _paketProzesslaeufeID,
                        "ETL_Paket_Prozesslaeufe_ID",
                        [
                            new Tuple<string, object>("Endzeitpunkt", DateTime.Now),
                            new Tuple<string, object>("Ist_abgeschlossen", true)
                        ],
                        _prozesslaeufe
                    )).Wait();
            }
            catch (Exception e)
            {
                Task.Run(() => SafeExit(
                    _processor,
                    e,
                    $"Package.Abort (ID: {_id})",
                    _prozesslaeufe
                ));
                return;
            }

            Task.Run(() => {
                PackageLog($"Package with ID {_id} aborted");
            }).Wait();
        }

        /// <summary>
        /// reads the dependencies table and starts all packages that this package depends on. This includes checking
        /// if another workflow already executes the package, which can be used directly.
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        private void HandleDependencies()
        {
            Task.Run(() => PackageLog("Start reading dependencies", true), _workflow.GetCancelSource().Token).Wait();

            // search for dependencies
            DataTable childPackages;
            try
            {
                childPackages = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    $"SELECT " +
                    $"    A.ETL_Paket_Abhaengigkeiten_ID, " +
                    $"    A.ETL_Pakete_ID, " +
                    $"    A.Vorlauf_ETL_Pakete_ID, " +
                    $"    A.Ist_aktiv " +
                    $"FROM pc.ETL_PAKET_ABHAENGIGKEITEN AS A " +
                    $"JOIN pc.ETL_Pakete AS B " +
                    $"    ON A.Vorlauf_ETL_Pakete_ID = B.ETL_Pakete_ID " +
                    $"WHERE A.ETL_Pakete_ID = {_id} AND A.ETL_Workflow_ID = {_workflow.GetID()} AND " +
                    $"      A.Ist_Aktiv = 1 AND B.Ist_aktiv = 1",
                    _prozesslaeufe
                );
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "HandleDependencies",
                    "Retrieving dependencies failed!",
                    ref DummySem,
                    _prozesslaeufe
                );
            }

            // check if this package is dependend from other packages and execute them before
            List<Task> tasks = [];
            try
            {
                if (childPackages.Rows.Count > 0)
                {
                    // start all dependent packages and wait for successfull execution before starting this package
                    // Task.Delay(100).Wait();
                    Task.Run(() => PackageLog(
                        $"Start all depending packages of package {_id}"
                    ), _workflow.GetCancelSource().Token).Wait();

                    foreach (DataRow child in childPackages.Rows)
                    {
                        // check if there is already a task executing the depending package
                        _processor.ExecPackageTaskLock.Wait();
                        _usedSem = _processor.ExecPackageTaskLock;

                        // check if package is initializing -> wait until initialized
                        if (_processor.CheckPackageInInitializingDepPackagesList(
                            int.Parse(child["VORLAUF_ETL_PAKETE_ID"].ToString() ??
                                        throw new ETLException("No Vorlauf_ETL_Pakete_ID"))))
                        {
                            // wait until initialized
                            while (!_processor.ExecutingPackagesTasks.Any(
                            m => m.Item1.GetID() == int.Parse(child["VORLAUF_ETL_PAKETE_ID"].ToString() ??
                                                                throw new ETLException("No Vorlauf_ETL_Pakete_ID"))))
                            {
                                _processor.ExecPackageTaskLock.Release();
                                _usedSem = null;

                                Task.Delay(2 * 1000).Wait();

                                _processor.ExecPackageTaskLock.Wait();
                                _usedSem = _processor.ExecPackageTaskLock;
                            }

                            // add the existing task to captured list
                            try
                            {
                                Tuple<Package, Task> existingTask = _processor.ExecutingPackagesTasks.Find(
                                    task => task.Item1.GetID() == int.Parse(
                                        child["VORLAUF_ETL_PAKETE_ID"].ToString() ??
                                            throw new ETLException("No Vorlauf_ETL_Pakete_ID"))) ??
                                    throw new ETLException("Could not find existing task");

                                _processor.ExecPackageTaskLock.Release();
                                _usedSem = null;

                                tasks.Add(existingTask.Item2);
                            }
                            catch (Exception e)
                            {
                                throw HandleErrorCatch(
                                    _processor,
                                    e,
                                    "HandleDependencies",
                                    "Adding existing task to package list failed!",
                                    ref DummySem,
                                    _prozesslaeufe
                                );
                            }
                        } else
                        {
                            if (_processor.ExecutingPackagesTasks.Any(
                                m => m.Item1.GetID() == int.Parse(child["VORLAUF_ETL_PAKETE_ID"].ToString() ??
                                    throw new ETLException("No Vorlauf_ETL_Pakete_ID"))))
                            {
                                // add the existing task to captured list
                                try
                                {
                                    Tuple<Package, Task> existingTask = _processor.ExecutingPackagesTasks.Find(
                                        task => task.Item1.GetID() == int.Parse(
                                            child["VORLAUF_ETL_PAKETE_ID"].ToString() ??
                                                throw new ETLException("No Vorlauf_ETL_Pakete_ID"))) ??
                                        throw new ETLException("Could not find existing task");

                                    _processor.ExecPackageTaskLock.Release();
                                    _usedSem = null;

                                    tasks.Add(existingTask.Item2);
                                }
                                catch (Exception e)
                                {
                                    throw HandleErrorCatch(
                                        _processor,
                                        e,
                                        "HandleDependencies",
                                        "Adding existing task to package list failed!",
                                        ref DummySem,
                                        _prozesslaeufe
                                    );
                                }
                            }
                            else
                            {
                                // add package to initializing list and release lock
                                _processor.AddInitializingDepPackage(
                                    int.Parse(child["VORLAUF_ETL_PAKETE_ID"].ToString() ??
                                                throw new ETLException("No Vorlauf_ETL_Pakete_ID")));

                                _processor.ExecPackageTaskLock.Release();
                                _usedSem = null;

                                // create new package
                                Package depPack;
                                try
                                {
                                    depPack = new Package(
                                        int.Parse(child["VORLAUF_ETL_PAKETE_ID"].ToString() ??
                                                    throw new ETLException("No Vorlauf_ETL_Pakete_ID")),
                                        _processor,
                                        _workflow
                                    );
                                    _workflow.AddExecutingpackage(depPack, _prozesslaeufe);
                                } catch (Exception e)
                                {
                                    throw HandleErrorCatch(
                                        _processor,
                                        e,
                                        "HandleDependencies",
                                        "Creating depending package failed!",
                                        ref DummySem,
                                        _prozesslaeufe
                                    );
                                }

                                // start package execution
                                Task depTask = Task.Run(
                                    () => depPack.Init(), _workflow.GetCancelSource().Token
                                ) ?? throw new ETLException(
                                    _processor,
                                    "Child Package Task was Null Element!",
                                    "HandleDependencies",
                                    _prozesslaeufe
                                );

                                // add package to executing list
                                try
                                {
                                    tasks.Add(depTask);
                                    _workflow.AddExecutingTask($"Package_{_id}", depTask, _prozesslaeufe);
                                    _processor.AddExecutingPackage(depPack, depTask, _prozesslaeufe);
                                    depPack.SetFlagForAddingToExecutingList();

                                    // remove from initializing
                                    _processor.RemoveInitializingDepPackage(
                                    int.Parse(child["VORLAUF_ETL_PAKETE_ID"].ToString() ??
                                                throw new ETLException("No Vorlauf_ETL_Pakete_ID")));
                                } catch (Exception e)
                                {
                                    throw HandleErrorCatch(
                                        _processor,
                                        e,
                                        "HandleDependencies",
                                        "Failed adding the depending package task to list!",
                                        ref DummySem,
                                        _prozesslaeufe
                                    );
                                }
                            }
                        }

                    }

                    // wait until all depending packages finished
                    Task.WaitAll([.. tasks], _workflow.GetCancelSource().Token);
                    Task.Run(() => {
                        PackageLog($"Dependencies for package {_id} finished!");
                    }, _workflow.GetCancelSource().Token).Wait();
                }
                else
                {
                    Task.Run(() => PackageLog(
                        $"No dependencies for package {_id}"
                    ), _workflow.GetCancelSource().Token).Wait();
                }
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "HandleDependencies",
                    "Checking depending packages failed!",
                    ref tmp,
                    _prozesslaeufe
                );
            }
        }

        /// <summary>
        /// handles the initialized realizations. This includes checking of priorizations and execution in right order
        /// based on priorization
        /// </summary>
        /// <exception cref="ETLException">in case of any error</exception>
        private void HandleRealizations()
        {
            try
            {
                int lastPriority = -1000;
                List<Task> realizationTasks = [];  // lists the tasks of realizations started

                // copy list to iterate over non changeable list
                List<Realization> startedRealizations = [.. _startedRealizations];
                foreach (Realization realization in startedRealizations)
                {
                    // check if this is the first realization and prio is initial value of -1000
                    if (lastPriority == -1000)
                        lastPriority = realization.GetPriorization();

                    // check if last priorization is the same -> start package, otherwise wait until all started
                    // realizations have finished
                    if (realization.GetPriorization() != lastPriority)
                    {
                        Task.WaitAll([.. realizationTasks], _workflow.GetCancelSource().Token);
                        realizationTasks.Clear();
                    }

                    Task realizationTask = Task.Run(realization.Init, _workflow.GetCancelSource().Token);
                    _workflow.AddExecutingTask($"Realization_{realization.GetID()}", realizationTask, _prozesslaeufe);
                    realizationTasks.Add(realizationTask);

                    lastPriority = realization.GetPriorization();
                }

                // wait until all left realization tasks have finished
                Task.WaitAll([.. realizationTasks], _workflow.GetCancelSource().Token);
                // Task.Delay(500).Wait();
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "HandleRealizations",
                    $"Failed starting the realization in prioritized way! ({e.GetType()})",
                    ref DummySem,
                    _prozesslaeufe
                );
            }
        }
    }
}
