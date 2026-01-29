using System.Data;
using System.Timers;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DIZService.Core
{
    /// <summary>
    /// symbolizes the level of the ETL service
    /// </summary>
    public enum Level
    {
        Workflow,
        Package,
        Realization,
        Step
    }

    /// <summary>
    /// organizes the execution of the scheduler and starts workflows is needed
    /// </summary>
    public class Processor : Helper
    {
        private const int Intervall = 10;  // check interval in seconds
        public int WaitingTime = 100;      // Time in milliseconds to use when process has to wait

        public bool Debug = false;  // set to true to get debug output

        public DBHelper DbHelper;  // use to get helper functions to work with DBs

        // ------ HELPER CLASSES TO HANDLE QUEUES, LOCKING AND WORKFLOWS -------
        public ParallelLockManager LockManager;
        public QueueManager QueueManager;
        public WorkflowManager WorkflowManager;

        // -------------- COUNTER FOR RUNNING PROCESSES ON LEVELS --------------
        private int _numExecutingWorkflows = 0;
        private int _numExecutingPackages = 0;
        private int _numExecutingRealizations = 0;
        private int _numExecutingSteps = 0;

        private int _numExecutingNormalWorkflows = 0;
        private int _numExecutingNormalPackages = 0;
        private int _numExecutingNormalRealizations = 0;
        private int _numExecutingNormalSteps = 0;

        // semaphore to lock the access on thread counter
        private readonly SemaphoreSlim _numThreadLock = new(1, 1);

        // semaphores to lock access on counter for executing processes on given levels
        private readonly SemaphoreSlim _numExecWFLock = new(1, 1);  // workflow
        private readonly SemaphoreSlim _numExecPLock = new(1, 1);   // package
        private readonly SemaphoreSlim _numExecRLock = new(1, 1);   // realization
        private readonly SemaphoreSlim _numExecSLock = new(1, 1);   // step

        // semaphore to lock access on accessed tables list
        private readonly SemaphoreSlim _accessedTablesLock = new(1, 1);
        // semaphore to lock access on executing packages list
        public SemaphoreSlim ExecPackageTaskLock = new(1, 1);

        // lists all tables that are accessed by any process
        private readonly List<string> _accessedTables = [];
        // list that maps the packageID to corresponding task for later waiting of successfull execution
        public List<Tuple<Package, Task>> ExecutingPackagesTasks { get; set; } = [];

        public int MaxThreads = 10;   // limits the maximum number of threads
        private int _numThreads = 0;  // the number of running threads

        public int MaxWaitWithoutMessage = 5;                      // minutes to wait without logging output
        public DateTime LastSchedulerMessage = DateTime.MinValue;  // tracks when the last output was printed

        // captures the IDs of workflows that could not be created again
        private readonly Dictionary<int, int> _failedCreatingWorkflows = [];

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        public string Stage;
        public string Servicename;
        private readonly List<int> _initializingDepPackages = [];
        private static SemaphoreSlim s_dummySem = new(1, 1);

        public Processor(string stage, string serviceName, bool main = false)
        {
            Stage = stage;
            Servicename = serviceName;

            DbHelper = new DBHelper(this);

            if (main)
                UpdateParameters(true);

            // initialize the processor manager
            LockManager = new ParallelLockManager(this, Debug);
            QueueManager = new QueueManager(this, Debug);
            WorkflowManager = new WorkflowManager(this, Debug);
        }

        /// <summary>
        /// shows all necessary tracking variables in log
        /// </summary>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        public void PrintStatus(Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            string accessedTables = "";
            foreach (string table in _accessedTables)
            {
                accessedTables += $"{table}, ";
            }

            string status = $"Number of Threads: {_numThreads} | " +
                            $"Number of executing: {_numExecutingWorkflows} (Workflows); {_numExecutingPackages} " +
                            $"(Packages); {_numExecutingRealizations} (Realizations); {_numExecutingSteps} (Steps) |" +
                            $" Accessed Tables: {"{"}{accessedTables}{"}"} | " +
                            $"Number Executing Package Tasks: {ExecutingPackagesTasks.Count} | Queue Numbers: " +
                            $"{QueueManager.WaitingWorkflows.Count}; {QueueManager.WaitingPackages.Count}; " +
                            $"{QueueManager.WaitingRealizations.Count}; {QueueManager.WaitingSteps.Count}";

            Task.Run(() => Log(this, status, prozesslaeufe, null)).Wait();
        }

        /// <summary>
        /// adds the given package to initializing depending package list if not already exists. If so an
        /// exception is thrown
        /// </summary>
        /// <param name="packageID">package to add</param>
        /// <exception cref="ETLException">if package already in list</exception>
        public void AddInitializingDepPackage(int packageID)
        {
            if (_initializingDepPackages.Contains(packageID))
                throw new ETLException(
                    $"Cannot add package {packageID} to Initializing Listz! There already exists such an package " +
                    $"initializing!"
                );

            _initializingDepPackages.Add(packageID);
        }

        /// <summary>
        /// removes the given package from initializing depending packages list. throws an exception if given
        /// package not in list
        /// </summary>
        /// <param name="packageID">package to remove</param>
        /// <exception cref="ETLException">if given package not in list</exception>
        public void RemoveInitializingDepPackage(int packageID)
        {
            if (!_initializingDepPackages.Contains(packageID))
                throw new ETLException($"There is no package {packageID} in list to remove!");

            _initializingDepPackages.Remove(packageID);
        }

        /// <summary>
        /// checks if given packages exists in initializiong depending packages list
        /// </summary>
        /// <param name="packageID">packageID to check for</param>
        /// <returns>true if package in list, flase otherwise</returns>
        public bool CheckPackageInInitializingDepPackagesList(int packageID)
        {
            return _initializingDepPackages.Contains(packageID);
        }

        /// <summary>
        /// increases or adds a counter for a given workflowID, when Workflow could not be created. If workflow failed
        /// 10 times sets the workflow inactive
        /// </summary>
        /// <param name="workflowID">workflow to increase counter for</param>
        public void AddFailedCreatingWorkflow(int workflowID)
        {
            if (_failedCreatingWorkflows.TryGetValue(workflowID, out int value))
            {
                Task.Run(() => Log(
                    this,
                    $"(A) Increase Failure Counter for Workflow {workflowID} to " +
                    $"{value + 1}",
                    _dummyTuple
                )).Wait();
                _failedCreatingWorkflows[workflowID] += 1;
            } else
            {
                Task.Run(() => Log(
                    this,
                    $"(B) Increase Failure Counter for Workflow {workflowID} to 1",
                    _dummyTuple
                )).Wait();
                _failedCreatingWorkflows[workflowID] = 1;
            }

            if (_failedCreatingWorkflows[workflowID] == 10)
            {
                // send error mail as notification that something went wrong
                Task.Run(() => Log(
                    this,
                    $"Sending Email for failed Creation of Workflow {workflowID} after 10 tries!",
                    _dummyTuple
                )).Wait();
                SendInfoMail(
                    this,
                    "Fehler beim Erstellen eines Workflows",
                    $"Der Dienst war nicht in der Lage den Workflow mit der ID {workflowID} neu zu erstellen! Der " +
                    "Workflow wurde nach 10 Versuchen deaktiviert!",
                    workflowID,
                    _dummyTuple
                );
                // deactivate workflow
                DbHelper.ExecuteCommandDIZ(
                    this,
                    $"UPDATE pc.ETL_Workflow SET Ist_aktiv = 0 WHERE ETL_Workflow_ID = {workflowID};",
                    _dummyTuple
                );
                // reset counter if workflow is started again
                ResetFailedCreatingWorkflowErrorcount(workflowID);
            }
        }

        /// <summary>
        /// resets the counter of the workflow for failed creation when workflow was created
        /// </summary>
        /// <param name="workflowID">ID of workflow to remove from dictionary</param>
        public void ResetFailedCreatingWorkflowErrorcount(int workflowID)
        {
            _failedCreatingWorkflows.Remove(workflowID);
        }

        /// <summary>
        /// adds a given package with the task that executes it to the tracking list
        /// </summary>
        /// <param name="package">package to add to list</param>
        /// <param name="packageTask">task that executes the package</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        public void AddExecutingPackage(Package package,Task packageTask,Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                ExecPackageTaskLock.Wait();
                _usedSem = ExecPackageTaskLock;
                Task.Run(() => Log(
                    this, $"Add executing package {package.GetID()}", prozesslaeufe, Debug)).Wait();
                ExecutingPackagesTasks.Add(new Tuple<Package, Task>(package, packageTask));
                ExecPackageTaskLock.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "AddExecutingPackage",
                    "Failed adding executing package!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// removes the given package (with task) from list of executing packages
        /// </summary>
        /// <param name="package">package to remove</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void RemoveExecutingPackage(Package package,Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                ExecPackageTaskLock.Wait();
                _usedSem = ExecPackageTaskLock;
                Task.Run(() => Log(
                    this, $"Remove executing package {package.GetID()}", prozesslaeufe, Debug)).Wait();
                ExecutingPackagesTasks.RemoveAll(item => item.Item1 == package);
                ExecPackageTaskLock.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "RemoveExecutingPackage",
                    "Failed removing executing package!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// increases the number of threads
        /// </summary>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when #threads greater than allowed</exception>
        public void IncreaseThreadNumber(Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _numThreadLock.Wait();
                _usedSem = _numThreadLock;
                if (_numThreads + 1 > MaxThreads)
                    throw new ETLException(
                        this,
                        $"increasing the number of threads would exceed the allowed number of threads! (MaxThreads: " +
                        $"{MaxThreads} | Executing Would be: {_numThreads + 1})",
                        "IncreaseThreadNumber",
                        prozesslaeufe
                    );

                _numThreads++;
                Task.Run(() => {
                    Log(
                        this,
                        $"Increased number of Threads ({_numThreads - 1} -> {_numThreads})",
                        prozesslaeufe,
                        Debug
                    );
                }).Wait();
                _numThreadLock.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "IncreaseThreadNumber",
                    "Failed increasing the thread number!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }
        /// <summary>
        /// decreases the number of threads
        /// </summary>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when negative number of threads would be reached</exception>
        public void DecreaseThreadNumber(Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _numThreadLock.Wait();
                _usedSem = _numThreadLock;
                if (_numThreads - 1 < 0)
                    throw new ETLException(
                        this,
                        "Decreasing the number of threads would be negative!",
                        "DecreaseThreadNumber",
                        prozesslaeufe
                    );

                _numThreads--;
                Task.Run(() => {
                    Log(
                        this,
                        $"Decreased number of Threads ({_numThreads + 1} -> {_numThreads})",
                        prozesslaeufe,
                        Debug
                    );
                }).Wait();
                _numThreadLock.Release();
                _usedSem = null;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "DecreaseThreadNumber",
                    "Failed decreasing the thread number!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }
        /// <summary>
        /// checks if another thread can be started
        /// </summary>
        /// <returns>true if another thread can be started</returns>
        public bool CheckFreeThreads()
        {
            try
            {
                _numThreadLock.Wait();
                _usedSem = _numThreadLock;

                bool check = _numThreads < MaxThreads;

                _numThreadLock.Release();
                _usedSem = null;
                return check;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "DecreaseThreadNumber",
                    "Failed checking for free threads!",
                    ref tmp,
                    _dummyTuple
                );
            }
        }

        /// <summary>
        /// increase the number of executing entities on given level
        /// </summary>
        /// <param name="level">level to increase on</param>
        /// <param name="id">ID of item that wants to increase counter</param>
        /// <param name="locked">true if item wants to run in stand-alone</param>
        /// <param name="module">the object (workflow, package, realization, step) to increase counter for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="cancleSource">source that gets cancled when error appers</param>
        /// <exception cref="ETLException">when level not known</exception>
        public void IncreaseNumExecuting(
            Level level,
            int id,
            bool locked,
            object module,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            CancellationTokenSource cancleSource
        )
        {
            try
            {
                string logMessage;
                int numAnnounced;
                switch (level)
                {
                    case Level.Workflow:
                        _numExecWFLock.Wait();
                        _usedSem = _numExecWFLock;

                        _numExecutingWorkflows++;
                        if (!locked)
                            _numExecutingNormalWorkflows++;

                        ((Workflow)module).IncreasedNumExecWorkflows = true;

                        WorkflowManager.WorkflowSteerLock.Release();
                        numAnnounced = LockManager.GetNumAnnounced(Level.Workflow, prozesslaeufe);
                        int beforeNumNormalWorkflows = _numExecutingNormalWorkflows - 1 < 0 ? 0 :
                                                       _numExecutingNormalWorkflows - 1;
                        logMessage = $"Increasing number of executing {level}s ({_numExecutingWorkflows - 1} -> " +
                            $"{_numExecutingWorkflows} | {beforeNumNormalWorkflows} ->" +
                            $"{_numExecutingNormalWorkflows}) A: {numAnnounced} | E: {_numExecutingWorkflows} | " +
                            $"locked: {locked}";
                        Task.Run(() => { Log(this, logMessage, prozesslaeufe, Debug); }).Wait();

                        if (locked ||                                   // blocking process
                            !locked && _numExecutingWorkflows <= 1      // no blocking process with no exec. workflows
                        )
                        {
                            _numExecWFLock.Release();
                            _usedSem = null;

                            Task.Run(() => LockManager.GetSemaphore(
                                level,
                                id,
                                locked,
                                module,
                                this,
                                prozesslaeufe,
                                cancleSource.Token
                            ), cancleSource.Token).Wait();
                        } else
                        {
                            _numExecWFLock.Release();
                            _usedSem = null;
                        }

                        break;
                    case Level.Package:
                        _numExecPLock.Wait();
                        _usedSem = _numExecPLock;

                        _numExecutingPackages++;
                        if (!locked)
                            _numExecutingNormalPackages++;

                        ((Package)module).IncreasedNumExecPackages = true;

                        WorkflowManager.PackageSteerLock.Release();
                        numAnnounced = LockManager.GetNumAnnounced(Level.Package, prozesslaeufe);
                        int beforeNumNormalPackages = _numExecutingNormalPackages - 1 < 0 ? 0 :
                                                      _numExecutingNormalPackages - 1;
                        logMessage = $"Increasing number of executing {level}s ({_numExecutingPackages - 1} -> " +
                            $"{_numExecutingPackages} | {beforeNumNormalPackages} ->" +
                            $"{_numExecutingNormalPackages}) A: {numAnnounced} | E: {_numExecutingPackages} | " +
                            $"locked: {locked}";
                        Task.Run(() => { Log(this, logMessage, prozesslaeufe, Debug); }).Wait();

                        if (locked ||                                    // blocking process
                            !locked && _numExecutingNormalPackages <= 1  // no blocking process with no exec. packages
                        )
                        {
                            _numExecPLock.Release();
                            _usedSem = null;
                            Task.Run(() => LockManager.GetSemaphore(
                                level,
                                id,
                                locked,
                                module,
                                this,
                                prozesslaeufe,
                                cancleSource.Token
                            ), cancleSource.Token).Wait();
                        }
                        else
                        {
                            _numExecPLock.Release();
                            _usedSem = null;
                        }
                        break;
                    case Level.Realization:
                        _numExecRLock.Wait();
                        _usedSem = _numExecRLock;

                        _numExecutingRealizations++;
                        if (!locked)
                            _numExecutingNormalRealizations++;

                        ((Realization)module).IncreasedNumExecRealizations = true;
                        WorkflowManager.RealizationSteerLock.Release();
                        numAnnounced = LockManager.GetNumAnnounced(Level.Realization, prozesslaeufe);
                        int beforeNumNormalRealizations = _numExecutingNormalRealizations - 1 < 0 ? 0 :
                                                          _numExecutingNormalRealizations - 1;
                        logMessage = $"Increasing number of executing {level}s ({_numExecutingRealizations - 1} -> " +
                            $"{_numExecutingRealizations} | {beforeNumNormalRealizations} ->" +
                            $"{_numExecutingNormalRealizations}) A: {numAnnounced} | E: {_numExecutingRealizations} " +
                            $"| locked: {locked}";
                        Task.Run(() => { Log(this, logMessage, prozesslaeufe, Debug); }).Wait();

                        if (locked ||  // blocking process
                            !locked && _numExecutingNormalRealizations <= 1
                            // no blocking process with no exec. realizations
                        )
                        {
                            _numExecRLock.Release();
                            _usedSem = null;
                            Task.Run(() => LockManager.GetSemaphore(
                                level,
                                id,
                                locked,
                                module,
                                this,
                                prozesslaeufe,
                                cancleSource.Token
                            ), cancleSource.Token).Wait();
                        }
                        else
                        {
                            _numExecRLock.Release();
                            _usedSem = null;
                        }
                        break;
                    case Level.Step:
                        _numExecSLock.Wait();
                        _usedSem = _numExecSLock;

                        _numExecutingSteps++;
                        if (!locked)
                            _numExecutingNormalSteps++;

                        ((Step)module).IncreasedNumExecSteps = true;
                        WorkflowManager.StepSteerLock.Release();
                        numAnnounced = LockManager.GetNumAnnounced(Level.Step, prozesslaeufe);
                        int beforeNumNormalSteps = _numExecutingNormalSteps - 1 < 0 ? 0 : _numExecutingNormalSteps - 1;
                        logMessage = $"Increasing number of executing {level}s ({_numExecutingSteps - 1} -> " +
                            $"{_numExecutingSteps} | {beforeNumNormalSteps} -> {_numExecutingNormalSteps}) A" +
                            $": {numAnnounced} | E: {_numExecutingSteps} | locked: {locked}";
                        Task.Run(() => { Log(this, logMessage, prozesslaeufe, Debug); }).Wait();

                        if (locked ||                                 // blocking process
                            !locked && _numExecutingNormalSteps <= 1  // no blocking process with no exec. steps
                        )
                        {
                            _numExecSLock.Release();
                            _usedSem = null;
                            Task.Run(() => LockManager.GetSemaphore(
                                level,
                                id,
                                locked,
                                module,
                                this,
                                prozesslaeufe,
                                cancleSource.Token
                            ), cancleSource.Token).Wait();
                        } else
                        {
                            _numExecSLock.Release();
                            _usedSem = null;
                        }

                        break;
                    default:
                        throw new ETLException(
                            this,
                            $"No Level {level} were found! -> Could not increase counter",
                            "IncreaseNumExecuting",
                            prozesslaeufe
                        );
                }
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "IncreaseNumExecuting",
                    $"Failed Increasing the number of executing items on {level} level!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }
        /// <summary>
        /// decreases the number of executing entities on given level with freeing semaphores
        /// </summary>
        /// <param name="level">level to decrease on</param>
        /// <param name="locked">true if item wants to run in stand-alone</param>
        /// <param name="module">object of module (workflow, package, realization, step) to dec. counter for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when decreasing leads to negative number or level unknown</exception>
        public void DecreaseNumExecuting(
            Level level,
            bool locked,
            object module,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            int numAnnounced;
            try
            {
                switch (level)
                {
                    case Level.Workflow:
                        _numExecWFLock.Wait();
                        _usedSem = _numExecWFLock;
                        if (_numExecutingWorkflows - 1 < 0)
                            throw new ETLException(
                                this,
                                "Decreasing number of workflow would be negative",
                                "DecreaseNumExecuting",
                                prozesslaeufe
                            );

                        _numExecutingWorkflows--;
                        if (!locked)
                            _numExecutingNormalWorkflows--;

                        ((Workflow)module).IncreasedNumExecWorkflows = false;
                        numAnnounced = LockManager.GetNumAnnounced(Level.Workflow, prozesslaeufe);
                        Task.Run(() => {
                            Log(
                                this,
                                $"Decreasing number of executing {level}s ({_numExecutingWorkflows + 1} -> " +
                                $"{_numExecutingWorkflows} | {_numExecutingNormalWorkflows + 1} -> " +
                                $"{_numExecutingNormalWorkflows} ) - State after decrease: E: " +
                                $"{_numExecutingWorkflows} | A: {numAnnounced}",
                                prozesslaeufe,
                                Debug
                            );
                        }).Wait();

                        if (_numExecutingNormalWorkflows == 0 || locked)
                            LockManager.FreeSemaphore(level, prozesslaeufe);

                        _numExecWFLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _numExecPLock.Wait();
                        _usedSem = _numExecPLock;
                        if (_numExecutingPackages - 1 < 0)
                            throw new ETLException(
                                this,
                                "Decreasing number of packages would be negative",
                                "DecreaseNumExecuting",
                                prozesslaeufe
                            );

                        _numExecutingPackages--;
                        if (!locked)
                            _numExecutingNormalPackages--;

                        ((Package)module).IncreasedNumExecPackages = false;
                        numAnnounced = LockManager.GetNumAnnounced(Level.Package, prozesslaeufe);
                        Task.Run(() => {
                            Log(
                                this,
                                $"Decreasing number of executing {level}s ({_numExecutingPackages + 1} -> " +
                                $"{_numExecutingPackages} | {_numExecutingNormalPackages + 1} -> " +
                                $"{_numExecutingNormalPackages} ) - State after decrease: E: " +
                                $"{_numExecutingPackages} | A: {numAnnounced}",
                                prozesslaeufe,
                                Debug
                            );
                        }).Wait();

                        if (_numExecutingNormalPackages == 0 || locked)
                            LockManager.FreeSemaphore(level, prozesslaeufe);

                        _numExecPLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _numExecRLock.Wait();
                        _usedSem = _numExecRLock;
                        if (_numExecutingRealizations - 1 < 0)
                            throw new ETLException(
                                this,
                                "Decreasing number of realizations would be negative",
                                "DecreaseNumExecuting",
                                prozesslaeufe
                            );

                        _numExecutingRealizations--;
                        if (!locked)
                            _numExecutingNormalRealizations--;

                        ((Realization)module).IncreasedNumExecRealizations = false;
                        numAnnounced = LockManager.GetNumAnnounced(Level.Realization, prozesslaeufe);
                        Task.Run(() => {
                            Log(
                                this,
                                $"Decreasing number of executing {level}s ({_numExecutingRealizations + 1} -> " +
                                $"{_numExecutingRealizations} | {_numExecutingNormalRealizations + 1} -> " +
                                $"{_numExecutingNormalRealizations} ) - State after decrease: E: " +
                                $"{_numExecutingRealizations} | A: {numAnnounced}",
                                prozesslaeufe,
                                Debug
                            );
                        }).Wait();

                        if (_numExecutingNormalRealizations == 0 || locked)
                            LockManager.FreeSemaphore(level, prozesslaeufe);

                        _numExecRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _numExecSLock.Wait();
                        _usedSem = _numExecSLock;
                        if (_numExecutingSteps - 1 < 0)
                            throw new ETLException(
                                this,
                                "Decreasing number of steps would be negative",
                                "DecreaseNumExecuting",
                                prozesslaeufe
                            );

                        _numExecutingSteps--;
                        if (!locked)
                            _numExecutingNormalSteps--;

                        ((Step)module).IncreasedNumExecSteps = false;
                        numAnnounced = LockManager.GetNumAnnounced(Level.Step, prozesslaeufe);
                        Task.Run(() => {
                            Log(
                                this,
                                $"Decreasing number of executing {level}s ({_numExecutingSteps + 1} -> " +
                                $"{_numExecutingSteps} | {_numExecutingNormalSteps + 1} -> " +
                                $"{_numExecutingNormalSteps} ) - State after decrease: E: " +
                                $"{_numExecutingSteps} | A: {numAnnounced}",
                                prozesslaeufe,
                                Debug
                            );
                        }).Wait();

                        if (_numExecutingNormalSteps == 0 || locked)
                            LockManager.FreeSemaphore(level, prozesslaeufe);

                        _numExecSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            this,
                            $"No Level {level} were found! -> Could not decrease counter",
                            "DecreaseNumExecuting",
                            prozesslaeufe
                        );
                }
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "DecreaseNumExecuting",
                    $"Failed decreasing the number of executing processes on level {level}!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// decreases the needed counters without freeing semaphores. This is needed for abortions and waiting for
        /// semaphores
        /// </summary>
        /// <param name="level">level to decrease on</param>
        /// <param name="locked">true if item wants to run in stand-alone</param>
        /// <param name="module">object of module (workflow, package, realization, step) to dec. counter for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">when decreasing leads to negative number or level unknown</exception>
        public void DecreaseNumExecutingError(
            Level level,
            bool locked,
            object module,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            int numAnnounced;
            try
            {
                switch (level)
                {
                    case Level.Workflow:
                        _numExecWFLock.Wait();
                        _usedSem = _numExecWFLock;
                        if (_numExecutingWorkflows - 1 < 0)
                            throw new ETLException(
                                this,
                                "Decreasing number of workflow would be negative",
                                "DecreaseNumExecutingError",
                                prozesslaeufe
                            );

                        _numExecutingWorkflows--;
                        if (!locked)
                            _numExecutingNormalWorkflows--;

                        ((Workflow)module).IncreasedNumExecWorkflows = false;
                        numAnnounced = LockManager.GetNumAnnounced(Level.Workflow, prozesslaeufe);
                        Task.Run(() => {
                            Log(
                                this,
                                $"Decreasing number of executing {level}s ({_numExecutingWorkflows + 1} -> " +
                                $"{_numExecutingWorkflows} | {_numExecutingNormalWorkflows + 1} -> " +
                                $"{_numExecutingNormalWorkflows} ) - State after decrease: E: " +
                                $"{_numExecutingWorkflows} | A: {numAnnounced}",
                                prozesslaeufe,
                                Debug
                            );
                        }).Wait();

                        _numExecWFLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _numExecPLock.Wait();
                        _usedSem = _numExecPLock;
                        if (_numExecutingPackages - 1 < 0)
                            throw new ETLException(
                                this,
                                "Decreasing number of packages would be negative",
                                "DecreaseNumExecutingError",
                                prozesslaeufe
                            );

                        _numExecutingPackages--;
                        if (!locked)
                            _numExecutingNormalPackages--;

                        ((Package)module).IncreasedNumExecPackages = false;
                        numAnnounced = LockManager.GetNumAnnounced(Level.Package, prozesslaeufe);
                        Task.Run(() => {
                            Log(
                                this,
                                $"Decreasing number of executing {level}s ({_numExecutingPackages + 1} -> " +
                                $"{_numExecutingPackages} | {_numExecutingNormalPackages + 1} -> " +
                                $"{_numExecutingNormalPackages} ) - State after decrease: E: " +
                                $"{_numExecutingPackages} | A: {numAnnounced}",
                                prozesslaeufe,
                                Debug
                            );
                        }).Wait();

                        _numExecPLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _numExecRLock.Wait();
                        _usedSem = _numExecRLock;
                        if (_numExecutingRealizations - 1 < 0)
                            throw new ETLException(
                                this,
                                "Decreasing number of realizations would be negative",
                                "DecreaseNumExecutingError",
                                prozesslaeufe
                            );

                        _numExecutingRealizations--;
                        if (!locked)
                            _numExecutingNormalRealizations--;

                        ((Realization)module).IncreasedNumExecRealizations = false;
                        numAnnounced = LockManager.GetNumAnnounced(Level.Realization, prozesslaeufe);
                        Task.Run(() => {
                            Log(
                                this,
                                $"Decreasing number of executing {level}s ({_numExecutingRealizations + 1} -> " +
                                $"{_numExecutingRealizations} | {_numExecutingNormalRealizations + 1} -> " +
                                $"{_numExecutingNormalRealizations} ) - State after decrease: E: " +
                                $"{_numExecutingRealizations} | A: {numAnnounced}",
                                prozesslaeufe,
                                Debug
                            );
                        }).Wait();

                        _numExecRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _numExecSLock.Wait();
                        _usedSem = _numExecSLock;
                        if (_numExecutingSteps - 1 < 0)
                            throw new ETLException(
                                this,
                                "Decreasing number of steps would be negative",
                                "DecreaseNumExecutingError",
                                prozesslaeufe
                            );

                        _numExecutingSteps--;
                        if (!locked)
                            _numExecutingNormalSteps--;

                        ((Step)module).IncreasedNumExecSteps = false;
                        numAnnounced = LockManager.GetNumAnnounced(Level.Step, prozesslaeufe);
                        Task.Run(() => {
                            Log(
                                this,
                                $"Decreasing number of executing {level}s ({_numExecutingSteps + 1} -> " +
                                $"{_numExecutingSteps} | {_numExecutingNormalSteps + 1} -> " +
                                $"{_numExecutingNormalSteps} ) - State after decrease: E: " +
                                $"{_numExecutingSteps} | A: {numAnnounced}",
                                prozesslaeufe,
                                Debug
                            );
                        }).Wait();

                        _numExecSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            this,
                            $"No Level {level} were found! -> Could not decrease counter",
                            "DecreaseNumExecutingError",
                            prozesslaeufe
                        );
                }
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "DecreaseNumExecutingError",
                    $"Failed decreasing the number of executing processes on level {level}!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// add the given table to the accessed list
        /// </summary>
        /// <param name="table">name of table to add</param>
        /// <param name="workflow">Workflow pbject for placeholder replacing</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">table already in list</exception>
        public void AddAccessedTable(string table,Workflow workflow,Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                table = ReplacePlaceholder(this, workflow, table, prozesslaeufe, Debug);

                _accessedTablesLock.Wait();
                _usedSem = _accessedTablesLock;
                string output = $"ZugriffTabellen: ";
                foreach (string wf in _accessedTables)
                {
                    output += $"{wf}, ";
                }
                if (_accessedTables.Any(tab => tab == table))
                    throw new ETLException(
                        this,
                        $"Processor: There is already a table with name {table} in accessed list! ({output})",
                        "Processor.AddAccessedTable",
                        prozesslaeufe
                    );

                _accessedTables.Add(table);

                output = $"ZugriffTabellen: ";
                foreach (string wf in _accessedTables)
                {
                    output += $"{wf}, ";
                }
                _accessedTablesLock.Release();
                _usedSem = null;
                Task.Run(() => {
                    Log(
                        this,
                        $"Processor: Added table {table} to accessed tables in processor! ({output})",
                        prozesslaeufe,
                        Debug
                    );
                }).Wait();
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "Processor.AddAccessedTable",
                    $"Processor: Failed adding table {table} to accessed list!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }
        /// <summary>
        /// removes the given table from accessed tables
        /// </summary>
        /// <param name="table">name of table to remove</param>
        /// <param name="workflow">Workflow pbject for placeholder replacing</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">any error</exception>
        public void RemoveAccessedTable(string table,Workflow workflow,Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                table = ReplacePlaceholder(this, workflow, table, prozesslaeufe, Debug);

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
                        this,
                        $"Processor: Removed table {table} from accessed tables in processor! ({output})",
                        prozesslaeufe,
                        Debug
                    );
                }).Wait();
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "Processor.RemoveAccessedTable",
                    $"Processor: Failed removing accessed table {table} from list!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }
        /// <summary>
        /// checks if the given table is already accessed by another process
        /// </summary>
        /// <param name="table">table to access</param>
        /// <returns>true if table is already accessed, false when it can be used</returns>
        public bool CheckAccessedTable(string table)
        {
            try
            {
                _accessedTablesLock.Wait();
                _usedSem = _accessedTablesLock;

                bool check = _accessedTables.Contains(table);

                _accessedTablesLock.Release();
                _usedSem = null;
                return check;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    this,
                    e,
                    "CheckAccessedTable",
                    $"Failed checking if table is already accessed!",
                    ref tmp,
                    _dummyTuple
                );
            }
        }

        /// <summary>
        /// iteratively checks if a workflow can be started
        /// </summary>
        public void StartProcessor()
        {
            // do first check for workflows to execute
            try
            {
                Task.Run(() => Log(this, $"BaseDirectory: {BaseDirectory}", _dummyTuple)).Wait();
                Task.Run(() => Log(this, $"Check for Workflows to execute ({DateTime.Now})", _dummyTuple)).Wait();
                Task.Run(() => CheckForExecution(DateTime.Now));
            }
            catch (Exception e)
            {
                Task.Run(() => {
                    ErrorLog(
                        this,
                        "Dienst",
                        "First check for workflows to execute failed!",
                        "major",
                        e,
                        "StartProcessor",
                        _dummyTuple
                    );
                }).Wait();
            }

            // create timer that all intervall seconds checks if a workflow needs to be executed within the next
            // interval seconds
            try
            {
                System.Timers.Timer timer = new();
                void OnElapsedTime(object? source, ElapsedEventArgs e)
                {
                    DateTime jetzt = DateTime.Now;
                    if (Debug || (jetzt - LastSchedulerMessage).TotalMinutes >= MaxWaitWithoutMessage)
                    {
                        Task.Run(() => Log(this, $"Check for Workflows to execute ({jetzt})", _dummyTuple)).Wait();
                        Task.Delay(5).Wait();
                    }
                    Task.Run(() => UpdateParameters());
                    Task.Run(() => CheckForExecution(jetzt));
                }
                timer.Elapsed += new ElapsedEventHandler(OnElapsedTime);
                timer.Interval = 1000 * Intervall;
                timer.Enabled = true;
            }
            catch (Exception e)
            {
                Task.Run(() => {
                    ErrorLog(
                        this,
                        "Dienst",
                        "Running the periodic timer failed!",
                        "major",
                        e,
                        "StartProcessor",
                        _dummyTuple
                    );
                }).Wait();
            }
        }

        /// <summary>
        /// checks if a configuration parameter has changed in DB and adapts service to it
        /// </summary>
        /// <param name="start">signalizes if update is done for the first time at service start</param>
        private void UpdateParameters(bool start = false)
        {
            Task.Run(() => Log(
                this,
                $"Updating Parameters!",
                _dummyTuple,
                Debug
            )).Wait();

            Dictionary<string, object> parameters = ReadConfigurations(this);

            try
            {
                if (start || Debug != (bool)parameters["Debug"])
                    Task.Run(() => Log(
                        this,
                        $"PARAMETER Debug: {parameters["Debug"]}",
                        _dummyTuple
                    )).Wait();

                Debug = (bool)parameters["Debug"];
            }
            catch
            {
                Debug = true;
                Task.Run(() => Log(
                    this,
                    $"PARAMETER Debug: {Debug}",
                    _dummyTuple
                )).Wait();
            }

            try
            {
                if (start || MaxThreads != (int)parameters["Anzahl_ETL_Threads"])
                    Task.Run(() => Log(
                        this,
                        $"PARAMETER Max. Number of Threads: {parameters["Anzahl_ETL_Threads"]}",
                        _dummyTuple
                    )).Wait();

                MaxThreads = (int)parameters["Anzahl_ETL_Threads"];
            }
            catch
            {
                MaxThreads = 10;
                Task.Run(() => Log(
                    this,
                    $"PARAMETER Max. Number of Threads: {MaxThreads}",
                    _dummyTuple
                )).Wait();
            }

            try
            {
                if (start || MaxWaitWithoutMessage != (int)parameters["LogInterval"])
                    Task.Run(() => Log(
                        this,
                        $"PARAMETER Logging Intervall: {parameters["LogInterval"]}",
                        _dummyTuple
                    )).Wait();

                MaxWaitWithoutMessage = (int)parameters["LogInterval"];
            }
            catch
            {
                MaxWaitWithoutMessage = 5;
                Task.Run(() => Log(
                    this,
                    $"PARAMETER Logging Intervall: {MaxWaitWithoutMessage}",
                    _dummyTuple
                )).Wait();
            }
        }

        /// <summary>
        /// reads all workflows that are active and checks if some of them need to be started within the next
        /// [interval] seconds and starts them
        /// </summary>
        /// <param name="start">DateTime of start</param>
        /// <exception cref="ETLException">in case of any error</exception>
        private void CheckForExecution(DateTime start)
        {
            Scheduler scheduler = new(this);

            // receive schedule to execute
            DataTable schedule;
            try
            {
                schedule = scheduler.GetWorkflows();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    this,
                    e,
                    "CheckForExecution",
                    "Retrieving scheduled workflows failed!",
                    ref s_dummySem,
                    _dummyTuple
                );
            }

            // execute scheduled workflows
            try
            {
                // check for scheduled workflows and check for execution
                if (schedule.Rows.Count > 0)
                {
                    foreach (DataRow workflow in schedule.Rows)
                    {
                        // extract workflow information
                        int workflowID;
                        string requestDate;
                        int paketeID;
                        int zeitplanAusfuehrungenID;
                        try
                        {
                            workflowID = int.Parse(workflow["ETL_WORKFLOW_ID"].ToString() ??
                                                    throw new ETLException("No ETL_WORKFLOW_ID"));
                            paketeID = int.Parse(workflow["ETL_PAKETE_ID"].ToString() ??
                                                    throw new ETLException("No ETL_PAKETE_ID"));
                            zeitplanAusfuehrungenID = WorkflowManager.GetZeitplanAusfuehrungenID(
                                int.Parse(workflow["ETL_WORKFLOW_ID"].ToString() ??
                                                    throw new ETLException("No ETL_WORKFLOW_ID")),
                                _dummyTuple
                            );
                            requestDate = workflow["Anforderungszeitpunkt"].ToString() ??
                                            throw new ETLException("No Anforderungszeitpunkt");
                        }
                        catch (Exception e)
                        {
                            Task.Run(() => {
                                ErrorLog(
                                    this,
                                    "Dienst",
                                    "Extracting Workflow_ID, Anforderungszeitpunkt or Prozesslaeufe_ID failed!",
                                    "major",
                                    e,
                                    "CheckForExecution",
                                    _dummyTuple
                                );
                            }).Wait();
                            continue;
                        }

                        // check if a workflow already exists (in right state) and initializes new run or create a new
                        if (WorkflowManager.ExistsWorkflow(workflowID, _dummyTuple))
                        {
                            Workflow w = WorkflowManager.GetWorkflow(workflowID, _dummyTuple);
                            if (WorkflowManager.IsWorkflow(WorkflowStage.Executing, w, _dummyTuple))
                            {
                                continue;
                            } else
                            {
                                WorkflowStage stage = WorkflowManager.GetWorkflowStage(w, _dummyTuple);

                                if (stage == WorkflowStage.Scheduled)
                                {
                                    // calculate the time to wait for the Anforderungszeitpunkt
                                    int secondsToStartWorkflows = (int)(DateTime.Parse(
                                        requestDate) - start).TotalSeconds;
                                    secondsToStartWorkflows = secondsToStartWorkflows < 0 ? 0 : secondsToStartWorkflows;

                                    // check if this workflow shall be started within this interval
                                    if (secondsToStartWorkflows <= Intervall)
                                    {
                                        // initialize the workflow
                                        try
                                        {
                                            // start workflow
                                            Task.Run(() => w.Init(DateTime.Parse(requestDate)));
                                        }
                                        catch (Exception e)
                                        {
                                            throw HandleErrorCatch(
                                                this,
                                                e,
                                                "CheckForExecution",
                                                "Initializing workflow failed!",
                                                ref DummySem,
                                                prozesslaeufe: _dummyTuple
                                            );
                                        }
                                    }
                                } else
                                {
                                    continue;
                                }
                            }
                        } else // new workflow to create
                        {
                            throw new ETLException(
                                this,
                                "Scheduled workflow is not known in system!",
                                "CheckForExecution",
                                _dummyTuple
                            );
                        }
                    }
                }
                else
                {
                    Task.Run(() => Log(this, "No scheduled workflows to execute", _dummyTuple, Debug)).Wait();
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    this,
                    e,
                    "CheckForExecution",
                    "Starting workflows failed",
                    ref DummySem,
                    _dummyTuple
                );
            }
        }
    }
}
