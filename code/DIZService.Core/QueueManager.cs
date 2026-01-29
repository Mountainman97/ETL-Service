namespace DIZService.Core
{
    // handles all queueing variables and gives multiple helper functions to
    // easy control queues
    public class QueueManager(Processor processor, bool debug) : Helper
    {
        private readonly bool _debug = debug;         // use for logging
        private readonly Processor _processor = processor;

        // ------------------- QUEUES FOR WAITING PROCESSES --------------------
        public List<int> WaitingWorkflows = [];     // lists all IDs of workflows waiting for execution
        public List<int> WaitingPackages = [];      // lists all IDs of packages waiting for execution
        public List<int> WaitingRealizations = [];  // lists all IDs of realizations waiting for execution
        public List<int> WaitingSteps = [];         // lists all IDs of steps waiting for execution

        // ------------------- QUEUES FOR PROCESSES WAITING FOR SEMAPHORES --------------------
        public List<int> WaitingSemWorkflows = [];
        public List<int> WaitingSemPackages = [];
        public List<int> WaitingSemRealizations = [];
        public List<int> WaitingSemSteps = [];

        // ------------------- SEMAPHORES TO GET UNIQUE ACCESS ON QUEUES --------------------
        private readonly SemaphoreSlim _queueWLock = new(1, 1);  // workflow
        private readonly SemaphoreSlim _queuePLock = new(1, 1);  // package
        private readonly SemaphoreSlim _queueRLock = new(1, 1);  // realization
        private readonly SemaphoreSlim _queueSLock = new(1, 1);  // step

        // ------------------- SEMAPHORES TO GET UNIQUE ACCESS ON SEMAPHORE QUEUES --------------------
        private readonly SemaphoreSlim _semQueueWLock = new(1, 1);  // workflow
        private readonly SemaphoreSlim _semQueuePLock = new(1, 1);  // package
        private readonly SemaphoreSlim _semQueueRLock = new(1, 1);  // realization
        private readonly SemaphoreSlim _semQueueSLock = new(1, 1);  // step

        // maps lists with waiting IDs (steps) to tables
        public Dictionary<string, List<int>> WaitingTableQueues = [];
        // use to grant unique access on table queue
        private readonly SemaphoreSlim _queueTableLock = new(1, 1);

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        // ------------------------ HANDLE LEVEL QUEUES ------------------------

        /// <summary>
        /// adds a given ID to the queue of the given level
        /// </summary>
        /// <param name="level">Level to add to queue</param>
        /// <param name="id">ID of item to add to queue</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">if unknown level or other error</exception>
        public void AddToQueue(Level level,int id,Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                string list;
                switch (level)
                {
                    case Level.Workflow:
                        _queueWLock.Wait();
                        _usedSem = _queueWLock;

                        Task.Run(() => WaitingWorkflows.Add(id)).Wait();
                        list = GetListString(_processor, "Workflow Queue", WaitingWorkflows, prozesslaeufe);

                        _queueWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _queuePLock.Wait();
                        _usedSem = _queuePLock;

                        Task.Run(() => WaitingPackages.Add(id)).Wait();
                        list = GetListString(_processor, "Package Queue", WaitingPackages, prozesslaeufe);

                        _queuePLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _queueRLock.Wait();
                        _usedSem = _queueRLock;

                        Task.Run(() => WaitingRealizations.Add(id)).Wait();
                        list = GetListString(
                            _processor, "Realization Queue", WaitingRealizations, prozesslaeufe);

                        _queueRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _queueSLock.Wait();
                        _usedSem = _queueSLock;

                        Task.Run(() => WaitingSteps.Add(id)).Wait();
                        list = GetListString(_processor, "Step Queue", WaitingSteps, prozesslaeufe);

                        _queueSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(_processor, $"Unknown Level! ({level})", "AddToQueue", prozesslaeufe);
                }
                Task.Run(() => {
                    Log(
                        _processor,
                        $"Added {id} to queue on {level} level! ({list})",
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
                    "AddToQueue",
                    $"Failed adding item with ID {id} on {level} level to queue!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if the given ID is first in queue of the given level -> true, otherwise false
        /// </summary>
        /// <param name="level">Level to check queue</param>
        /// <param name="id">ID to check for first</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if ID is first in queue, otherwise false</returns>
        /// <exception cref="ETLException">unknown level or other error</exception>
        public bool CheckQueueFirst(Level level, int id, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                bool first = false;
                switch (level)
                {
                    case Level.Workflow:
                        _queueWLock.Wait();
                        _usedSem = _queueWLock;

                        first = WaitingWorkflows.IndexOf(id) == 0;

                        _queueWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _queuePLock.Wait();
                        _usedSem = _queuePLock;

                        first = WaitingPackages.IndexOf(id) == 0;

                        _queuePLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _queueRLock.Wait();
                        _usedSem = _queueRLock;

                        first = WaitingRealizations.IndexOf(id) == 0;

                        _queueRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _queueSLock.Wait();
                        _usedSem = _queueSLock;

                        first = WaitingSteps.IndexOf(id) == 0;

                        _queueSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is no level {level} to check for first item in queue!",
                            "CheckQueueFirst",
                            prozesslaeufe
                        );
                }

                Task.Run(() => Log(
                    _processor, $"Check for first Item in Queue for {id}: {first}!", prozesslaeufe, _debug
                )).Wait();

                if (first)
                    Task.Run(() => Log(
                        _processor,
                        $"Item with ID {id} is next in Queue on {level} level!",
                        prozesslaeufe,
                        _debug
                    )).Wait();

                return first;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "CheckQueueFirst",
                    $"Failed checking if {level} with ID {id} is first in queue!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// removes the given ID from queue of given level
        /// </summary>
        /// <param name="level">Level from whose queue to remove</param>
        /// <param name="id">ID to remove</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">unknown level or other error</exception>
        public void RemoveFromQueue(Level level, int id, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                string list;
                switch (level)
                {
                    case Level.Workflow:
                        _queueWLock.Wait();
                        _usedSem = _queueWLock;

                        WaitingWorkflows.Remove(id);
                        list = GetListString(_processor, "Workflow Queue", WaitingWorkflows, prozesslaeufe);

                        _queueWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _queuePLock.Wait();
                        _usedSem = _queuePLock;

                        WaitingPackages.Remove(id);
                        list = GetListString(_processor, "Package Queue", WaitingPackages, prozesslaeufe);

                        _queuePLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _queueRLock.Wait();
                        _usedSem = _queueRLock;

                        WaitingRealizations.Remove(id);
                        list = GetListString(
                            _processor, "Realization Queue", WaitingRealizations, prozesslaeufe);

                        _queueRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _queueSLock.Wait();
                        _usedSem = _queueSLock;

                        WaitingSteps.Remove(id);
                        list = GetListString(_processor, "Step Queue", WaitingSteps, prozesslaeufe);

                        _queueSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is not level {level} to remove item from queue!",
                            "RemoveFromQueue",
                            prozesslaeufe
                        );
                }
                Task.Run(() => {
                    Log(
                        _processor,
                        $"Removed {id} from queue on {level} level! ({list})",
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
                   "RemoveFromQueue",
                    $"Failed removing {level} with ID {id} from queue!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        // ------------------------ HANDLE SEMAPHORE QUEUES ------------------------

        /// <summary>
        /// adds a given ID to the queue of the given level
        /// </summary>
        /// <param name="level">Level to add to queue</param>
        /// <param name="id">ID of item to add to queue</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">if unknown level or other error</exception>
        public void AddToSemaphoreQueue(Level level, int id, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                string list;
                switch (level)
                {
                    case Level.Workflow:
                        _semQueueWLock.Wait();
                        _usedSem = _semQueueWLock;

                        Task.Run(() => WaitingSemWorkflows.Add(id)).Wait();
                        list = GetListString(
                            _processor, "Workflow Semaphore Queue", WaitingSemWorkflows, prozesslaeufe);

                        _semQueueWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _semQueuePLock.Wait();
                        _usedSem = _semQueuePLock;

                        Task.Run(() => WaitingSemPackages.Add(id)).Wait();
                        list = GetListString(
                            _processor, "Package Semaphore Queue", WaitingSemPackages, prozesslaeufe);

                        _semQueuePLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _semQueueRLock.Wait();
                        _usedSem = _semQueueRLock;

                        Task.Run(() => WaitingSemRealizations.Add(id)).Wait();
                        list = GetListString(
                            _processor, "Realization Semaphore Queue", WaitingSemRealizations, prozesslaeufe);

                        _semQueueRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _semQueueSLock.Wait();
                        _usedSem = _semQueueSLock;

                        Task.Run(() => WaitingSemSteps.Add(id)).Wait();
                        list = GetListString(
                            _processor, "Step Semaphore Queue", WaitingSemSteps, prozesslaeufe);

                        _semQueueSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"Unknown Level! ({level})",
                            "AddToSemaphoreQueue",
                            prozesslaeufe
                        );
                }
                Task.Run(() => {
                    Log(
                        _processor,
                        $"Added {id} to semaphore queue on {level} level! ({list})",
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
                    "AddToSemaphoreQueue",
                    $"Failed adding item with ID {id} on {level} level to semaphore queue!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if the given ID is first in queue of the given level -> true, otherwise false
        /// </summary>
        /// <param name="level">Level to check queue</param>
        /// <param name="id">ID to check for first</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if ID is first in queue, otherwise false</returns>
        /// <exception cref="ETLException">unknown level or other error</exception>
        public bool CheckSemaphoreQueueFirst(Level level, int id, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                bool first = false;
                switch (level)
                {
                    case Level.Workflow:
                        _semQueueWLock.Wait();
                        _usedSem = _semQueueWLock;

                        first = WaitingSemWorkflows.IndexOf(id) == 0;

                        _semQueueWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _semQueuePLock.Wait();
                        _usedSem = _semQueuePLock;

                        first = WaitingSemPackages.IndexOf(id) == 0;

                        _semQueuePLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _semQueueRLock.Wait();
                        _usedSem = _semQueueRLock;

                        first = WaitingSemRealizations.IndexOf(id) == 0;

                        _semQueueRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _semQueueSLock.Wait();
                        _usedSem = _semQueueSLock;

                        first = WaitingSemSteps.IndexOf(id) == 0;

                        _semQueueSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is no level {level} to check for first item in semaphore queue!",
                            "CheckSemaphoreQueueFirst",
                            prozesslaeufe
                        );
                }

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Check for first Item in semaphore Queue for {id}: {first}!",
                        prozesslaeufe,
                        _debug
                    );
                }).Wait();

                if (first)
                    Task.Run(() => {
                        Log(
                            _processor,
                            $"Item with ID {id} is next in semaphore Queue on {level} level!",
                            prozesslaeufe,
                            _debug
                        );
                    }).Wait();

                return first;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "CheckSemaphoreQueueFirst",
                    $"Failed checking if {level} with ID {id} is first in semaphore queue!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// removes the given ID from queue of given level
        /// </summary>
        /// <param name="level">Level from whose queue to remove</param>
        /// <param name="id">ID to remove</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">unknown level or other error</exception>
        public void RemoveFromSemaphoreQueue(Level level, int id, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                string list;
                switch (level)
                {
                    case Level.Workflow:
                        _semQueueWLock.Wait();
                        _usedSem = _semQueueWLock;

                        WaitingSemWorkflows.Remove(id);
                        list = GetListString(
                            _processor, "Workflow Semaphore Queue", WaitingSemWorkflows, prozesslaeufe);

                        _semQueueWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _semQueuePLock.Wait();
                        _usedSem = _semQueuePLock;

                        WaitingSemPackages.Remove(id);
                        list = GetListString(
                            _processor, "Package Semaphore Queue", WaitingSemPackages, prozesslaeufe);

                        _semQueuePLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _semQueueRLock.Wait();
                        _usedSem = _semQueueRLock;

                        WaitingSemRealizations.Remove(id);
                        list = GetListString(
                            _processor, "Realization Semaphore Queue", WaitingSemRealizations, prozesslaeufe);

                        _semQueueRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _semQueueSLock.Wait();
                        _usedSem = _semQueueSLock;

                        WaitingSemSteps.Remove(id);
                        list = GetListString(
                            _processor, "Step Semaphore Queue", WaitingSemSteps, prozesslaeufe);

                        _semQueueSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is not level {level} to remove item from semaphore queue!",
                            "RemoveFromSemaphoreQueue",
                            prozesslaeufe
                        );
                }
                Task.Run(() => {
                    Log(
                        _processor,
                        $"Removed {id} from semaphore queue on {level} level! ({list})",
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
                   "RemoveFromSemaphoreQueue",
                    $"Failed removing {level} with ID {id} from semaphore queue!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        // ------------------------ HANDLE TABLE QUEUES ------------------------

        /// <summary>
        /// creates or adds the given ID to the queue of a table
        /// </summary>
        /// <param name="id">ID to add to queue</param>
        /// <param name="table">Tablename to create queue for or add ID to</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">any error</exception>
        public void AddToTableQueue(int id, string table, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _queueTableLock.Wait();
                _usedSem = _queueTableLock;

                if (WaitingTableQueues.TryGetValue(table, out List<int>? value))
                {
                    value.Add(id);
                    Task.Run(() => {
                        Log(
                            _processor,
                            $"Added {id} to queue of table {table}!",
                            prozesslaeufe,
                            _debug
                        );
                    }).Wait();
                }
                else
                {  // add new key
                    WaitingTableQueues[table] = [id];
                    Task.Run(() => {
                        Log(
                            _processor,
                            $"Created new Queue with {id} for table {table}!",
                            prozesslaeufe,
                            _debug
                        );
                    }).Wait();
                }
                _queueTableLock.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "AddToTableQueue",
                    $"Failed adding item with ID {id} from queue for table {table}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if the given ID is in first place in queue of the given table
        /// </summary>
        /// <param name="id">ID to check for first place</param>
        /// <param name="table">tablename to check for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if ID is first in queue, false otherwise</returns>
        /// <exception cref="ETLException">any error</exception>
        public bool CheckTableQueueFirst(int id, string table, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _queueTableLock.Wait();
                _usedSem = _queueTableLock;

                bool queueFirst = WaitingTableQueues[table].IndexOf(id) == 0;
                Task.Run(() => Log(
                    _processor,
                    $"Check for first item in table Queue of {id}: {queueFirst}",
                    prozesslaeufe,
                    _debug
                )).Wait();

                _queueTableLock.Release();
                _usedSem = null;

                return queueFirst;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "CheckTableQueueFirst",
                    $"Failed checking if item with ID {id} is first in queue for table {table}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// removes the given ID from queue of the given table
        /// </summary>
        /// <param name="id">ID to remove from queue</param>
        /// <param name="table">tablename to get right queue</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">any error</exception>
        public void RemoveFromTableQueue(int id, string table, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            try
            {
                _queueTableLock.Wait();
                _usedSem = _queueTableLock;

                WaitingTableQueues[table].Remove(id);
                Task.Run(() => {
                    Log(
                        _processor,
                        $"Removed {id} from queue of table {table}!",
                        prozesslaeufe,
                        _debug
                    );
                }).Wait();

                _queueTableLock.Release();
                _usedSem = null;
            }
            catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                   _processor,
                   e,
                   "RemoveFromTableQueue",
                    $"Failed removing item with ID {id} from queue for table {table}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }
    }
}
