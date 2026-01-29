namespace DIZService.Core
{
    // handles all variables that handle the locking on different levels and
    // gives multiple helper functions for easy access
    public class ParallelLockManager(Processor processor, bool debug) : Helper
    {
        private readonly bool _debug = debug;  // use for logging
        private readonly Processor _processor = processor;

        // ----------------- FLAGS TO SIGNALIZE LOCK ON LEVEL ------------------
        // -----------------  set to true to run separately   ------------------
        private bool _lockWorkflow = false;     // signalizes lock for workflow -> no workflow will start
        private readonly SemaphoreSlim _varLockWLock = new(1, 1);
        private bool _lockPackage = false;      // signalizes lock for package -> no package will start
        private readonly SemaphoreSlim _varLockPLock = new(1, 1);
        private bool _lockRealization = false;  // signalizes lock for realization -> no realization will start
        private readonly SemaphoreSlim _varLockRLock = new(1, 1);
        private bool _lockStep = false;         // signalizes lock for step -> no step will start
        private readonly SemaphoreSlim _varLockSLock = new(1, 1);

        private int _announcedWorkflows = 0;        // counts how many workflows announced a lock
        private int _announcedPackages = 0;         // counts how many packages announced a lock
        private int _announcedRealizations = 0;     // counts how many realizations announced a lock
        private int _announcedSteps = 0;            // counts how many steps announced a lock

        // ------------------- SEMAPHORES TO LOCK EXECUTIONS -------------------
        // -------------------  WaitOne() to lock execution  -------------------
        private readonly SemaphoreSlim _workflowSemaphore = new(1, 1);
        private readonly SemaphoreSlim _packageSemaphore = new(1, 1);
        private readonly SemaphoreSlim _realizationSemaphore = new(1, 1);
        private readonly SemaphoreSlim _stepSemaphore = new(1, 1);

        // semaphore to lock, when increasing or decreasing the thread and executing numbers
        public readonly SemaphoreSlim NumberIncDecSemaphore = new(1, 1);

        // general placeholder to check which semaphore is used at the moment to release it in case of error
        private SemaphoreSlim? _usedSem = null;

        // -------------------- FUNCTIONS TO ANNOUNCE LOCK ---------------------

        /// <summary>
        /// returns the number of processes on given level that have announced to run in locked mode
        /// </summary>
        /// <param name="level">level get number of announced locks for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>number of processes on given level announced lock</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public int GetNumAnnounced(
            Level level,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                return level switch
                {
                    Level.Workflow => _announcedWorkflows,
                    Level.Package => _announcedPackages,
                    Level.Realization => _announcedRealizations,
                    Level.Step => _announcedSteps,
                    _ => throw new ETLException(
                        _processor,
                        "Given Level was not found!",
                        "GetNumAnnounced",
                        prozesslaeufe
                    ),
                };
            } catch (Exception e)
            {
                throw new ETLException(
                    _processor,
                    $"Could not determine the number of announced locking processes on {level} level!",
                    "GetNumAnnounced",
                    e,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// sets the flag on given level for locking the process to true
        /// </summary>
        /// <param name="level">Level to lock execution on</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">unknown level or any other error</exception>
        public void AnnounceLock(
            Level level,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                switch (level)
                {
                    case Level.Workflow:
                        _varLockWLock.Wait();
                        _usedSem = _varLockWLock;
                        _announcedWorkflows++;
                        _lockWorkflow = true;
                        Task.Run(() => {
                            Log(
                                _processor,
                                $"Anounced Lock on {level} level! ({_lockWorkflow} | {_announcedWorkflows})",
                                prozesslaeufe,
                                _debug
                            );
                        }).Wait();
                        _varLockWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _varLockPLock.Wait();
                        _usedSem = _varLockPLock;
                        _lockPackage = true;
                        _announcedPackages++;
                        Task.Run(() => {
                            Log(
                                _processor,
                                $"Anounced Lock on {level} level! ({_lockPackage} | {_announcedPackages})",
                                prozesslaeufe,
                                _debug
                            );
                        }).Wait();
                        _varLockPLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _varLockRLock.Wait();
                        _usedSem = _varLockRLock;
                        _lockRealization = true;
                        _announcedRealizations++;
                        Task.Run(() => {
                            Log(
                                _processor,
                                $"Anounced Lock on {level} level! ({_lockRealization} | {_announcedRealizations})",
                                prozesslaeufe,
                                _debug
                            );
                        }).Wait();
                        _varLockRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _varLockSLock.Wait();
                        _usedSem = _varLockSLock;
                        _lockStep = true;
                        _announcedSteps++;
                        Task.Run(() => {
                            Log(
                                _processor,
                                $"Anounced Lock on {level} level! ({_lockStep} | {_announcedSteps})",
                                prozesslaeufe,
                                _debug
                            );
                        }).Wait();
                        _varLockSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is no level {level} to announce a lock for!",
                            "AnnounceLock",
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
                   "AnnounceLock",
                   $"Failed announcing a lock in level {level}!",
                   ref tmp,
                   prozesslaeufe
                );
            }
        }

        /// <summary>
        /// sets the flag for waiting lock process to false
        /// </summary>
        /// <param name="level">level to remove flag for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">level unknown or other error</exception>
        public void RemoveLockFlag(
            Level level,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                switch (level)
                {
                    case Level.Workflow:
                        _varLockWLock.Wait();
                        _usedSem = _varLockWLock;
                        _announcedWorkflows--;
                        if (_announcedWorkflows == 0)
                        {
                            _lockWorkflow = false;
                        }
                        Task.Run(() => {
                            Log(
                                _processor,
                                $"Removed Lock on {level} level! ({_lockWorkflow} | {_announcedWorkflows})",
                                prozesslaeufe,
                                _debug
                            );
                        }).Wait();
                        _varLockWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _varLockPLock.Wait();
                        _usedSem = _varLockPLock;
                        _announcedPackages--;
                        if (_announcedPackages == 0)
                        {
                            _lockPackage = false;
                        }
                        Task.Run(() => {
                            Log(
                                _processor,
                                $"Removed Lock on {level} level! ({_lockPackage} | {_announcedPackages})",
                                prozesslaeufe,
                                _debug
                            );
                        }).Wait();
                        _varLockPLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _varLockRLock.Wait();
                        _usedSem = _varLockRLock;
                        _announcedRealizations--;
                        if (_announcedRealizations == 0)
                        {
                            _lockRealization = false;
                        }
                        Task.Run(() => {
                            Log(
                                _processor,
                                $"Removed Lock on {level} level! ({_lockRealization} | {_announcedRealizations})",
                                prozesslaeufe,
                                _debug
                            );
                        }).Wait();
                        _varLockRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _varLockSLock.Wait();
                        _usedSem = _varLockSLock;
                        _announcedSteps--;
                        if (_announcedSteps == 0)
                        {
                            _lockStep = false;
                        }
                        Task.Run(() => {
                            Log(
                                _processor,
                                $"Removed Lock on {level} level! ({_lockStep} | {_announcedSteps})",
                                prozesslaeufe,
                                _debug
                            );
                        }).Wait();
                        _varLockSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is no level {level} to remove a lock for!",
                            "RemoveLockFlag",
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
                    "RemoveLockFlag",
                    $"Failed removing the lock on level {level}!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// checks if the given level is announced to be locked
        /// </summary>
        /// <param name="level">level to check for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>true if announced, false otherwise</returns>
        /// <exception cref="ETLException">unknown level</exception>
        public bool IsAnnounced(
            Level level,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                bool announced = false;
                switch (level)
                {
                    case Level.Workflow:
                        _varLockWLock.Wait();
                        _usedSem = _varLockWLock;
                        announced = _lockWorkflow;
                        _varLockWLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Package:
                        _varLockPLock.Wait();
                        _usedSem = _varLockPLock;
                        announced = _lockPackage;
                        _varLockPLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Realization:
                        _varLockRLock.Wait();
                        _usedSem = _varLockRLock;
                        announced = _lockRealization;
                        _varLockRLock.Release();
                        _usedSem = null;
                        break;
                    case Level.Step:
                        _varLockSLock.Wait();
                        _usedSem = _varLockSLock;
                        announced = _lockStep;
                        _varLockSLock.Release();
                        _usedSem = null;
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            "There is no such level to check for announced lock",
                            "IsAnnounced",
                            prozesslaeufe
                        );
                }

                Task.Run(() => Log(
                    _processor, $"Announced lock for {level}:{announced}", prozesslaeufe, _debug)).Wait();

                return announced;
            } catch (Exception e)
            {
                SemaphoreSlim tmp = _usedSem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "IsAnnounced",
                    $"Failed checking if lock on {level} level is announced!",
                    ref tmp,
                    prozesslaeufe
                );
            }

        }

        // ------------- GET SEMAPHORE PLACE TO RUN IN STAND-ALONE -------------

        /// <summary>
        /// waits for free semaphore on given level
        /// </summary>
        /// <param name="level">Level to get the semaphore for</param>
        /// <param name="id">ID of module to get semaphore</param>
        /// <param name="locked">true if module is locked</param>
        /// <param name="module">the module (workflow, package, realization, step) to get semaphore for</param>
        /// <param name="processor">processor controller to get right semaphore and queue</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="token">Token that is called when cancellation is started</param>
        /// <exception cref="ETLException">unknown level or any other error</exception>
        public void GetSemaphore(
            Level level,
            int id,
            bool locked,
            object module,
            Processor processor,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            CancellationToken token
        )
        {
            bool addedToQueue = false;
            try
            {
                processor.QueueManager.AddToSemaphoreQueue(level, id, prozesslaeufe);
                addedToQueue = true;

                Task.Run(() => {
                    Log(_processor, $"Wait until first in Semaphore queue!", prozesslaeufe, _debug);
                }, token).Wait(token);

                while (!processor.QueueManager.CheckSemaphoreQueueFirst(level, id, prozesslaeufe))
                {
                    Task.Delay(processor.WaitingTime, token).Wait(token);
                }

                Task.Run(() => {
                    Log(_processor, $"Wait for open Semaphore on {level} level!", prozesslaeufe, _debug);
                }, token).Wait(token);

                switch (level)
                {
                    case Level.Workflow:
                        _workflowSemaphore.Wait(token);
                        break;
                    case Level.Package:
                        _packageSemaphore.Wait(token);
                        break;
                    case Level.Realization:
                        _realizationSemaphore.Wait(token);
                        break;
                    case Level.Step:
                        _stepSemaphore.Wait(token);
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is no level {level} to get a semaphore of!",
                            "GetSemaphore",
                            prozesslaeufe
                        );
                }

                Task.Run(() => {
                    Log(
                        _processor,
                        $"Received Semaphore on {level} level!",
                        prozesslaeufe,
                        _debug
                    );
                }, token).Wait(token);

                processor.QueueManager.RemoveFromSemaphoreQueue(level, id, prozesslaeufe);
                addedToQueue = false;
            }
            catch (Exception e)
            {
                Task.Run(() => Log(
                    _processor,
                    $"Catched Error while getting semaphore! {e}",
                    prozesslaeufe,
                    _debug
                ), token).Wait(token);

                if (e is OperationCanceledException)
                {
                    Task.Run(() => Log(_processor, $"Catched cancled", prozesslaeufe, _debug), token).Wait(token);
                    if (addedToQueue)
                        processor.QueueManager.RemoveFromSemaphoreQueue(level, id, prozesslaeufe);

                    processor.DecreaseNumExecutingError(level, locked, module, prozesslaeufe);

                    switch (level)
                    {
                        case Level.Workflow:
                            ((Workflow)module).IncreasedNumExecWorkflows = false;
                            break;
                        case Level.Package:
                            ((Package)module).IncreasedNumExecPackages = false;
                            break;
                        case Level.Realization:
                            ((Realization)module).IncreasedNumExecRealizations = false;
                            break;
                        case Level.Step:
                            ((Step)module).IncreasedNumExecSteps = false;
                            break;
                        default:
                            throw new ETLException(
                                _processor,
                                $"There is no level {level} to set flag of counter!",
                                "GetSemaphore",
                                prozesslaeufe
                            );
                    }
                } else
                {
                    throw HandleErrorCatch(
                       _processor,
                       e,
                       "GetSemaphore",
                       $"Failed getting semaphore on level {level}!",
                       ref DummySem,
                       prozesslaeufe
                    );
                }
            }
        }

        // -------------------- FUNCTIONS TO RELEASE LOCK ----------------------

        /// <summary>
        /// sets the flag to lock process on given level to false and releases the semaphore on given level
        /// </summary>
        /// <param name="level">Level that shall be released</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">unknown level or other error</exception>
        public void FreeSemaphore(
            Level level,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                switch (level)
                {
                    case Level.Workflow:
                        _workflowSemaphore.Release();
                        break;
                    case Level.Package:
                        _packageSemaphore.Release();
                        break;
                    case Level.Realization:
                        _realizationSemaphore.Release();
                        break;
                    case Level.Step:
                        _stepSemaphore.Release();
                        break;
                    default:
                        throw new ETLException(
                            _processor,
                            $"There is no level {level} to free semaphore for!",
                            "FreeSemaphore",
                            prozesslaeufe
                        );
                }
                Task.Run(() =>
                {
                    Log(
                        _processor,
                        $"Freed semaphore on {level} level!",
                        prozesslaeufe,
                        _debug
                    );
                }).Wait();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                   _processor,
                   e,
                    "FreeSemaphore",
                    $"Failed to free semaphore on level {level}!",
                   ref DummySem,
                   prozesslaeufe
                );
            }
        }
    }
}
