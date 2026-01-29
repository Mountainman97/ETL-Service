using System.Data;
using System.Text;
using System.Data.Common;
using System.Text.RegularExpressions;

using GenericParsing;
using Excel = ClosedXML.Excel;
using Microsoft.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;

namespace DIZService.Core
{
    partial
        /// <summary>
        /// class that can execute SQL Tasks from and to different sources
        /// </summary>
        class CommandExecutor(Processor processor) : Helper
    {
        private readonly Processor _processor = processor;  // use to log locally

        private const int Batchsize = 500000;  // datasize to keep per processing cache

        /// <summary>
        /// simulates a task by running a given delay in seconds
        /// </summary>
        /// <param name="delay">the delay in seconds</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        public void RunDummy(int delay, Tuple<int?, int?, int?, int?> prozesslaeufe)
        {
            Task.Run(() => Log(_processor, $"Run Dummy (Delay of {delay} seconds)", prozesslaeufe)).Wait();
            Task.Delay(delay * 1000).Wait();
            Task.Run(() => Log(_processor, "Finished Dummy", prozesslaeufe)).Wait();
        }

        /// <summary>
        /// copies the data from a source table with a query to a destination table in timeslices of one month
        /// </summary>
        /// <param name="srcConnection">DbConnection to source</param>
        /// <param name="dstConnection">DbConnection to destination</param>
        /// <param name="confID">ETL_Konfigurationen_ID of realization defining connection</param>
        /// <param name="query">query to select data from source</param>
        /// <param name="dstTable">name of destination table in destination connection</param>
        /// <param name="srcType">name of connection type (e.g. SQL, Oracle, ...)</param>
        /// <param name="dstType">name of connection type (e.g. SQL, Oracle, ...)</param>
        /// <param name="dstUser">user of destination</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="debug">set to true to get debug output</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void CopyDataTimesliced(
            DbConnection srcConnection,
            DbConnection dstConnection,
            string confID,
            string query,
            string dstTable,
            string srcType,
            string dstType,
            string dstUser,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            bool debug = false
        )
        {
            // open connections to Server
            try
            {
                srcConnection.Open();
                dstConnection.Open();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyDataTimesliced",
                    "Opening Connections to source and destination",
                    locked: ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            // get the boundaries for takeover
            DateTime executeFrom;
            DateTime executeTo;
            try
            {
                executeFrom = workflow.GetTakeoverTime().Item1;
                executeTo = workflow.GetTakeoverTime().Item2;
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyDataTimesliced",
                    $"Receiving takeover times failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [srcConnection, dstConnection]
                );
            }

            // replace the common placeholder for this usecase
            query = query.Replace($"##Uebernahme_von##", "&&EXECUTE_FROM&&");
            query = query.Replace($"##Uebernahme_bis##", "&&EXECUTE_TO&&");

            // replace all other placeholders as common
            query = ReplacePlaceholder(_processor, workflow, query, prozesslaeufe, debug);

            // Read data from source
            // Data Table that will include the data for target table
            DataTable destinationTable = new($"sql_{dstTable}");

            // initialize dataadapter to destination (fill to get target column names)
            try
            {
                DbDataAdapter dstDataAdapter = DBHelper.GetDataAdapter(
                    _processor,
                    dstType,
                    dstConnection,
                    $"SELECT * FROM {dstTable}",
                    prozesslaeufe
                );

                dstDataAdapter.Fill(destinationTable);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyDataTimesliced",
                    $"Filling DataTable with data from {dstTable} failed (probably forgot truncation?)",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [srcConnection, dstConnection]
                );
            }

            // add a month for first slice
            DateTime nextMonth = executeFrom.AddMonths(1).AddDays(-1);

            // read sliced data and write it to DB
            try
            {
                int rowNumber = 0;
                int sliceCounter = 1;
                while (true)
                {
                    // create the query with the slice times
                    string sliceQuery = query.Replace("&&EXECUTE_FROM&&", executeFrom.ToString("yyyyMMdd000000"));
                    sliceQuery = sliceQuery.Replace("&&EXECUTE_TO&&", nextMonth.ToString("yyyyMMdd235959"));

                    // get number of lines in source table
                    Task.Run(() => Log(
                        _processor,
                        $"Get Data From {executeFrom:yyyyMMdd000000} to {nextMonth:yyyyMMdd235959} ({sliceQuery})",
                        prozesslaeufe
                    )).Wait();

                    // initialize dataadpater to source for slice
                    DbDataAdapter srcDataAdapter;
                    try
                    {
                        srcDataAdapter = DBHelper.GetDataAdapter(
                            _processor, srcType, srcConnection, sliceQuery, prozesslaeufe);
                    }
                    catch (Exception e)
                    {
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "CopyDataTimesliced",
                            $"Receiving the DataAdapter for Operation failed!",
                            ref DummySem,
                            prozesslaeufe,
                            workflow.GetCancelSource()
                        );
                    }

                    DateTime startTime = DateTime.Now;
                    if (debug)
                    {
                        Task.Run(() => Log(
                            _processor,
                            $"Start Reading and Writing Data for slice {sliceCounter} at {startTime}",
                            prozesslaeufe
                        )).Wait();
                    }

                    // fill dataset with batch size tables and write them to DB
                    try
                    {
                        int lineCounter = 0;
                        DataTable dataslice = new();

                        DateTime start = DateTime.Now;
                        Task.Run(() => LogQuery(_processor, sliceQuery, Convert.ToInt32(confID), prozesslaeufe)).Wait();
                        srcDataAdapter.Fill(dataslice);
                        if (debug)
                            Task.Run(() => Log(
                                _processor,
                                $"Finished Data Read for slice {sliceCounter}",
                                prozesslaeufe
                            )).Wait();
                        destinationTable.Clear();

                        Task.Run(() => WriteDataToTable(
                                dataslice,
                                destinationTable.Copy(),
                                lineCounter,
                                dstType,
                                dstConnection,
                                dstTable,
                                dstUser,
                                prozesslaeufe,
                                workflow,
                                startTime,
                                debug
                            ), workflow.GetCancelSource().Token).Wait();

                        DateTime end = DateTime.Now;

                        if (debug)
                        {
                            Task.Run(() => Log(
                                _processor,
                                $"Finished slice {sliceCounter} after {(end - start)} ({dataslice.Rows.Count})",
                                prozesslaeufe
                            )).Wait();
                        }

                        rowNumber += dataslice.Rows.Count;
                        sliceCounter++;
                    }
                    catch (Exception e)
                    {
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "CopyDataTimesliced",
                            $"({dstTable}) Reading Data failed!",
                            ref DummySem,
                            prozesslaeufe,
                            workflow.GetCancelSource()
                        );
                    }

                    // check if end of full data was reached
                    if (nextMonth == executeTo)
                        break;

                    // set dates for next slice
                    executeFrom = nextMonth.AddDays(1);
                    nextMonth = executeFrom.AddMonths(1).AddDays(-1);
                    // check if the full next month would exceed the transfer time
                    if (nextMonth > executeTo)
                        nextMonth = executeTo;
                }

                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketschritt_Prozesslaeufe",
                    prozesslaeufe.Item4 != null ? (int) prozesslaeufe.Item4 : 0,
                    "ETL_Paketschritt_Prozesslaeufe_ID",
                    [new Tuple<string, object>("ErwarteteDaten", rowNumber)],
                    prozesslaeufe
                )).Wait();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyDataTimesliced",
                    "Reading the sliced data and writing it to DB failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [srcConnection, dstConnection]
                );
            }

            // close connections to Server
            try
            {
                srcConnection.Close();
                dstConnection.Close();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyDataTimesliced",
                    "Closing Connections to source and destination failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// transfers data from a given source connection to a destination connection based on a query to receive
        /// data from source and insert data into dst_table
        /// </summary>
        /// <param name="srcConnection">DbConnection to source</param>
        /// <param name="dstConnection">DbConnection to destination</param>
        /// <param name="confID">ETL_Konfigurationen_ID of realization defining connection</param>
        /// <param name="query">query to select data from source</param>
        /// <param name="dstTable">name of destination table in destination connection</param>
        /// <param name="srcType">name of connection type (e.g. SQL, Oracle, ...)</param>
        /// <param name="dstType">name of connection type (e.g. SQL, Oracle, ...)</param>
        /// <param name="dstUser">user of destination</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="debug">set to true to get debug output</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void CopyData(
            DbConnection srcConnection,
            DbConnection dstConnection,
            string confID,
            string query,
            string dstTable,
            string srcType,
            string dstType,
            string dstUser,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            bool debug = false
        )
        {
            // open connections to Server
            try
            {
                srcConnection.Open();
                dstConnection.Open();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyData",
                    $"Opening Connections to source and destination failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            query = ReplacePlaceholder(_processor, workflow, query, prozesslaeufe, debug);

            // get number of lines in source table
            int numRows;
            try
            {
                // replace placeholder in command
                DbCommand command = DBHelper.GetCommand(
                    _processor,
                    srcType,
                    srcConnection,
                    $"SELECT COUNT(*) FROM ({query}) tabelle",
                    prozesslaeufe
                );

                command.CommandTimeout = 0;
                numRows = int.Parse(command.ExecuteScalar()?.ToString() ?? "0");
                Task.Run(() => Log(_processor, $"Rows to Read: {numRows}", prozesslaeufe)).Wait();

                Task.Run(() => UpdateLog(
                    _processor,
                    "Logging.ETL_Paketschritt_Prozesslaeufe",
                    prozesslaeufe.Item4 != null ? (int)prozesslaeufe.Item4 : 0,
                    "ETL_Paketschritt_Prozesslaeufe_ID",
                    [new Tuple<string, object>("ErwarteteDaten", numRows)],
                    prozesslaeufe
                )).Wait();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyData",
                    $"Receiving number of lines to read failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [srcConnection, dstConnection]
                );
            }

            // calculate the number of batches needed
            int rest = numRows % Batchsize;
            int batches = (numRows - rest) / Batchsize;

            // Read data from source
            // Data Table that will include the data for target table
            DataTable destinationTable = new($"sql_{dstTable}");

            // initialize dataadpater to source
            DbDataAdapter srcDataAdapter;
            try
            {
                srcDataAdapter = DBHelper.GetDataAdapter(_processor, srcType, srcConnection, query, prozesslaeufe);
                (srcDataAdapter.SelectCommand ?? throw new ETLException("Source has no Command")).CommandTimeout = 0;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyData",
                    $"Receiving the DataAdapter for Operation failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [srcConnection, dstConnection]
                );
            }

            // initialize dataadapter to destination (fill to get target column names)
            try
            {
                DbDataAdapter dstDataAdapter = DBHelper.GetDataAdapter(
                    _processor,
                    dstType,
                    dstConnection,
                    $"SELECT * FROM {dstTable}",
                    prozesslaeufe
                );
                (dstDataAdapter.SelectCommand ?? 
                        throw new ETLException("Destination has no Command")).CommandTimeout = 0;
                dstDataAdapter.Fill(destinationTable);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyData",
                    $"Filling DataTable with data from {dstTable} failed (probably forgot truncation?)",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [srcConnection, dstConnection]
                );
            }

            DateTime startTime = DateTime.Now;
            if (debug)
                Task.Run(() => Log(_processor, $"Start Reading and Writing Data at {startTime}", prozesslaeufe)).Wait();

            int lineCounter = 0;    // variable to count read lines

            // fill dataset with batch size tables and write them to DB
            try
            {
                DataSet ds = new();
                for (int i = 0; i < batches + 1; i++)
                {
                    DateTime start = DateTime.Now;

                    // add a new table for batch to dataset
                    ds.Tables.Add($"{dstTable}_{(i + 1)}");

                    Task.Run(() => LogQuery(
                        _processor,
                        query,
                        Convert.ToInt32(confID),
                        prozesslaeufe
                    )).Wait();
                    // fill the datasource table with batch data
                    srcDataAdapter.Fill(ds, Batchsize * i, Batchsize, $"{dstTable}_{(i + 1)}");
                    if (debug)
                        Task.Run(() => Log(_processor, $"Finished Read for batch {(i + 1)}", prozesslaeufe)).Wait();

                    // write data into destination
                    destinationTable.Clear();
                    Task.Run(() => WriteDataToTable(
                        ds.Tables[$"{dstTable}_{(i + 1)}"] ?? throw new ETLException("No destination table"),
                        destinationTable.Copy(),
                        lineCounter,
                        dstType,
                        dstConnection,
                        dstTable,
                        dstUser,
                        prozesslaeufe,
                        workflow,
                        startTime,
                        debug
                    ), workflow.GetCancelSource().Token).Wait();

                    DateTime end = DateTime.Now;

                    if (debug)
                        Task.Run(() => Log(
                            _processor,
                            $"Finished batch {(i + 1)}/{(batches + 1)} after {(end - start)} ({lineCounter})",
                            prozesslaeufe
                        )).Wait();

                    // remove datatables to save memory
                    ds.Tables.Remove(
                        ds.Tables[$"{dstTable}_{(i + 1)}"] ?? throw new ETLException("No destination table"));
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyData",
                    $"({dstTable}) Reading Data failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [srcConnection, dstConnection]
                );
            }

            // close connections to Server
            try
            {
                srcConnection.Close();
                dstConnection.Close();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CopyData",
                    "Closing Connections to source and destination failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// returns the given range from a given DataTable
        /// </summary>
        /// <param name="sourceTable">DataTable containing full data</param>
        /// <param name="start">first row</param>
        /// <param name="end">last row</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <returns>DataTable containing given range rows</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DataTable GetDataTableLines(
            DataTable sourceTable,
            int start,
            int end,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            try
            {
                DataTable resultTable = sourceTable.Clone();

                // Copy rows to the new DataTable
                for (int i = start; i < Math.Min(end, sourceTable.Rows.Count); i++)
                {
                    resultTable.ImportRow(sourceTable.Rows[i]);
                }

                return resultTable;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetDataTableLines",
                    $"Extracting 50000 lines failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// adds needed columns for destination table not existing in source table and renames source columns
        /// </summary>
        /// <param name="destinationTable">DataTable containing Columns of destination</param>
        /// <param name="sourceTable">Source Table to add columns to and rename</param>
        /// <param name="dstUser">User that transfers the data</param>
        /// <param name="type">type of connection</param>
        /// <param name="connection">connection to get DB user</param>
        /// <param name="workflowStart">Date Time of workflow start if exists</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <returns>DataTable with renamed columns and added columns</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DataTable AddNeededColumns(
            DataTable destinationTable,
            DataTable sourceTable,
            string dstUser,
            string type,
            DbConnection connection,
            DateTime? workflowStart,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            try
            {
                foreach (DataColumn column in destinationTable.Columns)
                {
                    if (!sourceTable.Columns.Contains(column.ColumnName))
                    {
                        DataColumn newCol = new(column.ColumnName, column.DataType);
                        object? value;

                        switch (column.ColumnName)
                        {
                            case "Nutzer":
                                value = dstUser;
                                break;
                            case "Abfragezeitpunkt":
                                value = workflowStart != null ? workflowStart.ToString() : DateTime.Now.ToString();
                                break;
                            case "Datenproduzent":
                                try
                                {
                                    var user = DBHelper.GetCommand(
                                        _processor,
                                        type,
                                        connection,
                                        "SELECT SUSER_NAME();",
                                        prozesslaeufe
                                    ).ExecuteScalar();
                                    value = user;
                                }
                                catch
                                {
                                    value = "GET USER FAILED";
                                }
                                break;
                            default:
                                value = DBNull.Value;
                                break;
                        }

                        newCol.DefaultValue = value;
                        sourceTable.Columns.Add(newCol);
                    }
                    else
                    {
                        sourceTable.Columns[
                            sourceTable.Columns.IndexOf(column.ColumnName)
                        ].ColumnName = column.ColumnName;
                    }
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "AddNeededColumns",
                    $"Adding a column with default value or renaming column in source table failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            return sourceTable;
        }

        /// <summary>
        /// Writes the data given by sourceTable into the destination DB using the bulkCopy in batches
        /// </summary>
        /// <param name="sourceTable">DataTable containing data from source</param>
        /// <param name="destinationTable">DataTable for structure of destination</param>
        /// <param name="lineCounter">number of lines read till now</param>
        /// <param name="type">connection type of destination</param>
        /// <param name="connection">DbConnection to destination</param>
        /// <param name="dstTable">name of the destination table in destination DB</param>
        /// <param name="dstUser">user to use in destination DB</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="workflowStart">DateTime of when the workflow started</param>
        /// <param name="debug">set to true to get debug output</param>
        /// <returns>read/transfered line number after transfer (returns -1 when error occurs)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private void WriteDataToTable(
            DataTable sourceTable,
            DataTable destinationTable,
            int lineCounter,
            string type,
            DbConnection connection,
            string dstTable,
            string dstUser,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            DateTime? workflowStart = null,
            bool debug = false
        )
        {
            try
            {
                destinationTable.BeginLoadData();
                DataRowCollection datarows = destinationTable.Rows;
                int batchLinecounter = 0;

                // add Columns to source table that are needed for destination table and
                // rename the existing ones to destination version
                sourceTable = AddNeededColumns(
                    destinationTable,
                    sourceTable,
                    dstUser,
                    type,
                    connection,
                    workflowStart,
                    prozesslaeufe,
                    workflow
                );

                // write 50k rows at once in batches of this batch to DB
                try
                {
                    int lines = 0;
                    int counter = 0;
                    while (lines < sourceTable.Rows.Count)
                    {
                        if (debug)
                            Task.Run(() => Log(
                                _processor,
                                $"Get Lines {counter * 50000}-{(counter + 1) * 50000} from {sourceTable.Rows.Count} " +
                                "Rows of batch to write to DB",
                                prozesslaeufe
                            )).Wait();

                        DataTable batchDT = GetDataTableLines(
                            sourceTable, counter * 50000, (counter + 1) * 50000, prozesslaeufe, workflow);
                        Task.Run(() => WriteToServer(
                            batchDT,
                            type,
                            connection,
                            dstTable,
                            prozesslaeufe,
                            workflow,
                            debug: debug
                        ), workflow.GetCancelSource().Token).Wait();
                        counter++;
                        lines += 50000;
                    }
                }
                catch (Exception e)
                {
                    throw HandleErrorCatch(
                        _processor,
                        e,
                        "WriteDataToTable",
                        $"({dstTable}) Writing data to server failed!",
                        ref DummySem,
                        prozesslaeufe,
                        workflow.GetCancelSource()
                    );
                }
                lineCounter += batchLinecounter;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "WriteDataToTable",
                    $"({dstTable}) Writing data into target table failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// Opens the appropriate BulkCopy and writes the given Data into the DB
        /// throws an ETLException in case of any error
        /// </summary>
        /// <param name="data">DataTable containing data from source to insert/write</param>
        /// <param name="type">connection type of destination</param>
        /// <param name="connection">DbConnection to destination</param>
        /// <param name="dstTable">name of the destination table in destination DB</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="identity">Data ID that started this process and therefore will be noted in case of
        /// errors(optional)</param>
        /// <param name="debug">set to true to get debug output</param>
        /// <exception cref="ETLException">in case pf any error</exception>
        /// <exception cref="NYIException">in case of not implemented case</exception>
        private void WriteToServer(
            DataTable data,
            string type,
            DbConnection connection,
            string dstTable,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            string identity = "",
            bool debug = false
        )
        {
            try
            {
                DataRow lastRow = data.Rows[^1];
                if (debug)
                    Task.Run(() => Log(
                        _processor,
                        $"ID of first Row: {data.Rows[0][0]} | ID of last Row: {lastRow[0]}",
                        prozesslaeufe
                    )).Wait();

                switch (type)
                {
                    case "Access":
                        throw new NYIException(
                            _processor,
                            "Writing data for Access!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "Cache":
                        throw new NYIException(
                            _processor,
                            "Writing data for Cache!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "CSV":
                        throw new NYIException(
                            _processor,
                            "Writing data for CSV!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "DB2":
                        throw new NYIException(
                            _processor,
                            "Writing data for IBM!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "Excel":
                        throw new NYIException(
                            _processor,
                            "Writing data for Excel!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "Firebird":
                        throw new NYIException(
                            _processor,
                            "Writing data for Embarcadero InterBase!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "HL7":
                        throw new NYIException(
                            _processor,
                            "Writing data for Health Level 7!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "Infomix":
                        throw new NYIException(
                            _processor,
                            "Writing data for Infomix!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "JSON":
                        throw new NYIException(
                            _processor,
                            "Writing data for JavaScript Object Notation!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "MS-SQL Server":
                        if (debug)
                            Task.Run(() => Log(_processor, "Writing data for MS-SQL Server!", prozesslaeufe)).Wait();

                        using (SqlBulkCopy msSqlBulkCopy = new((SqlConnection)connection))
                        {
                            msSqlBulkCopy.DestinationTableName = dstTable;
                            foreach (DataColumn column in data.Columns)
                            {
                                msSqlBulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                            }

                            msSqlBulkCopy.BulkCopyTimeout = 0;
                            msSqlBulkCopy.WriteToServer(data);
                        }
                        break;
                    case "MySQL":
                        throw new NYIException(
                            _processor,
                            "Writing data for MySQL!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "Oracle":
                        if (debug)
                            Task.Run(() => Log(
                                _processor,
                                $"Writing data for Oracle! ({connection.State})",
                                prozesslaeufe
                            )).Wait();

                        using (OracleBulkCopy oracleBulkCopy = new((OracleConnection)connection))
                        {
                            oracleBulkCopy.DestinationTableName = dstTable;
                            foreach (DataColumn column in data.Columns)
                            {
                                oracleBulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                            }

                            oracleBulkCopy.BulkCopyTimeout = 0;
                            oracleBulkCopy.WriteToServer(data);
                        }
                        break;
                    case "Postgres":
                        throw new NYIException(
                            _processor,
                            "Writing data for PostgreSQL!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "SAP":
                        throw new NYIException(
                            _processor,
                            "Writing data for SAPRFC!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "WebRequest":
                        throw new NYIException(
                            _processor,
                            "Writing data for Webservice!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    case "XML":
                        throw new NYIException(
                            _processor,
                            "Writing data for Extensible Makup Language!",
                            "WriteToServer",
                            prozesslaeufe
                        ); // TODO
                    default:
                        throw new ETLException(
                            _processor,
                            $"Unkown Type for writing data ({type})",
                            "WriteToServer",
                            identity,
                            null,
                            prozesslaeufe
                        );
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "WriteToServer",
                    $"Writing data for {type} failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// executes the given SQL command (e.g., a truncation) on a given connection
        /// throws an ETLException in case of any error
        /// </summary>
        /// <param name="type">connection type of destination</param>
        /// <param name="connection">connection to destination DB</param>
        /// <param name="confID">ETL_Konfigurationen_ID of realization defining connection</param>
        /// <param name="command">command to execute</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="debug">set to true to get debug output</param>
        /// <exception cref="ETLException">in case of any error</exception>
        /// <exception cref="NYIException">in case of not implemented case</exception>
        public void ExecuteCommand(
            string type,
            DbConnection connection,
            string confID,
            string command,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            bool debug = false
        )
        {
            try
            {
                connection.Open();
                switch (type)
                {
                    case "MS-SQL Server":
                        if (debug)
                            Task.Run(() => Log(
                                _processor, $"Execute Command {command} ({type})", prozesslaeufe)).Wait();

                        SqlCommand sqlCommand = new(command, (SqlConnection)connection)
                        {
                            CommandTimeout = 0
                        };
                        // create a possible output parameter
                        SqlParameter outputParam = new()
                        {
                            ParameterName = "@OutputParam1",
                            SqlDbType = SqlDbType.Int,
                            Direction = ParameterDirection.Output,
                            Value = null
                        };
                        sqlCommand.Parameters.Add(outputParam);

                        Task.Run(() => LogQuery(_processor, command, Convert.ToInt32(confID), prozesslaeufe)).Wait();
                        sqlCommand.ExecuteNonQuery();

                        // check output parameter and throw error if needed
                        if (outputParam.Value != null &&
                            outputParam.Value != DBNull.Value &&
                            !string.IsNullOrEmpty(outputParam.Value.ToString()))
                        {
                            if (((int) outputParam.Value) == -1)
                                throw new ETLException();
                        }

                        break;
                    case "Oracle":
                        if (debug)
                            Task.Run(() => Log(
                                _processor, $"Execute Command {command} ({type})", prozesslaeufe)).Wait();

                        OracleCommand oracleCommand = new(command, (OracleConnection)connection);
                        oracleCommand.ExecuteNonQuery();
                        break;
                    default:
                        throw new NYIException(
                            _processor,
                            $"Executing command for {type} (NYI)",
                            "ExecuteCommand",
                            prozesslaeufe
                        );
                }
                connection.Close();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ExecuteCommand",
                    $"Executing {type} Command failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [connection]
                );
            }
        }

        /// <summary>
        /// Move a given file from one directory to another. Returns -1 if an error occurs
        /// </summary>
        /// <param name="file">name of file to transfer</param>
        /// <param name="srcDir">path to directory where file is initialy</param>
        /// <param name="dstDir">path to directory to move file to</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <exception cref="ETLException">n case of any error</exception>
        private void TransferFile(
            string file,
            string srcDir,
            string dstDir,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            try
            {
                if (Directory.Exists(srcDir) && Directory.Exists(dstDir) && File.Exists(srcDir + file))
                {
                    File.Move(srcDir + file, dstDir + file);
                    Task.Run(() => Log(
                        _processor,
                        $"Transfered File from {srcDir} to {dstDir}",
                        prozesslaeufe
                    )).Wait();
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransferFile",
                    "Moving File Failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// Remove all data from given Table in DB matching given filename
        /// </summary>
        /// <param name="file">name of file to delete data for</param>
        /// <param name="targetTable">DB table to insert data into</param>
        /// <param name="connection">Connection to DB to insert Data into</param>
        /// <param name="type">the type of connection used to get correct command etc.</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <exception cref="ETLException">in case of any error</exception>
        private void RemoveFileDataFromDB(
            string file,
            string targetTable,
            DbConnection connection,
            string type,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            string deleteCommand = $"DELETE FROM {targetTable} WHERE Dateiname = '{file}';";

            try
            {
                DBHelper.GetCommand(_processor, type, connection, deleteCommand, prozesslaeufe).ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "RemoveFileDataFromDB",
                    "Removing File Data from DB failed",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// Use after transfering xlsx file from one directory to another to reset old structure and move files back
        /// </summary>
        /// <param name="directory">path to directory that includes the directories Insert and TMP</param>
        /// <param name="targetTable">DB table to insert data into</param>
        /// <param name="connection">Connection to DB to insert Data into</param>
        /// <param name="type">the type of connection used to get correct command etc.</param>
        /// <param name="errorFile">name of file to write error log to</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <exception cref="ETLException">if transfering files fails</exception>
        private void ResetFile(
            string directory,
            string targetTable,
            DbConnection connection,
            string type,
            string errorFile,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            try
            {
                // transfer unsuccessfull files back to insert directory
                if (Directory.Exists(directory + @"\TMP\"))
                {
                    RemoveFileDataFromDB(
                        errorFile,
                        targetTable,
                        connection,
                        type,
                        prozesslaeufe,
                        workflow
                    );

                    TransferFile(
                        errorFile,
                        directory + @"\TMP\",
                        directory + @"\Insert\",
                        prozesslaeufe,
                        workflow
                    );
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ResetFile",
                    "Reseting location of file and DB data failed",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// Use after transfering xlsx file from one directory to another to reset old structure and move files back
        /// </summary>
        /// <param name="directory">path to directory that includes the directories Insert and TMP</param>
        /// <param name="targetTable">DB table to insert data into</param>
        /// <param name="connection">Connection to DB to insert Data into</param>
        /// <param name="type">the type of connection used to get correct command etc.</param>
        /// <param name="successfullFiles">list of filenames that were successfully transfered</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <exception cref="ETLException">if transfering files fails</exception>
        private void ResetFiles(
            string directory,
            string targetTable,
            DbConnection connection,
            string type,
            List<string> successfullFiles,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            try
            {
                // transfer unsuccessfull files back to insert directory
                if (Directory.Exists(directory + @"\TMP\"))
                {
                    foreach (string file in Directory.EnumerateFiles(directory + @"\TMP\", "*.xlsx"))
                    {
                        // reset filestructure and remove DB entries for files that are not successfully transfered
                        if (!successfullFiles.Contains(file.Split('\\').Last()))
                        {
                            RemoveFileDataFromDB(
                                file.Split('\\').Last(),
                                targetTable,
                                connection,
                                type,
                                prozesslaeufe,
                                workflow
                            );

                            TransferFile(
                                file.Split('\\').Last(),
                                directory + @"\TMP\",
                                directory + @"\Insert\",
                                prozesslaeufe,
                                workflow
                            );
                        }
                    }
                }

                // delete the successfully transfered files
                RemoveTransferedFiles(directory, successfullFiles, prozesslaeufe, workflow);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ResetFiles",
                    "Reseting filestructure and DB data failed",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// Deletes all files (filenames) from a given directory
        /// </summary>
        /// <param name="directory">path to directory that includes the Excle files to transfer</param>
        /// <param name="successfullFiles">list of filenames that were successfully transfered</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <exception cref="ETLException">thrown in case of error when deleting a file</exception>
        private void RemoveTransferedFiles(
            string directory,
            List<string> successfullFiles,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            try
            {
                foreach (string file in successfullFiles)
                {
                    Task.Run(() => Log(_processor, $"Remove {file}", prozesslaeufe )).Wait();
                    File.Delete(directory + @"\TMP\" + file);
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "RemoveTransferedFiles",
                    "Removing file from TMP directory failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// reads all Excel files from a given directory and inserts the data into a given table
        /// throws an ETLException in case of any errors
        /// </summary>
        /// <param name="directory">path to directory that includes the Excle files to transfer</param>
        /// <param name="confID">ETL_Konfigurationen_ID of realization defining connection</param>
        /// <param name="targetTable">DB table to insert data into</param>
        /// <param name="connection">Connection to DB to insert Data into</param>
        /// <param name="type">the type of connection used to get correct command etc.</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="debug">set to true to get debug output</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void TransferDataFromExcelToDB(
            string directory,
            string targetTable,
            DbConnection connection,
            string type,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            bool debug = false
        )
        {
            // open connection to DB
            try
            {
                connection.Open();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransferDataFromExcelToDB",
                    "Opening Connection to Target DB failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            if (debug)
                Task.Run(() => Log(_processor, "Reading Column Mapping", prozesslaeufe)).Wait();

            // read the column mapping from CSV
            DataTable columnMapping;
            try
            {
                columnMapping = ReadCSV(directory + @"\Insert\mapping.csv", prozesslaeufe, workflow);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransferDataFromExcelToDB",
                    "Reading the column mapping failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [connection]
                );
            }

            List<string> successfullFileTransfers = [];  // tracks the successfull transfered files

            // get list of files in directory
            List<string> filesToTransfer;
            try
            {
                filesToTransfer = GetFilesOfDirectory(directory, "xlsx", prozesslaeufe, workflow);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransferDataFromExcelToDB",
                    "Reading the column mapping failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [connection]
                );
            }

            Task.Run(() => {
                Log(_processor, $"{filesToTransfer.Count} Excel-Files to transfer to DB!", prozesslaeufe);
            }).Wait();

            // transfer data of each file into DB in batches
            try
            {
                foreach (string file in filesToTransfer)
                {
                    string rawFile;
                    try
                    {
                        rawFile = file.Split('\\').Last();
                    }
                    catch (Exception e)
                    {
                        Task.Run(() => ErrorLog(
                            _processor,
                            "Dienst",
                            "Separating Filename failed!",
                            "minor",
                            e,
                            "TransferDataFromExcelToDB",
                            prozesslaeufe
                        )).Wait();
                        continue;
                    }

                    Task.Run(() => Log(_processor, $"Transfer data for file {rawFile}", prozesslaeufe)).Wait();

                    // transfer file and write it to DB
                    try
                    {
                        // transfer file to temporary directory
                        TransferFile(
                            rawFile,
                            directory + @"\Insert\",
                            directory + @"\TMP\",
                            prozesslaeufe,
                            workflow
                        );

                        // check if file already in DB
                        //bool skip = CheckFileExistenceInDB(
                        //    type,
                        //    connection,
                        //    file,
                        //    rawFile,
                        //    targetTable,
                        //    ref successfullFileTransfers,
                        //    prozesslaeufe,
                        //    workflow,
                        //    debug
                        //);
                        //if (skip)
                        //{
                        //    continue;
                        //}

                        // transfer data of file to DB
                        ReadExceldataToDB(
                            type,
                            connection,
                            directory,
                            file,
                            rawFile,
                            targetTable,
                            columnMapping,
                            prozesslaeufe,
                            workflow,
                            debug
                        );
                    }
                    catch (Exception e)
                    {
                        try
                        {
                            ResetFile(directory, targetTable, connection, type, rawFile, prozesslaeufe, workflow);
                        } catch (Exception ex)
                        {
                            workflow.GetCancelSource().Cancel();
                            Task.Run(() => ErrorLog(
                                _processor,
                                "Dienst",
                                $"Resetting File location failed! Reading and Writing File ({file}) to DB failed!",
                                "major",
                                ex,
                                "TransferDataFromExcelToDB",
                                prozesslaeufe
                            )).Wait();
                            SafeExit(
                                _processor,
                                ex,
                                $"TransferDataFromExcelToDB (Workflow_ID: {workflow.GetID()})",
                                prozesslaeufe
                            );
                        }
                        throw HandleErrorCatch(
                            _processor,
                            e,
                            "TransferDataFromExcelToDB",
                            "Transfering file and writing it to DB failed!",
                            ref DummySem,
                            prozesslaeufe,
                            workflow.GetCancelSource()
                        );
                    }

                    Task.Run(() => Log(_processor, $"Data for file {file} transfered", prozesslaeufe)).Wait();
                    successfullFileTransfers.Add(rawFile);
                }
            }
            catch (Exception e)
            {
                try
                {
                    ResetFiles(
                        directory,
                        targetTable,
                        connection,
                        type,
                        successfullFileTransfers,
                        prozesslaeufe,
                        workflow
                    );
                }
                catch (Exception ex)
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    workflow.GetCancelSource().Cancel();
                    Task.Run(() => ErrorLog(
                        _processor,
                        "",
                        "Resetting File location failed!",
                        "major",
                        ex,
                        "TransferDataFromExcelToDB",
                        prozesslaeufe
                    )).Wait();
                    SafeExit(
                        _processor,
                        e,
                        $"TransferDataFromExcelToDB (Workflow_ID: {workflow.GetID()})",
                        prozesslaeufe
                    );
                }

                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransferDataFromExcelToDB",
                    "Transfering Excel data Failed! Reset File structure for next try!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [connection ?? throw new ETLException("No connection")]
                );
            }

            // remove all files that were successfully transfered
            try
            {
                DeleteFilesInDirectory(directory, "xlsx", prozesslaeufe, workflow);
            }
            catch (Exception e)
            {
                try
                {
                    ResetFiles(
                        directory,
                        targetTable,
                        connection,
                        type,
                        successfullFileTransfers,
                        prozesslaeufe,
                        workflow
                    );
                }
                catch (Exception ex)
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    workflow.GetCancelSource().Cancel();
                    Task.Run(() => ErrorLog(
                        _processor,
                        "",
                        "Resetting File location failed!",
                        "major",
                        ex,
                        "TransferDataFromExcelToDB",
                        prozesslaeufe
                    )).Wait();
                    SafeExit(
                        _processor,
                        e,
                        $"TransferDataFromExcelToDB (Workflow_ID: {workflow.GetID()})",
                        prozesslaeufe
                    );
                }

                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransferDataFromExcelToDB",
                    "Removing transferred files failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource(),
                    [connection ?? throw new ETLException("No connection")]
                );
            }

            // close the connection to DB
            try
            {
                connection.Close();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransferDataFromExcelToDB",
                    "Closing the connection to DB failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// reads all files of a given kind in a given directory and returns them as a list of string.
        /// Directory needs to include a subdirectory with name Insert from where the files are read
        /// </summary>
        /// <param name="directory">path to directory that includes the Excle files to transfer</param>
        /// <param name="fileEnd">fileending (e.g. xlsx)</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <returns>list of files (paths)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private List<string> GetFilesOfDirectory(
            string directory,
            string fileEnd,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            List<string> filesToTransfer = [];
            try
            {
                foreach (string file in Directory.EnumerateFiles(directory + @"\Insert\", $"*.{fileEnd}"))
                {
                    filesToTransfer.Add(file);
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "GetFilesOfDirectory",
                    "Extracting Files to insert into DB failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            return filesToTransfer;
        }

        /// <summary>
        /// tries to delete all files of given type in given directory and aborts in case of any error
        /// </summary>
        /// <param name="directory">path to directory that includes the Excle files to delete</param>
        /// <param name="fileEnd">fileending (e.g. xlsx)</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <exception cref="ETLException">in case of any error</exception>
        private void DeleteFilesInDirectory(
            string directory,
            string fileEnd,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            try
            {
                if (Directory.Exists(directory + @"\TMP\"))
                {
                    foreach (string file in Directory.EnumerateFiles(directory + @"\TMP\", $"*.{fileEnd}"))
                    {
                        File.Delete(file);
                    }
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "DeleteFilesInDirectory",
                    $"Deleting files in \"{directory}\\TMP\\\" failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// checks if a given file was already transfered to DB earlier and signalizes a skip for this file
        /// </summary>
        /// <param name="type">the type of connection used to get correct command etc.</param>
        /// <param name="connection">Connection to DB to insert Data into</param>
        /// <param name="file">full path to excel file to transfer</param>
        /// <param name="rawFile">filename of excel file to transfer</param>
        /// <param name="targetTable">name of target table to insert data into</param>
        /// <param name="successfullFileTransfers">list of files that were already successfully transfered</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="debug">true to get debug output or false for no output</param>
        /// <returns>true if all additional steps can be skipped</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private bool CheckFileExistenceInDB(
            string type,
            DbConnection connection,
            string file,
            string rawFile,
            string targetTable,
            ref List<string> successfullFileTransfers,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            bool debug = false
        )
        {
            try
            {
                int numEntries = int.Parse(DBHelper.GetCommand(
                    _processor,
                    type,
                    connection,
                    $"SELECT COUNT(*) FROM {targetTable} WHERE Dateiname = '{rawFile}'",
                    prozesslaeufe
                ).ExecuteScalar()?.ToString() ?? "0");

                if (debug)
                    Task.Run(() => Log(
                        _processor,
                        $"Number of entries for file {file} in DB: {numEntries}",
                        prozesslaeufe
                    )).Wait();

                if (numEntries > 0)
                {
                    successfullFileTransfers.Add(file);
                    Task.Run(() => Log(_processor, $"Skip file {file}", prozesslaeufe)).Wait();
                    return true;
                }

                return false;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "CheckFileExistenceInDB",
                    $"Checking if file is already in DB failed! ({file})",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// reads the data from given excel file and writes it to DB
        /// </summary>
        /// <param name="type">the type of connection used to get correct command etc.</param>
        /// <param name="connection">Connection to DB to insert Data into</param>
        /// <param name="confID">ETL_Konfigurationen_ID of realization defining connection</param>
        /// <param name="directory">path to directory that includes the Excle files to transfer</param>
        /// <param name="file">full path to excel file to transfer</param>
        /// <param name="rawFile">filename of excel file to transfer</param>
        /// <param name="targetTable">name of target table to insert data into</param>
        /// <param name="columnMapping">mapping of excel columnnames to DB columnnames</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="debug">true to get debug output or false for no output</param>
        /// <exception cref="ETLException">in case of any error</exception>
        private void ReadExceldataToDB(
            string type,
            DbConnection connection,
            string directory,
            string file,
            string rawFile,
            string targetTable,
            DataTable columnMapping,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            bool debug = false
        )
        {
            try
            {
                int batchCounter = 0;
                DataTable data;
                do
                {
                    // read structure of target table from SQL Server
                    DataTable dstTable = new();

                    SqlDataAdapter sqlda = new(
                        $"SELECT * FROM {targetTable}", (SqlConnection)connection);
                    sqlda.Fill(dstTable);
                    dstTable.Clear();

                    if (debug)
                        Task.Run(() => Log(_processor, "Read Excel Data", prozesslaeufe)).Wait();

                    // read batch from excel table
                    data = ReadExcelTable(
                        directory + @"\TMP\" + rawFile,
                        [.. columnMapping.AsEnumerable().Select(x => x[0].ToString() ?? string.Empty)],
                        [.. columnMapping.AsEnumerable().Select(x => x[1].ToString() ?? string.Empty)],
                        dstTable,
                        prozesslaeufe,
                        workflow,
                        batchCounter * Batchsize,
                        (batchCounter + 1) * Batchsize,
                        debug
                    );

                    if (data == null)
                        throw new ETLException(
                            _processor,
                            "Reading Excel Table return no data",
                            "TransferDataFromExcelToDB",
                            prozesslaeufe
                        );

                    // rename columns of data to needed names
                    foreach (DataColumn col in data.Columns)
                    {
                        DataRow[] selection = columnMapping.Select($"source_column = '{col.ColumnName}'");
                        try
                        {
                            object? value = selection.First().ItemArray[1];
                            string columnName = value != null ? value.ToString()! : string.Empty;
                            col.ColumnName = columnName;
                        }
                        catch
                        {
                            continue;
                        }
                    }

                    Task.Run(() => Log(
                        _processor, "Write Excel Data to SQL Server", prozesslaeufe, debug)).Wait();

                    if (data.Rows.Count == 0)
                    {
                        Task.Run(() => Log(_processor, "No Data to write!", prozesslaeufe)).Wait();
                    } else
                    {
                        // write batch into DB
                        Task.Run(() => Log(
                            _processor, $"Write {data.Rows.Count} lines to Server!", prozesslaeufe)).Wait();
                        WriteToServer(data, type, connection, targetTable, prozesslaeufe, workflow, file, debug);
                    }
                    // go to next batch
                    batchCounter++;
                } while (data.Rows.Count > 0);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadExceldataToDB",
                    $"Transferring data for Excel File ({rawFile}) failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        /// <summary>
        /// read a given number of lines from an excel file and saves them into a DataTable
        /// </summary>
        /// <param name="file">path to excel file to read data from</param>
        /// <param name="oldColumns">list of column names in excel file</param>
        /// <param name="newColumns">list of column names in target table (for renaming)</param>
        /// <param name="vorlage">DataTable that represents the structure of DB Table to insert data into</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <param name="start">start row to read</param>
        /// <param name="end">last row to read</param>
        /// <param name="debug">set to true to get debug output (default: false)</param>
        /// <returns>DataTable including number of lines from excel file (can be null in case of errors)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DataTable ReadExcelTable(
            string file,
            List<string> oldColumns,
            List<string> newColumns,
            DataTable vorlage,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow,
            int start = 0,
            int end = 100,
            bool debug = false
        )
        {
            if (debug)
                Task.Run(() => Log(_processor, $"Read Data from {file}", prozesslaeufe)).Wait();

            DataSet sheets = new();  // includes a DataTable for each sheet in Excel Table
            try
            {
                using Excel.XLWorkbook book = new(file);
                // extract data of each sheet in excel file
                Excel.IXLWorksheet sheet = book.Worksheets.First();

                DataTable table = new();  // table for one sheer
                table = vorlage.Copy();

                if (sheet.IsEmpty())  // check if sheet is empty and skip it
                {
                    Task.Run(() => ErrorLog(
                        _processor,
                        "Workflow",
                        "The Input-Excel-File is empty!",
                        "minor",
                        null,
                        "ReadExcelTable",
                        prozesslaeufe
                    )).Wait();
                    return table;
                }

                // read the column names of the excel file (only those needed)
                List<string> headers = [];
                sheet.FirstRowUsed()?.CellsUsed().ToList().ForEach(x =>
                {
                    headers.Add(x.Value.ToString());
                });
                int rowNumber = 0;

                // read the data rows
                foreach (Excel.IXLRow row in sheet.RowsUsed().Skip(1 + start))
                {
                    DataRow newRow = table.NewRow();

                    foreach (string column in oldColumns)
                    {
                        int indexRow = headers.IndexOf(column);
                        int indexNew = oldColumns.IndexOf(column);
                        newRow[newColumns[indexNew]] = row.Cell(indexRow + 1).Value.ToString();
                    }

                    // TODO: automatisch?
                    newRow["Dateiname"] = file.Split('\\').Last();
                    newRow["Exportdatum"] = DateTime.Now;
                    newRow["Mandanten_ID"] = "1";
                    newRow["LoeschDatum"] = DateTime.Now;
                    newRow["Datenherkunft"] = "LABCENTRE_EXPORT";
                    newRow["DataVorsystemPK"] = "1";
                    newRow["Datenproduzent"] = "ETL Service";
                    newRow["Abfragezeitpunkt"] = DateTime.Now;

                    table.Rows.Add(newRow);
                    rowNumber++;

                    // check if the batch number of lines was read
                    if (rowNumber == end - start)
                        break;
                }

                Task.Run(() => Log(_processor, $"Read {rowNumber} lines from Excel file!", prozesslaeufe)).Wait();

                sheets.Tables.Add(table);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadExcelTable",
                    $"Reading data from Excel file failed! ({file})",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            // return the table of the first sheet
            try
            {
                return sheets.Tables[0];
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadExcelTable",
                    "Extracting first sheet to return failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

        }

        /// <summary>
        /// reads a CSV file and returns a DataTable containing its data
        /// </summary>
        /// <param name="file">path to CSV file to read</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <returns>DataTable with data of CSV file (returns null if any error occurs)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private DataTable ReadCSV(
            string file,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            DataTable table = new("CSV");
            Task.Run(() => Log(_processor, $"Reading CSV File {file}", prozesslaeufe)).Wait();

            GenericParserAdapter parser;
            // open the parser for CSV files
            try
            {
                parser = new GenericParserAdapter(@file)
                {
                    FirstRowHasHeader = true,
                    TextQualifier = '\"'
                };
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadCSV",
                    "Opening CSV Parser failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            // read each line of the file (first line will be recognizes to get column names)
            try
            {
                bool start = true;
                while (parser.Read())
                {
                    if (start)
                    {
                        // read columns
                        for (int i = 0; i < 2; i++)
                        {
                            string columnName = parser.GetColumnName(i);
                            table.Columns.Add(columnName, typeof(string));
                        }
                        start = false;
                    }
                    DataRow dr = table.NewRow();

                    for (int i = 0; i < parser.ColumnCount; i++)
                    {
                        dr[i] = parser[i];
                    }

                    table.Rows.Add(dr);
                }
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadCSV",
                    $"Reading the CSV data failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            // close the parser
            try
            {
                parser.Close();
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "ReadCSV",
                    $"Closing the CSV parser failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            return table;
        }

        /// <summary>
        /// reads data from a DB given by a connection and a sql command and writes it as CSV to a given file
        /// </summary>
        /// <param name="command">SQL Command to execute</param>
        /// <param name="confID">ETL_Konfigurationen_ID of realization defining connection</param>
        /// <param name="file">path to file to write data to</param>
        /// <param name="parameterID">ETL_Paketschritt_Parameter_ID to read configurations to format</param>
        /// <param name="connection">connection to DB to retrieve data drom</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="workflow">workflow object that executes the step</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void TransferDBToCSV(
            string command,
            string confID,
            string file,
            int? parameterID,
            DbConnection connection,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            Workflow workflow
        )
        {
            // Create DataTable containing wanted data
            DataTable data;
            try
            {
                data = _processor.DbHelper.GetDataTableFromQuery(
                    _processor,
                    command,
                    prozesslaeufe,
                    confID,
                    builder: new SqlConnectionStringBuilder(connection.ConnectionString)
                );

                if (data == null || data.Columns.Count == 0)
                    throw new ETLException(
                        _processor,
                        "Wanted data does not exist or reading failed!",
                        "TransfeDBToCSV",
                        prozesslaeufe
                    );
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransfeDBToCSV",
                    $"Reading data from DB failed!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            // read configuration parameters
            string delimiter = ";";
            bool header = true;
            string dateFormat = "yyyy-MM-ddTHH:mm:ss.fff";
            string stringQualifier = "";
            string escChar = "\"";
            string nullValue = "";
            try
            {
                if (parameterID != null)
                {
                    DataTable parameter = _processor.DbHelper.GetDataTableFromQuery(
                        _processor,
                        $"SELECT * " +
                        $"FROM pc.ETL_Paketschritt_Parameter parameter " +
                        $"JOIN pc.ETL_Paketschritte_Paketschritt_Parameter map " +
                        $"     ON map.ETL_Paketschritt_Parameter_ID = parameter.ETL_Paketschritt_Parameter_ID " +
                        $"JOIN pc.ETL_Paketschritte steps " +
                        $"     ON steps.ETL_Paketschritte_ID = map.ETL_Paketschritte_ID " +
                        $"WHERE parameter.Ist_aktiv = 1 AND map.Ist_aktiv = 1 AND steps.Ist_aktiv = 1 AND " +
                        $"      parameter.ETL_Paketschritt_Parameter_ID = {parameterID} AND " +
                        $"      map.ETL_Workflow_ID = {workflow.GetID()}",
                        prozesslaeufe
                    );

                    // read parameters
                    if (parameter.Rows[0]["Trennzeichen"].ToString() != "")
                    {
                        object? value = parameter.Rows[0]["Trennzeichen"];
                        delimiter = value != null ? value.ToString()! : string.Empty;
                    }
                    if (parameter.Rows[0]["Kopfzeile"].ToString() != "")
                    {
                        header = parameter.Rows[0]["Kopfzeile"].ToString() == "True";
                    }
                    if (parameter.Rows[0]["Datumsformat"].ToString() != "")
                    {
                        object? value = parameter.Rows[0]["Datumsformat"];
                        dateFormat = value != null ? value.ToString()! : string.Empty;
                    }
                    if (parameter.Rows[0]["Textqualifizierer"].ToString() != "")
                    {
                        object? value = parameter.Rows[0]["Textqualifizierer"];
                        stringQualifier = value != null ? value.ToString()! : string.Empty;
                    }
                    if (parameter.Rows[0]["Escapecharacter"].ToString() != "")
                    {
                        object? value = parameter.Rows[0]["Escapecharacter"];
                        escChar = value != null ? value.ToString()! : string.Empty;
                    }
                    if (parameter.Rows[0]["Leerwert"].ToString() != "")
                    {
                        object? value = parameter.Rows[0]["Leerwert"];
                        nullValue = value != null ? value.ToString()! : string.Empty;
                    }
                }
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransfeDBToCSV",
                    $"Failed reading format parameters!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            // create CSV Content
            StringBuilder csvContent = new();

            // read and write column names if wanted
            try
            {
                if (header)
                {
                    for (int i = 0; i < data.Columns.Count; i++)
                    {
                        csvContent.Append(data.Columns[i].ColumnName);

                        // Kein Trennzeichen nach der letzten Spalte
                        if (i < data.Columns.Count - 1)
                            csvContent.Append(delimiter);
                    }
                    csvContent.AppendLine();
                }
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransfeDBToCSV",
                    $"Failed reading column names!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            // read data rows
            try
            {
                foreach (DataRow row in data.Rows)
                {
                    for (int i = 0; i < data.Columns.Count; i++)
                    {
                        string cell = row[i]?.ToString() ?? string.Empty;

                        // escape delimiter or quotation marks
                        if (data.Columns[i].DataType == typeof(string))
                        {
                            if (stringQualifier != "")
                            {
                                if (cell.Contains(delimiter) || cell.Contains(stringQualifier))
                                {
                                    cell = cell.Replace(stringQualifier, $"{escChar}{stringQualifier}");
                                    cell = stringQualifier + cell.Replace(delimiter, $"{escChar}{delimiter}") +
                                           stringQualifier;
                                } else
                                {
                                    cell = stringQualifier + cell + stringQualifier;
                                }
                            } else
                            {
                                if (cell.Contains(delimiter))
                                {
                                    cell = stringQualifier + cell.Replace(delimiter, $"{escChar}{delimiter}") +
                                           stringQualifier;
                                }
                                else
                                {
                                    cell = stringQualifier + cell + stringQualifier;
                                }
                            }
                        }

                        // formate date
                        if (data.Columns[i].DataType == typeof(DateTime))
                            cell = DateTime.Parse(cell).ToString(dateFormat);

                        if (cell == $"{stringQualifier}{stringQualifier}" || cell == "")
                            cell = nullValue;

                        csvContent.Append(cell);

                        // no delimiter after last column
                        if (i < data.Columns.Count - 1)
                            csvContent.Append(delimiter);
                    }
                    csvContent.AppendLine();
                }
            } catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransfeDBToCSV",
                    $"Failed reading data rows!",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }

            // write csv content to file
            string targetFile = MyRegex().Replace(file.Replace("'", "").Replace(" ", "_"), "_");
            try
            {
                File.WriteAllText(targetFile, csvContent.ToString(), Encoding.UTF8);
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    _processor,
                    e,
                    "TransfeDBToCSV",
                    $"Failed writing data to CSV File ({targetFile})",
                    ref DummySem,
                    prozesslaeufe,
                    workflow.GetCancelSource()
                );
            }
        }

        [GeneratedRegex(@"(?<=\d):(?=\d)")]
        private static partial Regex MyRegex();
    }
}
