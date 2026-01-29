using System.Data;
using System.Data.Common;

using Npgsql;
using Microsoft.Data.SqlClient;
using InterSystems.Data.IRISClient;
using Oracle.ManagedDataAccess.Client;
using InterSystems.Data.CacheClient;

namespace DIZService.Core
{
    /// <summary>
    /// A Helper class to run basic functions on DB to receive Commands, DataTables etc.
    /// </summary>
    public class DBHelper : Helper
    {
        private readonly string _serviceName;

        // connection string builder to connect to right stage DB (default = dev)
        public readonly SqlConnectionStringBuilder BaseConnectionsString;

        /// <summary>
        /// creates the connection string needed for given string based on environment variables
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        public DBHelper(Processor processor)
        {
            _serviceName = processor.Servicename;

            try
            {
                SqlConnectionStringBuilder stageBuilder = new()
                {
                    DataSource = $@"{Environment.GetEnvironmentVariable($"{_serviceName}_datasource")}",
                    UserID = Environment.GetEnvironmentVariable($"{_serviceName}_user"),
                    Password = Environment.GetEnvironmentVariable($"{_serviceName}_pwd"),
                    InitialCatalog = Environment.GetEnvironmentVariable($"{_serviceName}_db"),
                    TrustServerCertificate = true,
                };

                BaseConnectionsString = stageBuilder;
            } catch (Exception e)
            {
                SafeExit(processor, e, "DBHelper", _dummyTuple);
                Environment.Exit(4);
            }
        }

        /// <summary>
        /// Enter SQL query to receive Data from an SQL Server as DataTable.
        ///
        /// Ensure to not receive to big data! -> will raise error
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="query">query to execute to fill table</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="confID">ETL_Konfigurationen_ID of realization defining connection</param>
        /// <param name="builder">to build a connectionstring to target (if null the config connection is used</param>
        /// <returns>Filled DataTable including data based on query, otherwise null (error etc.)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public DataTable GetDataTableFromQuery(
            Processor processor,
            string query,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            string confID = "",
            bool log = false,
            SqlConnectionStringBuilder? builder = null
        )
        {
            SqlConnectionStringBuilder stringBuilder = builder ?? BaseConnectionsString;

            // establish and connect to DB
            using SqlConnection connection = new(stringBuilder.ConnectionString);
            connection.Open();

            // fill DataTable with query data
            DataTable table;
            try
            {
                SqlDataAdapter adapter = new(query, connection);
                table = new DataTable();

                if (log)
                    Task.Run(() => LogQuery(
                        processor, query, confID != "" ? Convert.ToInt32(confID) : -1, prozesslaeufe)).Wait();

                adapter.Fill(table);
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    $"Filling DataTable from query failed! ({query})",
                    "GetDataTableFromQuery",
                    e,
                    prozesslaeufe
                );
            }

            return table;
        }

        /// <summary>
        /// Executes a given command in DIZ_NET
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="command">SQL command to execute</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void ExecuteCommandDIZ(
            Processor processor,
            string command,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            bool fromError = false
        )
        {
            try
            {
                SqlConnection conn = new(BaseConnectionsString.ConnectionString);
                Serilog.Log.Information($"Open Connection ({BaseConnectionsString.ConnectionString})");
                conn.Open();
                Serilog.Log.Information($"Opened Connection ({BaseConnectionsString.ConnectionString})");

                SqlCommand cmd = new(command, conn);
                cmd.ExecuteNonQuery();

                conn.Close();
                Serilog.Log.Information("Closed Connection");
            }
            catch (Exception e)
            {
                if (!fromError)
                    throw new ETLException(
                        processor,
                        $"Executing Command failed! ({command})",
                        "ExecuteCommandDIZ",
                        e,
                        prozesslaeufe
                    );
            }
        }

        /// <summary>
        /// query given data from a DataTable and receive the wanted DataRow
        /// !!! Will return only one DataRow if only one exists, otherwise null
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="table">DataTable to extract row from</param>
        /// <param name="query">query to select wanted rows</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>DataRow that matches the query</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public static DataRow GetDataRowFromDataTable(
            Processor processor,
            DataTable table,
            string query,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            DataRow[] selection;
            try
            {
                selection = table.Select(query);
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    "Querying data from DataTable failed!",
                    "GetDataRowFromDataTable",
                    e,
                    prozesslaeufe
                );
            }

            if (selection.Length == 1)
            {
                return selection[0];
            }
            else
            {
                throw new ETLException(
                    processor,
                    $"Selected Data had more than one row! (Num Rows: {selection.Length} | {query})",
                    "GetDataRowFromDataTable",
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// returns the appropriate DB Connection based on type and connection information
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="type">defines DB "type" (e.g., SQL, Oracle, ...)</param>
        /// <param name="connection">DataRow that includes several information to establish connection to DB</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>DB Connection that can be used for DB commands, otherwise null (error etc.)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        /// <exception cref="NYIException">in case of not implemented case</exception>
        public static DbConnection? GetConnection(
            Processor processor,
            string type,
            DataRow connection,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                switch (type)
                {
                    case "Access":
                        throw new NYIException(processor, "MS Access NYI", "GetConnection", prozesslaeufe);
                    case "Cache":
                        CacheConnection cacheConnection = new(connection["Verbindungszeichenkette"].ToString());
                        return cacheConnection;
                    case "CSV":
                        throw new NYIException(processor, "CSV NYI", "GetConnection", prozesslaeufe);
                    case "DB2":
                        throw new NYIException(processor, "IBM NYI", "GetConnection", prozesslaeufe);
                    case "Excel":
                        // since the excel files are placed within directories we dont need a connection
                        return null;
                    case "Firebird":
                        throw new NYIException(
                            processor, "Embarcadero InterBase NYI", "GetConnection", prozesslaeufe);
                    case "HL7":
                        throw new NYIException(processor, "Health Level 7 NYI", "GetConnection", prozesslaeufe);
                    case "Infomix":
                        throw new NYIException(processor, "IBM NYI", "GetConnection", prozesslaeufe);
                    case "JSON":
                        throw new NYIException(
                            processor, "JS Object Notation NYI", "GetConnection", prozesslaeufe);
                    case "MS-SQL Server":
                        SqlConnection msSqlConnection = new(connection["Verbindungszeichenkette"].ToString());
                        return msSqlConnection;
                    case "MySQL":
                        throw new NYIException(processor, "MySQL NYI", "GetConnection", prozesslaeufe);
                    case "Oracle":
                        OracleConnection oracleConnection = new(
                            "Data Source=(DESCRIPTION =" +
                            "(ADDRESS = (PROTOCOL = TCP)" +
                                        $"(HOST = {connection["IP_Adresse"]})" +
                                        $"(PORT = {connection["Netzwerkport"]}))" +
                            "(CONNECT_DATA =" +
                                $"(SERVICE_NAME = {connection["Datenbankschema"]})));" +
                                $"User Id={connection["Benutzer"]}" +
                                $";Password={connection["Kennwort"]};");
                        return oracleConnection;
                    case "Postgres":
                        NpgsqlConnection postgreSQLConnection = new(connection["Verbindungszeichenkette"].ToString());
                        return postgreSQLConnection;
                    case "SAP":
                        throw new NYIException(processor, "SAPRFC NYI", "GetConnection", prozesslaeufe);
                    case "WebRequest":
                        throw new NYIException(processor, "Webservice NYI", "GetConnection", prozesslaeufe);
                    case "XML":
                        throw new NYIException(
                            processor, "Extensible Makup Language NYI", "GetConnection", prozesslaeufe);
                    default:
                        throw new ETLException(
                            processor, "Unknown Connection Type", "GetConnection", prozesslaeufe);
                }
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    "Defining connection failed!",
                    "GetConnection",
                    e,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// Resturns an appropriate data adapter to fill, e.g., DataTables based on a connection and needed query
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="type">defines DB "type" (e.g., SQL, Oracle, ...)</param>
        /// <param name="connection">connection to DB</param>
        /// <param name="query">query to execute and use to fill, e.g., DataTable with</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>DB DataAdapter that can be used for DataTables, otherwise null (error etc.)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        /// <exception cref="NYIException">in case of not implemented case</exception>
        public static DbDataAdapter GetDataAdapter(
            Processor processor,
            string type,
            DbConnection connection,
            string query,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                switch (type)
                {
                    case "Access":
                        throw new NYIException(processor, "MS Access NYI", "GetDataAdapter", prozesslaeufe);
                    case "Cache":
                        CacheDataAdapter cacheDataAdapter = new(query, (CacheConnection) connection);
                        return cacheDataAdapter;
                    case "CSV":
                        throw new NYIException(processor, "CSV NYI", "GetDataAdapter", prozesslaeufe);
                    case "DB2":
                        throw new NYIException(processor, "IBM NYI", "GetDataAdapter", prozesslaeufe);
                    case "Excel":
                        throw new NYIException(processor, "Excel NYI", "GetDataAdapter", prozesslaeufe);
                    case "Firebird":
                        throw new NYIException(
                            processor, "Embarcadero InterBase NYI", "GetDataAdapter", prozesslaeufe);
                    case "HL7":
                        throw new NYIException(processor, "Health Level 7 NYI", "GetDataAdapter", prozesslaeufe);
                    case "Infomix":
                        throw new NYIException(processor, "IBM NYI", "GetDataAdapter", prozesslaeufe);
                    case "JSON":
                        throw new NYIException(
                            processor, "JavaScript Object Notation NYI", "GetDataAdapter", prozesslaeufe);
                    case "MS-SQL Server":
                        SqlDataAdapter msSqlDataAdapter = new(query, (SqlConnection)connection);
                        return msSqlDataAdapter;
                    case "MySQL":
                        throw new NYIException(processor, "MySQL AB NYI", "GetDataAdapter", prozesslaeufe);
                    case "Oracle":
                        OracleDataAdapter oracleDataAdapter = new(query, (OracleConnection)connection);
                        return oracleDataAdapter;
                    case "Postgres":
                        NpgsqlDataAdapter postgreSQLDataAdapter = new(query, (NpgsqlConnection)connection);
                        return postgreSQLDataAdapter;
                    case "SAP":
                        throw new NYIException(processor, "SAPRFC NYI", "GetDataAdapter", prozesslaeufe);
                    case "WebRequest":
                        throw new NYIException(processor, "Webservice NYI", "GetDataAdapter", prozesslaeufe);
                    case "XML":
                        throw new NYIException(
                            processor, "Extensible Makup Language NYI", "GetDataAdapter", prozesslaeufe);
                    default:
                        throw new ETLException(
                            processor, "Unknown DataAdapter Type", "GetDataAdapter", prozesslaeufe);
                }
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    "Defining a DataAdapter failed!",
                    "GetDataAdapter",
                    e,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// Resturns an appropriate DB Command that can be exexcuted (e.g., for truncation commands)
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="type">defines DB "type" (e.g., SQL, Oracle, ...)</param>
        /// <param name="connection">connection to DB</param>
        /// <param name="query">query to execute and use to fill, e.g., DataTable with</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>DB Command that can be executed, otherwise null (error etc.)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        /// <exception cref="NYIException">in case of not implemented case</exception>
        public static DbCommand GetCommand(
            Processor processor,
            string type,
            DbConnection connection,
            string query,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                switch (type)
                {
                    case "Access":
                        throw new NYIException(processor, "MS Access NYI", "GetCommand", prozesslaeufe);
                    case "Cache":
                        CacheCommand cahceCommand = new(query, (CacheConnection) connection);
                        return cahceCommand;
                    case "CSV":
                        throw new NYIException(processor, "CSV NYI", "GetCommand", prozesslaeufe);
                    case "DB2":
                        throw new NYIException(processor, "IBM NYI", "GetCommand", prozesslaeufe);
                    case "Excel":
                        throw new NYIException(processor, "Excel NYI", "GetCommand", prozesslaeufe);
                    case "Firebird":
                        throw new NYIException(
                            processor, "Embarcadero InterBase NYI", "GetCommand", prozesslaeufe);
                    case "HL7":
                        throw new NYIException(processor, "Health Level 7 NYI", "GetCommand", prozesslaeufe);
                    case "Infomix":
                        throw new NYIException(processor, "IBM NYI", "GetCommand", prozesslaeufe);
                    case "JSON":
                        throw new NYIException(
                            processor, "JavaScript Object Notation NYI", "GetCommand", prozesslaeufe);
                    case "MS-SQL Server":
                        SqlCommand msSqlCommand = new(query, (SqlConnection)connection);
                        return msSqlCommand;
                    case "MySQL":
                        throw new NYIException(processor, "MySQL AB NYI", "GetCommand", prozesslaeufe);
                    case "Oracle":
                        OracleCommand oracleCommand = new(query, (OracleConnection)connection);
                        return oracleCommand;
                    case "Postgres":
                        NpgsqlCommand postgreSQLCommand = new(query, (NpgsqlConnection)connection);
                        return postgreSQLCommand;
                    case "SAP":
                        throw new NYIException(processor, "SAPRFC NYI", "GetCommand", prozesslaeufe);
                    case "WebRequest":
                        throw new NYIException(processor, "Webservice NYI", "GetCommand", prozesslaeufe);
                    case "XML":
                        throw new NYIException(
                            processor, "Extensible Makup Language NYI", "GetCommand", prozesslaeufe);
                    default:
                        throw new ETLException(processor, "Unknown Command Type", "GetCommand", prozesslaeufe);
                }
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    "Defining Command failed!",
                    "GetCommand",
                    e,
                    prozesslaeufe
                );
            }
        }
    }
}
