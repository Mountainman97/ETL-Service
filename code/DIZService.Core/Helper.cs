using System.Net;
using System.Data;
using System.Net.Mail;
using System.Data.Common;
using System.Text.RegularExpressions;

using Microsoft.Data.SqlClient;
using AdysTech.CredentialManager;

namespace DIZService.Core
{
    /// <summary>
    /// Exception to when given functionalities are not implemented yet and should be implemented some time. Will write
    /// automatically an error into Log
    /// </summary>
    public class NYIException : Exception
    {
        private readonly Helper helper = new();

        public NYIException() : base() { }
        public NYIException(
            Processor processor,
            string message,
            string procedure,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message)
        {
            Task.Run(() => helper.ErrorLog(
                processor,
                "Dienst",
                message,
                "minor",
                null,
                procedure,
                prozesslaeufe
            )).Wait();
        }
        public NYIException(
            Processor processor,
            string message,
            string procedure,
            Exception inner,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message, inner)
        {
            Task.Run(() => helper.ErrorLog(
                processor,
                "Dienst",
                message,
                "minor",
                inner,
                procedure,
                prozesslaeufe
            )).Wait();
        }
    }

    public class ETLException : Exception
    {
        private readonly Helper helper = new();

        public ETLException() : base() { }
        public ETLException(string message) : base(message) { }
        public ETLException(string message, Exception inner) : base(message, inner) { }
        public ETLException(
            Processor processor,
            string message,
            string procedure,
            Workflow workflow,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message)
        {
            if (processor != null)
            {
                if (!processor.WorkflowManager.IsWorkflow(
                        WorkflowStage.Failed,
                        workflow,
                        prozesslaeufe
                ))
                {
                    Task.Run(() => helper.ErrorLog(
                        processor,
                        "Dienst",
                        message,
                        "minor",
                        null,
                        procedure,
                        prozesslaeufe
                    )).Wait();
                    processor.WorkflowManager.SetWorkflowFailed(workflow, prozesslaeufe);
                    new Helper().OutputList(
                        processor, processor.WorkflowManager.GetFailedWorkflows(), "ErrorWorkflows");
                }
            }
        }
        public ETLException(
            Processor processor,
            string message,
            string procedure,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message)
        {
            Task.Run(() => helper.ErrorLog(
                processor,
                "Dienst",
                message,
                "minor",
                null,
                procedure,
                prozesslaeufe
            )).Wait();
        }
        public ETLException(
            Processor processor,
            string message,
            string procedure,
            Exception inner,
            Workflow workflow,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message, inner)
        {
            if (processor != null)
            {
                if (!processor.WorkflowManager.IsWorkflow(
                        WorkflowStage.Failed,
                        workflow,
                        prozesslaeufe
                ))
                {
                    Task.Run(() => helper.ErrorLog(
                        processor,
                        "Dienst",
                        message,
                        "minor",
                        inner,
                        procedure,
                        prozesslaeufe
                    )).Wait();
                    processor.WorkflowManager.SetWorkflowFailed(workflow, prozesslaeufe);
                    new Helper().OutputList(
                        processor, processor.WorkflowManager.GetFailedWorkflows(), "ErrorWorkflows");
                }
            }
        }
        public ETLException(
            Processor processor,
            string message,
            string procedure,
            Exception inner,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message, inner)
        {
            Task.Run(() => helper.ErrorLog(
                processor,
                "Dienst",
                message,
                "minor",
                inner,
                procedure,
                prozesslaeufe
            )).Wait();
        }
        public ETLException(
            Processor processor,
            string message,
            string procedure,
            Exception inner
        ) : base(message, inner)
        {
            Task.Run(() => helper.ErrorLog(
                processor,
                "Dienst",
                message,
                "minor",
                inner,
                procedure,
                processor.DbHelper._dummyTuple
            )).Wait();
        }
        public ETLException(
            Processor processor,
            string message,
            string procedure,
            Exception inner,
            CancellationTokenSource source,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message, inner)
        {
            source.Cancel();

            Task.Run(() => helper.ErrorLog(
                processor,
                "Dienst",
                message,
                "minor",
                inner,
                procedure,
                prozesslaeufe
            )).Wait();
        }
        public ETLException(
            Processor processor,
            string message,
            string procedure,
            string identity,
            Exception inner,
            Workflow workflow,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message, inner)
        {
            if (processor != null)
            {
                if (!processor.WorkflowManager.IsWorkflow(
                        WorkflowStage.Failed,
                        workflow,
                        prozesslaeufe
                ))
                {
                    Task.Run(() => helper.ErrorLog(
                        processor,
                        "Dienst",
                        message,
                        "minor",
                        inner,
                        procedure,
                        fehlerobjekt: identity,
                        prozesslaeufe: prozesslaeufe
                    )).Wait();
                    processor.WorkflowManager.SetWorkflowFailed(workflow, prozesslaeufe);
                    new Helper().OutputList(
                        processor, processor.WorkflowManager.GetFailedWorkflows(), "ErrorWorkflows");
                }
            }
        }
        public ETLException(
            Processor processor,
            string message,
            string procedure,
            string identity,
            Exception? inner,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        ) : base(message, inner)
        {
            Task.Run(() => helper.ErrorLog(
                processor,
                "Dienst",
                message,
                "minor",
                inner,
                procedure,
                fehlerobjekt: identity,
                prozesslaeufe: prozesslaeufe
            )).Wait();
        }
    }

    public enum LogType
    {
        Info,
        Warning,
        Error,
        Fatal
    }

    public partial class Helper
    {
        // gives the directory of execution (ether Release or Debug directory in project)
        public readonly string BaseDirectory = AppDomain.CurrentDomain.BaseDirectory;

        private const string DateFormat = "yyyy-MM-ddTHH:mm:ss.fff";  // to formate dates in the right format for DB
        private const string JsonDateFormat = "yyyy-MM-ddTHH:mm:ss";  // to formate dates in the right format for DB

        private const string LoggingTable = "Logging.ETL_Meldungen";             // name of normal logging table
        private const string QueryTable = "Logging.ETL_SQL_Anfragen";            // name of query logging table
        private const string ErrorLoggingTable = "Logging.ETL_Fehlermeldungen";  // name of error log table

        private readonly string _loggingFile = "ETL_log";                // path to logging file (local)
        private readonly string _queryFile = "ETL_queries";              // path to query logging file (local)
        private readonly string _errorLoggingFile = "ETL_errorLog.log";  // path to error logg file (local)

        public SemaphoreSlim DummySem = new(1,1);  // use as default value for HandleErrorCatch

        // use as default/dummy tuple for prozesslaeufe tuple
        public readonly Tuple<int?, int?, int?, int?> _dummyTuple = new(
            null, null, null, null
        );

        // lists all prozesslaeufe tables (except for workflows)
        private readonly List<string> _htmlTablesSearch =
        [
            "Logging.ETL_Paket_Prozesslaeufe",
            "Logging.ETL_Paketumsetzung_Prozesslaeufe",
            "Logging.ETL_Paketschritt_Prozesslaeufe"
        ];

        /// <summary>
        /// reads all given configuration parameters and returns them as dictionary
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <returns>dictionary of parameters</returns>
        public Dictionary<string, object> ReadConfigurations(Processor processor)
        {
            int stdNumThreads = 1;
            bool stdDebug = false;
            int stdLogInterval = 5;

            // open connection to SQL Server for reading configurations
            using SqlConnection connection = new(processor.DbHelper.BaseConnectionsString.ConnectionString);
            connection.Open();

            Dictionary<string, object> parameters = new()
            {
                { "Anzahl_ETL_Threads", stdNumThreads },
                { "Debug", stdDebug },
                { "LogInterval", stdLogInterval },
            };

            System.Data.DataTable configs;
            try
            {
                configs = processor.DbHelper.GetDataTableFromQuery(
                    processor,
                    "SELECT * FROM conf.Konfigurationsparameter",
                    _dummyTuple
                );
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    "Querying Configurations failed!",
                    "minor",
                    e,
                    "ReadConfigurations",
                    _dummyTuple
                )).Wait();
                return parameters;
            }

            foreach (DataRow config in configs.Rows)
            {
                string parameter = config["Parametername"]?.ToString() ?? "";

                switch (parameter)
                {
                    case "Anzahl_ETL_Threads":
                        // read the max. number of threads from given configuration
                        try
                        {
                            parameters["Anzahl_ETL_Threads"] = int.Parse(config["Parameterwert"]?.ToString() ?? "1");
                        }
                        catch (Exception e)
                        {
                            Task.Run(() => ErrorLog(
                                processor,
                                "Dienst",
                                "Extracting the max. number of threads parameter failed!",
                                "minor",
                                e,
                                "ReadConfigurations",
                                _dummyTuple
                            )).Wait();
                        }
                        break;
                    case "Debug":
                        try
                        {
                            parameters["Debug"] = int.Parse(config["Parameterwert"].ToString() ??
                                                    throw new ETLException("No Parameterwert")) == 1;
                        }
                        catch (Exception e)
                        {
                            Task.Run(() => ErrorLog(
                                processor,
                                "Dienst",
                                "Extracting the modus parameter failed!",
                                "minor",
                                e,
                                "ReadConfigurations",
                                _dummyTuple
                            )).Wait();
                        }
                        break;
                    case "LogInterval":
                        try
                        {
                            parameters["LogInterval"] = int.Parse(config["Parameterwert"].ToString() ??
                                                            throw new ETLException("No Parameterwert"));
                        }
                        catch (Exception e)
                        {
                            Task.Run(() => ErrorLog(
                                processor,
                                "Dienst",
                                "Extracting the logging interval failed!",
                                "minor",
                                e,
                                "ReadConfigurations",
                                _dummyTuple
                            )).Wait();
                        }
                        break;
                    default:
                        break;
                }
            }

            return parameters;
        }

        /// <summary>
        /// generates and sends a html via email to given receiver with the given information
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="subject">subject of the email</param>
        /// <param name="message">message to display in mail</param>
        /// <param name="workflowID">ID of workflow that the message belongs to</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="receiver">email address of the receiver (Default = ma.bergmann@mul-ct.de)</param>
        /// <param name="ccReceiver">email address of the optional cc receiver (Default = ch.papritz@mul-ct.de)</param>
        public void SendInfoMail(
            Processor processor,
            string subject,
            string message,
            int workflowID,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            string receiver = "TBD",
            string ccReceiver = "TBD"
        )
        {
            try
            {
                MailMessage mail = new()
                {
                    From = new MailAddress("TBD") // set sender address
                };
                mail.To.Add(receiver); // det destination address

                if (processor.Stage == "PROD")
                    mail.CC.Add(ccReceiver);

                string wwwdir = BaseDirectory + @"..\www";

                string htmlBody = File.ReadAllText($"{wwwdir}\\diz_info_mail.html");

                mail.Subject = $"[{processor.Servicename}] [{processor.Stage}] [INFO] {subject}"; // set subject of mail

                htmlBody = htmlBody.Replace("{{TITLE}}", $"Information for Workflow with ID {workflowID}");
                htmlBody = htmlBody.Replace("{{MESSAGE}}", message);

                // set the html body into mail
                mail.Body = htmlBody;
                mail.IsBodyHtml = true; // define that body is html

                // insert logo images
                Attachment inlineImagetrs = new($"{wwwdir}\\logo_trs_dark.png")
                {
                    ContentId = "trs"  // Content-ID for trs logo image
                };

                // set Inline-Flag
                (inlineImagetrs.ContentDisposition ?? throw new ETLException("No inlineImagetrs")).Inline = true;
                mail.Attachments.Add(inlineImagetrs);
                var inlineImagemul = new Attachment($"{wwwdir}\\logo_mul_dark.png")
                {
                    ContentId = "mul" // Content-ID für das Bild
                };
                // set Inline-Flag
                (inlineImagemul.ContentDisposition ?? throw new ETLException("No inlineImagetrs")).Inline = true;
                mail.Attachments.Add(inlineImagemul);

                NetworkCredential cred = CredentialManager.GetCredentials("TBD");

                // configure SMTP-Server
                SmtpClient smtpClient = new("TBD")
                {
                    Port = 25,
                    Credentials = cred,
                    EnableSsl = false // activate SSL/TLS
                };

                // send E-Mail
                smtpClient.Send(mail);
                Task.Run(() => Log(
                    processor,
                    $"Info-Email was send successfully for Workflow {workflowID}",
                    _dummyTuple
                )).Wait();
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    $"Failed sending Info Mail for Workflow {workflowID}",
                    "minor",
                    e,
                    "SendInfoMail",
                    prozesslaeufe
                )).Wait();
            }
        }

        /// <summary>
        /// generates and sends a html via email to given receiver with information of stopping the service
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="subject">subject of the email</param>
        /// <param name="message">message to display in mail</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="receiver">email address of the receiver (Default = ma.bergmann@mul-ct.de)</param>
        /// <param name="ccReceiver">email address of the optional cc receiver (Default = ch.papritz@mul-ct.de)</param>
        public void SendStopMail(
            Processor processor,
            string subject,
            string message,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            string receiver = "TBD",
            string ccReceiver = "TBD"
        )
        {
            try
            {
                MailMessage mail = new()
                {
                    From = new MailAddress("TBD") // set sender address
                };
                mail.To.Add(receiver); // det destination address

                if (processor.Stage == "PROD")
                    mail.CC.Add(ccReceiver);

                string wwwdir = BaseDirectory + @"..\www";

                string htmlBody = File.ReadAllText($"{wwwdir}\\diz_info_mail.html");

                // set subject of mail
                mail.Subject = $"[{processor.Servicename}] [{processor.Stage}] [MAJOR] {subject}";

                htmlBody = htmlBody.Replace("{{TITLE}}", "Dienst wurde unerwartet beendet!");
                htmlBody = htmlBody.Replace("{{MESSAGE}}", message);
                htmlBody = htmlBody.Replace("{{SERVICENAME}}", processor.Servicename);

                // set the html body into mail
                mail.Body = htmlBody;
                mail.IsBodyHtml = true; // define that body is html

                // insert logo images
                var inlineImagetrs = new Attachment($"{wwwdir}\\logo_trs_dark.png")
                {
                    ContentId = "trs"  // Content-ID for trs logo image
                };
                // set Inline-Flag
                (inlineImagetrs.ContentDisposition ?? throw new ETLException("No inlineImagetrs")).Inline = true;
                mail.Attachments.Add(inlineImagetrs);
                var inlineImagemul = new Attachment($"{wwwdir}\\logo_mul_dark.png")
                {
                    ContentId = "mul" // Content-ID für das Bild
                };
                // set Inline-Flag
                (inlineImagemul.ContentDisposition ?? throw new ETLException("No inlineImagetrs")).Inline = true;
                mail.Attachments.Add(inlineImagemul);

                NetworkCredential cred = CredentialManager.GetCredentials("TBD");

                // configure SMTP-Server
                SmtpClient smtpClient = new("TBD")
                {
                    Port = 25,
                    Credentials = cred,
                    EnableSsl = false // activate SSL/TLS
                };

                // send E-Mail
                smtpClient.Send(mail);
                Task.Run(() => Log(
                    processor,
                    $"Stop-Email was send successfully",
                    _dummyTuple
                )).Wait();
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    $"Failed sending Stop Mail!",
                    "minor",
                    e,
                    "SendStopMail",
                    prozesslaeufe
                )).Wait();
            }
        }

        /// <summary>
        /// generates and sends a html summary of the given workflow execution to a given receiver
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="success">true if exceution was successful</param>
        /// <param name="emailReceiver">email addresses of the receiver (Default = ma.bergmann@mul-ct.de)</param>
        /// <param name="ccReceiverList">email address of the optional cc receivers</param>
        /// <param name="workflow">workflow object that ended</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        public void SendResultMail(
            Processor processor,
            bool success,
            List<string> emailReceiver,
            List<string> ccReceiverList,
            Workflow workflow,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                if (emailReceiver is null || emailReceiver.Count == 0)
                    emailReceiver = ["TBD"];

                MailMessage mail = new()
                {
                    From = new MailAddress("TBD") // set sender address
                };

                // add all necessary receiver
                foreach (string receiver in emailReceiver)
                {
                    mail.To.Add(receiver);
                }

                // add all necessary CC receiver
                foreach (string ccReceiver in ccReceiverList)
                {
                    mail.CC.Add(ccReceiver);
                }

                string wwwdir = BaseDirectory + @"..\www";
                string htmlBody = File.ReadAllText($"{wwwdir}\\diz_mail.html");

                string successImg = "<img src=\"cid:check\" alt=\"success\" width=\"50px\">";
                string errorImg = "<img src=\"cid:error\" alt=\"success\" width=\"50px\">";

                if (success)
                {
                    htmlBody = htmlBody.Replace("{{success}}", "Erfolgreich");
                    htmlBody = htmlBody.Replace("{{succcessColor}}", "#44d478");
                    htmlBody = htmlBody.Replace("{{succcessImg}}", successImg);

                    htmlBody = htmlBody.Replace("{{ERRORTABLE}}", "");

                    var inlineImageCheck = new Attachment($"{wwwdir}\\check50.png")
                    {
                        ContentId = "check" // Content-ID success image
                    };
                    // set Inline-Flag
                    (inlineImageCheck.ContentDisposition ?? throw new ETLException("No inlineImagetrs")).Inline = true;
                    mail.Attachments.Add(inlineImageCheck);

                    // set subject of mail
                    mail.Subject = $"[{processor.Servicename}] [{processor.Stage}] [SUCCESS] " +
                                   $"Report Workflow {workflow.GetID()}";
                }
                else
                {
                    htmlBody = htmlBody.Replace("{{success}}", "Fehlgeschlagen");
                    htmlBody = htmlBody.Replace("{{succcessColor}}", "#c4254a");
                    htmlBody = htmlBody.Replace("{{succcessImg}}", errorImg);

                    string errorData = AnalyzeWorkflowError(processor, prozesslaeufe.Item1 ?? -1, prozesslaeufe);
                    htmlBody = htmlBody.Replace("{{ERRORTABLE}}", errorData);

                    var inlineImageError = new Attachment($"{wwwdir}\\error50.png")
                    {
                        ContentId = "error" // Content-ID for error image
                    };
                    // set Inline-Flag
                    (inlineImageError.ContentDisposition ?? throw new ETLException("No inlineImagetrs")).Inline = true;
                    mail.Attachments.Add(inlineImageError);

                    // set subject of mail
                    mail.Subject = $"[{processor.Servicename}] [{processor.Stage}] [FAILED] " +
                                   $"Report Workflow {workflow.GetID()}";
                }

                htmlBody = htmlBody.Replace("{{workflowID}}", workflow.GetID().ToString());

                DataRow execTime = processor.DbHelper.GetDataTableFromQuery(
                    processor,
                    $"SELECT A.Startzeitpunkt AS Startzeitpunkt, A.Endzeitpunkt AS Endzeitpunkt, " +
                    $"CASE WHEN A.Endzeitpunkt IS NULL " +
                    $"  THEN DATEDIFF(MINUTE, A.Startzeitpunkt, GETDATE()) " +
                    $"  ELSE DATEDIFF(MINUTE, A.Startzeitpunkt, A.Endzeitpunkt) " +
                    $"END AS Dauer_m, " +
                    $"CASE WHEN A.Endzeitpunkt IS NULL " +
                    $"  THEN DATEDIFF(SECOND, A.Startzeitpunkt, GETDATE()) " +
                    $"  ELSE DATEDIFF(SECOND, A.Startzeitpunkt, A.Endzeitpunkt) " +
                    $"END AS Dauer_s " +
                    $"FROM pc.ETL_Zeitplan_Ausfuehrungen AS A " +
                    $"     JOIN Logging.ETL_Prozesslaeufe AS B " +
                    $"ON A.ETL_Zeitplan_Ausfuehrungen_ID = B.ETL_Zeitplan_Ausfuehrungen_ID " +
                    $"WHERE  B.ETL_Prozesslaeufe_ID = {prozesslaeufe.Item1}",
                    prozesslaeufe).Rows[0];

                htmlBody = htmlBody.Replace("{{execStart}}", execTime["Startzeitpunkt"].ToString());
                htmlBody = htmlBody.Replace("{{execEnd}}", execTime["Endzeitpunkt"]?.ToString() ?? "-");

                htmlBody = htmlBody.Replace("{{takeoverFrom}}", workflow.GetTakeoverTime().Item1.ToString());
                htmlBody = htmlBody.Replace("{{takeoverTo}}", workflow.GetTakeoverTime().Item2.ToString());

                if (execTime["Dauer_m"].ToString() == "0")
                {
                    htmlBody = htmlBody.Replace("{{execTime}}", $"{execTime["Dauer_s"]} Sekunden");
                }
                else
                {
                    htmlBody = htmlBody.Replace("{{execTime}}", $"{execTime["Dauer_m"]} Minuten");
                }

                string data = AnalyzeWorkflowRun(
                    processor, prozesslaeufe.Item1 ?? -1, workflow, prozesslaeufe);
                htmlBody = htmlBody.Replace("{{TABLE}}", data);

                // add visualization to mail
                try
                {
                    var visual = new Attachment(
                        $"{wwwdir}\\..\\logs\\vizualizations\\Visual_Process_" + prozesslaeufe.Item1 + ".png")
                    {
                        ContentId = "check" // Content-ID success image
                    };
                    // set Inline-Flag
                    (visual.ContentDisposition ?? throw new ETLException("No visual")).Inline = false;
                    mail.Attachments.Add(visual);
                } catch (Exception e)
                {
                    Task.Run(() => ErrorLog(
                        processor,
                        "Dienst",
                        $"Failed adding vizualization to mail!",
                        "minor",
                        e,
                        "SendResultMail",
                        prozesslaeufe
                    )).Wait();
                }

                // set the html body into mail
                mail.Body = htmlBody;
                mail.IsBodyHtml = true; // define that body is html

                // insert logo images
                var inlineImagetrs = new Attachment($"{wwwdir}\\logo_trs_dark.png")
                {
                    ContentId = "trs"  // Content-ID for trs logo image
                };
                // set Inline-Flag
                (inlineImagetrs.ContentDisposition ?? throw new ETLException("No inlineImagetrs")).Inline = true;
                mail.Attachments.Add(inlineImagetrs);
                var inlineImagemul = new Attachment($"{wwwdir}\\logo_mul_dark.png")
                {
                    ContentId = "mul" // Content-ID für das Bild
                };
                // set Inline-Flag
                (inlineImagemul.ContentDisposition ?? throw new ETLException("No inlineImagetrs")).Inline = true;
                mail.Attachments.Add(inlineImagemul);

                NetworkCredential cred = CredentialManager.GetCredentials("TBD");

                // configure SMTP-Server
                SmtpClient smtpClient = new("TBD")
                {
                    Port = 25,
                    Credentials = cred,
                    EnableSsl = false // activate SSL/TLS
                };

                // send E-Mail
                smtpClient.Send(mail);
                Task.Run(() => Log(
                    processor,
                    $"Email was send successfully for Workflow {workflow.GetID()}",
                    _dummyTuple
                )).Wait();
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    $"Failed sending Result Mail for Workflow {workflow.GetID()}",
                    "minor",
                    e,
                    "SendResultMail",
                    prozesslaeufe
                )).Wait();
            }
        }

        /// <summary>
        /// analyzes the execution times and successes of packages and initializes the analyzation of realizations
        /// and steps and converts them into html table rows
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="prozesslaeufeID">ID of overall prozesslauf</param>
        /// <param name="workflow">workflow object that ended</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>html table string</returns>
        private string AnalyzeWorkflowRun(
            Processor processor,
            int prozesslaeufeID,
            Workflow workflow,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            string collectCMD = $"SELECT * FROM {_htmlTablesSearch[0]} WHERE ETL_Prozesslaeufe_ID = {prozesslaeufeID}";
            System.Data.DataTable packages = processor.DbHelper.GetDataTableFromQuery(
                                                                    processor, collectCMD, prozesslaeufe);

            string wwwdir = BaseDirectory + @"..\www";

            if (packages.Rows.Count == 0)
            {
                return "";
            }
            else
            {
                string htmlTable = File.ReadAllText($"{wwwdir}\\resultTable.txt");

                foreach (DataRow item in packages.Rows)
                {
                    string informationCMD = $"SELECT Paketname AS Information " +
                                            $"FROM pc.ETL_Pakete WHERE ETL_Pakete_ID = " +
                                            $"{item["ETL_Pakete_ID"]}";
                    System.Data.DataTable info = processor.DbHelper.GetDataTableFromQuery(
                                                                        processor, informationCMD, prozesslaeufe);

                    string paketRow = File.ReadAllText($"{wwwdir}\\resultTableRow.txt");

                    paketRow = paketRow.Replace("{{MODUL}}", $"Paket {item["ETL_Pakete_ID"]}");
                    paketRow = paketRow.Replace("{{INFO}}", info.Rows[0]["Information"].ToString());
                    paketRow = paketRow.Replace(
                        "{{VON}}",
                        item["Ausfuehrungsstartzeitpunkt"].ToString() == "" ? "-" :
                        item["Ausfuehrungsstartzeitpunkt"].ToString());
                    paketRow = paketRow.Replace(
                        "{{BIS}}",
                        item["Ausfuehrungsendzeitpunkt"].ToString() == "" ? "-" :
                        item["Ausfuehrungsendzeitpunkt"].ToString());

                    if (item["Ausfuehrungsendzeitpunkt"].ToString() != "")
                    {
                        if (item["Ausfuehrungsstartzeitpunkt"].ToString() != "")
                        {
                            double time_m = Math.Round(
                                (DateTime.Parse(item["Ausfuehrungsendzeitpunkt"].ToString() ??
                                        throw new ETLException("No Ausfuehrungsendzeitpunkt")) -
                                 DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                        throw new ETLException("No Ausfuehrungsstartzeitpunkt"))).TotalMinutes, 0);
                            double time_s = Math.Round(
                                (DateTime.Parse(item["Ausfuehrungsendzeitpunkt"].ToString() ??
                                        throw new ETLException("No Ausfuehrungsendzeitpunkt")) -
                                DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                        throw new ETLException("No Ausfuehrungsstartzeitpunkt"))).TotalSeconds, 0);
                            paketRow = paketRow.Replace(
                                "{{DAUER}}", time_m > 0 ? $"{time_m} Minuten" : $"{time_s} Sekunden");
                        }
                        else
                        {
                            paketRow = paketRow.Replace("{{DAUER}}", "-");
                        }
                    }
                    else
                    {
                        try
                        {
                            double time_m = Math.Round(
                                (DateTime.Now - DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString()
                                        ?? throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                            ).TotalMinutes, 0);
                            double time_s = Math.Round(
                                (DateTime.Now - DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString()
                                        ?? throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                            ).TotalSeconds, 0);
                            paketRow = paketRow.Replace(
                                "{{DAUER}}", time_m > 0 ? $"{time_m} Minuten" : $"{time_s} Sekunden");
                        }
                        catch
                        {
                            paketRow = paketRow.Replace("{{DAUER}}", "-");
                        }
                    }

                    paketRow = paketRow.Replace("{{SUCC}}", item["Erfolgreich"].ToString());

                    int paketProzesslaeufeID = Convert.ToInt32(item["ETL_Paket_Prozesslaeufe_ID"].ToString());

                    string realizationRows = AnalyzeLevel(
                        processor, prozesslaeufeID, paketProzesslaeufeID, 1, workflow, prozesslaeufe);
                    htmlTable = htmlTable.Replace("{{DATAROW}}", $"{paketRow}\n{realizationRows}\n" + "{{DATAROW}}");
                }

                htmlTable = htmlTable.Replace("{{DATAROW}}", $"");
                return htmlTable;
            }
        }

        /// <summary>
        /// analyzes the thrown errors of a given process and puts them into a table to show in email
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="prozesslaeufeID">ID of overall prozesslauf</param>
        /// <param name="workflow">workflow object that ended</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>html table string with error messages</returns>
        private string AnalyzeWorkflowError(
            Processor processor,
            int prozesslaeufeID,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            // collect errors
            string errorQuery = " " +
                $"SELECT p.ETL_Prozesslaeufe_ID AS PID " +
                $"      ,CAST(CONCAT('Workflow ', p.ETL_Workflow_ID) AS nvarchar(50)) AS Modul_ID " +
                $"      , w.Workflowname AS Bezeichnung " +
                $"      ,'-' AS Soll_Befehl " +
                $"      ,'-' AS Exec_Befehl " +
                $"      ,p.Anforderungszeitpunkt " +
                $"      ,p.Startzeitpunkt AS Start " +
                $"      ,p.Endzeitpunkt AS Ende " +
                $"      ,f.Meldungstext " +
                $"      ,f.Fehlertext " +
                $"      ,f.Fehlertyp " +
                $"      ,f.Prozedur " +
                $"      ,f.ETL_Fehlermeldungen_ID " +
                $"FROM Logging.ETL_Prozesslaeufe AS p " +
                $"JOIN pc.ETL_Workflow AS w " +
                $"    ON w.ETL_Workflow_ID = p.ETL_Workflow_ID " +
                $"JOIN Logging.ETL_Fehlermeldungen AS f " +
                $"    ON f.ETL_Prozesslaeufe_ID = p.ETL_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paket_Prozesslaeufe_ID IS NULL " +
                $"       AND f.ETL_Paketumsetzung_Prozesslaeufe_ID IS NULL " +
                $"       AND f.ETL_Paketschritt_Prozesslaeufe_ID IS NULL " +
                $"WHERE p.Erfolgreich = 0 AND p.ETL_Prozesslaeufe_ID = {prozesslaeufeID} " +
                $" " +
                $"UNION " +
                $" " +
                $"SELECT p.ETL_Paket_Prozesslaeufe_ID AS PID " +
                $"      ,CAST(CONCAT('Paket ', p.ETL_Pakete_ID) AS nvarchar(50)) AS Modul_ID " +
                $"      , pak.Paketname AS Bezeichnung " +
                $"      ,'-' AS Soll_Befehl " +
                $"      ,'-' AS Exec_Befehl " +
                $"      ,p.Anforderungszeitpunkt " +
                $"      ,p.Startzeitpunkt AS Start " +
                $"      ,p.Endzeitpunkt AS Ende " +
                $"      ,f.Meldungstext " +
                $"      ,f.Fehlertext " +
                $"      ,f.Fehlertyp " +
                $"      ,f.Prozedur " +
                $"      ,f.ETL_Fehlermeldungen_ID " +
                $"FROM Logging.ETL_Paket_Prozesslaeufe AS p " +
                $"JOIN pc.ETL_Pakete AS pak " +
                $"    ON pak.ETL_Pakete_ID = p.ETL_Pakete_ID " +
                $"JOIN Logging.ETL_Fehlermeldungen AS f " +
                $"    ON f.ETL_Prozesslaeufe_ID = p.ETL_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paket_Prozesslaeufe_ID = p.ETL_Paket_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paketumsetzung_Prozesslaeufe_ID IS NULL " +
                $"       AND f.ETL_Paketschritt_Prozesslaeufe_ID IS NULL " +
                $"WHERE p.Erfolgreich = 0 AND p.ETL_Prozesslaeufe_ID = {prozesslaeufeID} " +
                $" " +
                $"UNION " +
                $" " +
                $"SELECT p.ETL_Paketumsetzung_Prozesslaeufe_ID AS PID " +
                $"      ,CAST(CONCAT('Umsetzung ', p.ETL_Paket_Umsetzungen_ID) AS nvarchar(50)) AS Modul_ID " +
                $"      , u.Umsetzungsname AS Bezeichnung " +
                $"      ,'-' AS Soll_Befehl " +
                $"      ,'-' AS Exec_Befehl " +
                $"      ,p.Anforderungszeitpunkt " +
                $"      ,p.Startzeitpunkt AS Start " +
                $"      ,p.Endzeitpunkt AS Ende " +
                $"      ,f.Meldungstext " +
                $"      ,f.Fehlertext " +
                $"      ,f.Fehlertyp " +
                $"      ,f.Prozedur " +
                $"      ,f.ETL_Fehlermeldungen_ID " +
                $"FROM Logging.ETL_Paketumsetzung_Prozesslaeufe AS p " +
                $"JOIN pc.ETL_Paket_Umsetzungen AS u " +
                $"    ON u.ETL_Paket_Umsetzungen_ID = p.ETL_Paket_Umsetzungen_ID " +
                $"JOIN Logging.ETL_Fehlermeldungen AS f " +
                $"    ON f.ETL_Prozesslaeufe_ID = p.ETL_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paket_Prozesslaeufe_ID = p.ETL_Paket_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paketumsetzung_Prozesslaeufe_ID = p.ETL_Paketumsetzung_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paketschritt_Prozesslaeufe_ID IS NULL " +
                $"WHERE p.Erfolgreich = 0 AND p.ETL_Prozesslaeufe_ID = {prozesslaeufeID} " +
                $" " +
                $"UNION " +
                $" " +
                $"SELECT p.ETL_Paketschritt_Prozesslaeufe_ID AS PID " +
                $"      ,CAST(CONCAT('Schritt ', p.ETL_Paketschritte_ID) AS nvarchar(50)) AS Modul_ID " +
                $"      , s.Schrittname AS Bezeichnung " +
                $"      ,CASE WHEN s.Befehl IS NULL THEN '-' ELSE s.Befehl END AS Soll_Befehl " +
                $"      ,CASE WHEN anf.Query IS NULL THEN '-' ELSE anf.Query END AS Exec_Befehl " +
                $"      ,p.Anforderungszeitpunkt " +
                $"      ,p.Startzeitpunkt AS Start " +
                $"      ,p.Endzeitpunkt AS Ende " +
                $"      ,f.Meldungstext " +
                $"      ,f.Fehlertext " +
                $"      ,f.Fehlertyp " +
                $"      ,f.Prozedur " +
                $"      ,f.ETL_Fehlermeldungen_ID " +
                $"FROM Logging.ETL_Paketschritt_Prozesslaeufe AS p " +
                $"JOIN pc.ETL_Paketschritte AS s " +
                $"    ON s.ETL_Paketschritte_ID = p.ETL_Paketschritte_ID " +
                $"JOIN Logging.ETL_Fehlermeldungen AS f " +
                $"    ON f.ETL_Prozesslaeufe_ID = p.ETL_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paket_Prozesslaeufe_ID = p.ETL_Paket_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paketumsetzung_Prozesslaeufe_ID = p.ETL_Paketumsetzung_Prozesslaeufe_ID " +
                $"       AND f.ETL_Paketschritt_Prozesslaeufe_ID = p.ETL_Paketschritt_Prozesslaeufe_ID " +
                $"LEFT JOIN Logging.ETL_SQL_Anfragen AS anf " +
                $"    ON anf.ETL_Prozesslaeufe_ID = p.ETL_Prozesslaeufe_ID " +
                $"       AND anf.ETL_Paket_Prozesslaeufe_ID = p.ETL_Paket_Prozesslaeufe_ID " +
                $"       AND anf.ETL_Paketumsetzung_Prozesslaeufe_ID = p.ETL_Paketumsetzung_Prozesslaeufe_ID " +
                $"       AND anf.ETL_Paketschritt_Prozesslaeufe_ID = p.ETL_Paketschritt_Prozesslaeufe_ID " +
                $"       AND anf.ETL_Konfigurationen_ID IS NOT NULL " +
                $"WHERE p.Erfolgreich = 0 AND p.ETL_Prozesslaeufe_ID = {prozesslaeufeID}";
            System.Data.DataTable errormessages = processor.DbHelper.GetDataTableFromQuery(
                                                                        processor, errorQuery, prozesslaeufe);

            string wwwdir = BaseDirectory + @"..\www";

            if (errormessages.Rows.Count == 0)
            {
                return "";
            }
            else
            {
                string htmlTable = File.ReadAllText($"{wwwdir}\\failureTable.txt");

                foreach (DataRow item in errormessages.Rows)
                {
                    string paketRow = File.ReadAllText($"{wwwdir}\\failureTableRow.txt");

                    paketRow = paketRow.Replace("{{PROZESS}}", $"{item["PID"]}");
                    paketRow = paketRow.Replace("{{MODUL}}", $"{item["Modul_ID"]}");
                    paketRow = paketRow.Replace("{{BEFEHL}}", $"{item["Exec_Befehl"] ?? "-"}");
                    paketRow = paketRow.Replace(
                        "{{ANFORDERUNG}}",
                        item["Anforderungszeitpunkt"].ToString() == "" ? "-" :
                        item["Anforderungszeitpunkt"].ToString());
                    paketRow = paketRow.Replace(
                        "{{VON}}",
                        item["Start"].ToString() == "" ? "-" :
                        item["Start"].ToString());
                    paketRow = paketRow.Replace(
                        "{{BIS}}",
                        item["Ende"].ToString() == "" ? "-" :
                        item["Ende"].ToString());

                    if (item["Ende"].ToString() != "")
                    {
                        if (item["Start"].ToString() != "")
                        {
                            double time_m = Math.Round(
                                (DateTime.Parse(item["Ende"].ToString() ?? throw new ETLException("No Ende")) -
                                 DateTime.Parse(item["Start"].ToString() ??
                                        throw new ETLException("No Start"))).TotalMinutes, 0);
                            double time_s = Math.Round(
                                (DateTime.Parse(item["Ende"].ToString() ?? throw new ETLException("No Ende")) -
                                DateTime.Parse(item["Start"].ToString() ??
                                        throw new ETLException("No Start"))).TotalSeconds, 0);
                            paketRow = paketRow.Replace(
                                "{{DAUER}}", time_m > 0 ? $"{time_m} Minuten" : $"{time_s} Sekunden");
                        }
                        else
                        {
                            paketRow = paketRow.Replace("{{DAUER}}", "-");
                        }
                    }
                    else
                    {
                        try
                        {
                            double time_m = Math.Round(
                                (DateTime.Now - DateTime.Parse(item["Start"].ToString() ??
                                        throw new ETLException("No Start"))
                            ).TotalMinutes, 0);
                            double time_s = Math.Round(
                                (DateTime.Now - DateTime.Parse(item["Start"].ToString() ??
                                        throw new ETLException("No Start"))
                            ).TotalSeconds, 0);
                            paketRow = paketRow.Replace(
                                "{{DAUER}}", time_m > 0 ? $"{time_m} Minuten" : $"{time_s} Sekunden");
                        }
                        catch
                        {
                            paketRow = paketRow.Replace("{{DAUER}}", "-");
                        }
                    }

                    paketRow = paketRow.Replace("{{MELDUNGSTEXT}}", item["Meldungstext"].ToString());
                    paketRow = paketRow.Replace(
                        "{{FEHLERTEXT}}",
                        item["Fehlertext"].ToString() == "" ? "-" : item["Fehlertext"].ToString());
                    paketRow = paketRow.Replace("{{FEHLERID}}", item["ETL_Fehlermeldungen_ID"].ToString());

                    htmlTable = htmlTable.Replace("{{DATAROW}}", $"{paketRow}\n" + "{{DATAROW}}");
                }

                htmlTable = htmlTable.Replace("{{DATAROW}}", $"");
                return htmlTable;
            }
        }

        /// <summary>
        /// analyzes realizations and steps executions and converts them to a html string for a html table
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="prozesslaeufeID">ID of overall prozesslauf</param>
        /// <param name="ID">ID of mother prozesslauf</param>
        /// <param name="level">1 for realization | otherwise step</param>
        /// <param name="workflow">workflow object that ended</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>html string for tablerow for executions</returns>
        private string AnalyzeLevel(
            Processor processor,
            int prozesslaeufeID,
            int ID,
            int level,
            Workflow workflow,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            string prevID = level == 1 ? "ETL_Paket_Prozesslaeufe_ID" : "ETL_Paketumsetzung_Prozesslaeufe_ID";
            string collectCMD = $"SELECT * FROM {_htmlTablesSearch[level]} WHERE ETL_Prozesslaeufe_ID = " +
                                $"{prozesslaeufeID} AND {prevID} = {ID}";
            System.Data.DataTable prozesses = processor.DbHelper.GetDataTableFromQuery(
                                                                    processor, collectCMD, prozesslaeufe);

            string wwwdir = BaseDirectory + @"..\www";

            if (level == 1)  // realization level
            {
                string realizationRows = "";

                foreach (DataRow item in prozesses.Rows)
                {
                    string informationCMD = $"SELECT Umsetzungsname AS Information " +
                                            $"FROM pc.ETL_Paket_Umsetzungen WHERE " +
                                            $"ETL_Paket_Umsetzungen_ID = {item["ETL_Paket_Umsetzungen_ID"]} " +
                                            $"AND Ist_aktiv = 1";
                    System.Data.DataTable info = processor.DbHelper.GetDataTableFromQuery(
                                                                        processor, informationCMD, prozesslaeufe);

                    string realizationRow = File.ReadAllText($"{wwwdir}\\resultTableRow.txt");

                    realizationRow = realizationRow.Replace(
                        "{{MODUL}}", $"Umsetzung {item["ETL_Paket_Umsetzungen_ID"]}");
                    realizationRow = realizationRow.Replace("{{INFO}}", info.Rows[0]["Information"].ToString());
                    realizationRow = realizationRow.Replace(
                        "{{VON}}",
                        item["Ausfuehrungsstartzeitpunkt"].ToString() == "" ? "-" :
                        item["Ausfuehrungsstartzeitpunkt"].ToString());
                    realizationRow = realizationRow.Replace(
                        "{{BIS}}",
                        item["Ausfuehrungsendzeitpunkt"].ToString() == "" ? "-" :
                        item["Ausfuehrungsendzeitpunkt"].ToString());
                    if (item["Ausfuehrungsendzeitpunkt"].ToString() != "")
                    {
                        double time_m = Math.Round(
                            (DateTime.Parse(item["Ausfuehrungsendzeitpunkt"].ToString() ??
                                    throw new ETLException("No Ausfuehrungsendzeitpunkt")) -
                            DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                    throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                        ).TotalMinutes, 0);
                        double time_s = Math.Round(
                            (DateTime.Parse(item["Ausfuehrungsendzeitpunkt"].ToString() ??
                                    throw new ETLException("No Ausfuehrungsendzeitpunkt")) -
                            DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                    throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                        ).TotalSeconds, 0);
                        realizationRow = realizationRow.Replace(
                            "{{DAUER}}", time_m > 0 ? $"{time_m} Minuten" : $"{time_s} Sekunden");
                    }
                    else
                    {
                        if (item["Ausfuehrungsstartzeitpunkt"].ToString() != "")
                        {
                            double time_m = Math.Round(
                                (DateTime.Now - DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                        throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                            ).TotalMinutes, 0);
                            double time_s = Math.Round(
                                (DateTime.Now - DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                        throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                            ).TotalSeconds, 0);
                            realizationRow = realizationRow.Replace(
                                "{{DAUER}}", time_m > 0 ? $"{time_m} Minuten" : $"{time_s} Sekunden");
                        }
                        else
                        {
                            realizationRow = realizationRow.Replace("{{DAUER}}", "-");
                        }
                    }
                    realizationRow = realizationRow.Replace("{{SUCC}}", item["Erfolgreich"].ToString());

                    int paketumsetzungProzesslaeufeID = Convert.ToInt32(
                        item["ETL_Paketumsetzung_Prozesslaeufe_ID"].ToString());

                    string stepRows = AnalyzeLevel(
                        processor, prozesslaeufeID, paketumsetzungProzesslaeufeID, 2, workflow, prozesslaeufe);
                    realizationRows += realizationRow + "\n" + stepRows;
                }

                return realizationRows;
            }
            else  // step level
            {
                string stepRows = "";

                foreach (DataRow item in prozesses.Rows)
                {
                    string informationCMD = $"SELECT Schrittname, Zieltabelle, Befehlstyp " +
                                            $"FROM pc.ETL_Paketschritte WHERE " +
                                            $"ETL_Paketschritte_ID = {item["ETL_Paketschritte_ID"]}";
                    System.Data.DataTable info = processor.DbHelper.GetDataTableFromQuery(
                                                                        processor, informationCMD, prozesslaeufe);

                    string stepRow = File.ReadAllText($"{wwwdir}\\resultTableRow.txt");

                    stepRow = stepRow.Replace("{{MODUL}}", $"Schritt {item["ETL_Paketschritte_ID"]}");

                    string table = ReplacePlaceholder(
                        processor,
                        workflow,
                        info.Rows[0]["Zieltabelle"].ToString() ?? throw new ETLException("No Zieltabelle"),
                        prozesslaeufe
                     );

                    stepRow = stepRow.Replace(
                        "{{INFO}}", $"{info.Rows[0]["Schrittname"]}");
                    stepRow = stepRow.Replace(
                        "{{VON}}",
                        item["Ausfuehrungsstartzeitpunkt"].ToString() == "" ? "-" :
                        item["Ausfuehrungsstartzeitpunkt"].ToString());
                    stepRow = stepRow.Replace(
                        "{{BIS}}",
                        item["Ausfuehrungsendzeitpunkt"].ToString() == "" ? "-" :
                        item["Ausfuehrungsendzeitpunkt"].ToString());
                    if (item["Ausfuehrungsendzeitpunkt"].ToString() != "")
                    {
                        double time_m = Math.Round(
                            (DateTime.Parse(item["Ausfuehrungsendzeitpunkt"].ToString() ??
                                    throw new ETLException("No Ausfuehrungsendzeitpunkt")) -
                            DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                    throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                        ).TotalMinutes, 0);
                        double time_s = Math.Round(
                            (DateTime.Parse(item["Ausfuehrungsendzeitpunkt"].ToString() ??
                                    throw new ETLException("No Ausfuehrungsendzeitpunkt")) -
                            DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                    throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                        ).TotalSeconds, 0);
                        stepRow = stepRow.Replace("{{DAUER}}", time_m > 0 ? $"{time_m} Minuten" : $"{time_s} Sekunden");
                    }
                    else
                    {
                        if (item["Ausfuehrungsstartzeitpunkt"].ToString() != "")
                        {
                            double time_m = Math.Round(
                                (DateTime.Now - DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                        throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                            ).TotalMinutes, 0);
                            double time_s = Math.Round(
                                (DateTime.Now - DateTime.Parse(item["Ausfuehrungsstartzeitpunkt"].ToString() ??
                                        throw new ETLException("No Ausfuehrungsstartzeitpunkt"))
                            ).TotalSeconds, 0);
                            stepRow = stepRow.Replace(
                                "{{DAUER}}", time_m > 0 ? $"{time_m} Minuten" : $"{time_s} Sekunden");
                        }
                        else
                        {
                            stepRow = stepRow.Replace("{{DAUER}}", "-");
                        }
                    }
                    stepRow = stepRow.Replace("{{SUCC}}", item["Erfolgreich"].ToString());

                    stepRows += stepRow + "\n";
                }

                return stepRows;
            }
        }

        /// <summary>
        /// recursively goes through aggregated exception and checks if other exception than TaskCanceledException is
        /// included. If so the error is thrown and a message is printed. otherwise nothing happens
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="aggregateException">aggregated exception to analyze</param>
        public void HandleAggregateException(
            Processor processor,
            AggregateException aggregateException
        )
        {
            aggregateException.Handle((inner) =>
            {
                if (inner is AggregateException exception)
                {
                    HandleAggregateException(processor, exception);
                    return true;  // know how to handle
                }
                if (inner is TaskCanceledException)
                    return true;  // know how to handle

                if (inner is ETLException)
                    return true;  // know how to handle

                Task.Run(() =>
                {
                    Log(
                        processor,
                        $"Found unexpected Exception: {inner.GetType()} ({inner.Message} | {inner.StackTrace})",
                        _dummyTuple,
                        type: LogType.Error
                    );
                }).Wait();
                return false;  //  dont know how to hanlde
            });
        }

        /// <summary>
        /// tries releasing a used semaphore, signalizes cancellation to all workflow processes (if able to), handles
        /// the thrown exception (uncatched exception or new?) and returns an empty ETLException (known) or a new
        /// ETLException based on given exxception (unkown)
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="ex">catched exception</param>
        /// <param name="function">calling function name</param>
        /// <param name="message">Error message to print if exception uknown</param>
        /// <param name="locked">semaphore to release (if not null)</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="cancleSource">Cancelation Source to signalize cancelation</param>
        /// <param name="connections">possible list of connections that could be needed to be closed</param>
        /// <returns>empty ETLException if already catched or new ETLException if new error catched</returns>
        /// <exception cref="ETLException">when handling error failed</exception>
        public Exception HandleErrorCatch(
            Processor processor,
            Exception ex,
            string function,
            string message,
            ref SemaphoreSlim locked,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            CancellationTokenSource? cancleSource = null,
            List<DbConnection>? connections = null
        )
        {
            try
            {
                if (connections != null)
                {
                    foreach (DbConnection connection in connections)
                    {
                        if (connection != null && connection.State == ConnectionState.Open)
                            connection.Close();
                    }
                }

                // try releasing lock/semaphore
                try
                {
                    locked?.Release();
                }
                catch (Exception e)
                {
                    Task.Run(() => Log(
                        processor,
                        $"Semaphore could not be released! -> No error! ({function} | {e})",
                        prozesslaeufe,
                        true
                    )).Wait();
                }

                if (cancleSource != null)
                {
                    if (!cancleSource.IsCancellationRequested)
                    {
                        Log(processor, "Announcing cancellation to all processes!", prozesslaeufe);
                        cancleSource.Cancel();
                    }
                }

                // extract exceptions within aggregation
                if (ex is AggregateException exception)
                {
                    try
                    {
                        HandleAggregateException(processor, exception);
                        throw new ETLException();
                    }
                    catch (Exception excep)
                    {
                        if (excep is not TaskCanceledException)
                        {
                            // check if catched exception is already from ETL Service -> otherwise throw new exception
                            if (excep is ETLException)
                            {
                                return excep;
                            }
                            else
                            {
                                return new ETLException(
                                    processor,
                                    message,
                                    function,
                                    excep,
                                    prozesslaeufe
                                );
                            }
                        }
                        else
                        {
                            return new ETLException(
                                processor,
                                message,
                                function,
                                excep,
                                prozesslaeufe
                            );
                        }
                    }
                }
                else
                {
                    if (ex is not TaskCanceledException)
                    {
                        // check if catched exception is already from ETL Service -> otherwise throw new etl exception
                        if (ex is ETLException)
                        {
                            return ex;
                        }
                        else
                        {
                            if (ex is OperationCanceledException)
                                return new ETLException();

                            return new ETLException(
                                processor,
                                message,
                                function,
                                ex,
                                prozesslaeufe
                            );
                        }
                    }
                    else
                    {
                        return new ETLException(
                            processor,
                            message,
                            function,
                            ex,
                            prozesslaeufe
                        );
                    }
                }
            }
            catch (Exception e)
            {
                cancleSource?.Cancel();

                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    $"Handling the catch of an error failed!",
                    "major",
                    e,
                    "HandleErrorCatch",
                    prozesslaeufe
                )).Wait();
                SafeExit(processor, e, "HandleErrorCatch", prozesslaeufe);

                throw new ETLException(
                    processor,
                    "Failed handling error catch!",
                    "HandleErrorCatch",
                    e,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// converts a given list of integers into a string to print out a debug version of given list
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="listname">Displayname of list</param>
        /// <param name="outputlist">list of integers to generate string for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>string with representation of given list to print</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public string GetListString(
            Processor processor,
            string listname,
            List<int> outputlist,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                string output = $"{listname}: ";
                List<int> copyOutputList = [.. outputlist];
                foreach (int wf in copyOutputList)
                {
                    output += $"{wf}, ";
                }

                LogMessageLocal(processor, output, prozesslaeufe);
                return output;
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    $"Logging the list {listname} failed!",
                    "GetListString",
                    e,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// converts a given list of integers into a string and returns it
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="outputlist">list of integers to generate string for</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>string with representation of given list to print</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public static string ConvertListToString(
            Processor processor,
            List<int> outputlist,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            try
            {
                string output = "";
                List<int> copyOutputList = [.. outputlist];
                foreach (int wf in copyOutputList)
                {
                    if (copyOutputList.IndexOf(wf) < copyOutputList.Count - 1)
                    {
                        output += $"{wf}, ";
                    } else
                    {
                        output += $"{wf}";
                    }
                }
                return output;
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    $"Converting List to string failed!",
                    "ConvertListToString",
                    e,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// Writes a given SQL Query to DIZ_NET with possisble configuration (if no configuration target is DIZ_NET)
        /// This enables retrospective analysis of errors and workflows
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="query">SQL Query to write to DB</param>
        /// <param name="confID">ETL_Konfigurationen_ID of realization defining connection</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="debug">true if log is only for debug mode</param>
        public void LogQuery(
            Processor processor,
            string query,
            int confID,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            bool? debug = null
        )
        {
            bool log = true; // always log

            // except a debug value is set
            if (debug != null)
            {
                log = (bool)debug;
                if (!(bool)debug)  // if debug true log -> else do not log
                {
                    query = "DEBUG: " + query;
                }
            }

            int eventID = prozesslaeufe.Item1 != null && prozesslaeufe.Item1 >= 0 ? (int)prozesslaeufe.Item1 : 0;

            if (log)
            {
                DateTime logdate = DateTime.Now;

                // create command to insert log into DB
                string createCommand;
                try
                {
                    string konf = confID == -1 ? "NULL" : confID.ToString();
                    createCommand = $"INSERT INTO {QueryTable} " +
                                    $"(" +
                                    $"     ETL_Prozesslaeufe_ID" +
                                    $"    ,ETL_Paket_Prozesslaeufe_ID" +
                                    $"    ,ETL_Paketumsetzung_Prozesslaeufe_ID" +
                                    $"    ,ETL_Paketschritt_Prozesslaeufe_ID" +
                                    $"    ,Query" +
                                    $"    ,ETL_Konfigurationen_ID" +
                                    $"    ,Anlagedatum" +
                                    $"    ,Anlage_Nutzer" +
                                    $"    ,Letzte_Aenderung" +
                                    $"    ,Letzte_Aenderung_Nutzer" +
                                    $")" +
                                    $"VALUES" +
                                    $"(" +
                                    $"     {(prozesslaeufe.Item1 == null ? "NULL" : prozesslaeufe.Item1.ToString())}" +
                                    $"    ,{(prozesslaeufe.Item2 == null ? "NULL" : prozesslaeufe.Item2.ToString())}" +
                                    $"    ,{(prozesslaeufe.Item3 == null ? "NULL" : prozesslaeufe.Item3.ToString())}" +
                                    $"    ,{(prozesslaeufe.Item4 == null ? "NULL" : prozesslaeufe.Item4.ToString())}" +
                                    $"    ,'{query.Replace("'", "\"")}'" +
                                    $"    ,{konf}" +
                                    $"    ,'{logdate.ToString(DateFormat)}'" +
                                    $"    ,suser_name()" +
                                    $"    ,'{logdate.ToString(DateFormat)}'" +
                                    $"    ,suser_name()" +
                                    $")";
                }
                catch (Exception e)
                {
                    Task.Run(() => ErrorLog(
                        processor,
                        "Dienst",
                        "Error when creating logging row!",
                        "minor",
                        e,
                        "LogQuery",
                        prozesslaeufe
                    )).Wait();
                    return;
                }

                bool success = false;
                int tries = 20;
                string errorLogMessage = "";

                string wwwdir = BaseDirectory + @"..\www";

                // check if log directory exists and create if not
                if (!Directory.Exists($"{wwwdir}\\..\\logs\\"))
                    Directory.CreateDirectory($"{wwwdir}\\..\\logs\\");

                // check if logfile exists and create if not
                if (!File.Exists($"{wwwdir}\\..\\logs\\" + _queryFile + ".log"))
                    File.Create($"{wwwdir}\\..\\logs\\" + _queryFile + ".log").Dispose();

                // try writing log into a local file
                while (!success)
                {
                    if (tries > 0)
                    {
                        // write log into log file
                        try
                        {
                            using (StreamWriter stream = File.AppendText(
                                $"{wwwdir}\\..\\logs\\" + _queryFile + ".log"))
                            {
                                string output = (prozesslaeufe.Item1 == null ? DBNull.Value.ToString() :
                                                    prozesslaeufe.Item1.ToString()) +
                                                "," + (prozesslaeufe.Item2 == null ? DBNull.Value.ToString() :
                                                       prozesslaeufe.Item2.ToString()) +
                                                "," + (prozesslaeufe.Item3 == null ? DBNull.Value.ToString() :
                                                       prozesslaeufe.Item3.ToString()) + "," +
                                                "," + (prozesslaeufe.Item4 == null ? DBNull.Value.ToString() :
                                                       prozesslaeufe.Item4.ToString()) + "," +
                                                "\"" + query.Replace("\"", "\'") + "\"," +
                                                logdate.ToString();
                                stream.WriteLine(
                                    output.Replace(Environment.NewLine, " ")
                                );
                            }
                            success = true;
                            LogMessageLocal(
                                processor, $"Writing Log successfull after {20 - tries} tries!", prozesslaeufe);
                        }
                        catch (Exception e)
                        {
                            tries--;
                            errorLogMessage = e.ToString();
                            Task.Delay(500).Wait();
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                if (success == false)
                    Task.Run(() => ErrorLog(
                        processor,
                        "Dienst",
                        $"Error writing log to log file after 20 tries! " +
                        $"({$"{wwwdir}\\..\\logs\\" + _queryFile}.log ({errorLogMessage}))",
                        "minor",
                        null,
                        "LogQuery",
                        prozesslaeufe
                    )).Wait();

                // write log to Server
                try
                {
                    processor.DbHelper.ExecuteCommandDIZ(processor, createCommand, prozesslaeufe);
                }
                catch (Exception e)
                {
                    LogMessageLocal(processor, "Writing LOG to DB Failed", prozesslaeufe, LogType.Error);
                    LogMessageLocal(processor, createCommand, prozesslaeufe, LogType.Error);
                    Task.Run(() => ErrorLog(
                        processor,
                        "Dienst",
                        "Writing log to DB failed!",
                        "minor",
                        e,
                        "LogQuery",
                        prozesslaeufe
                    )).Wait();
                }
            }
        }

        public void LogMessageLocal(
            Processor processor,
            string message,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            LogType type = LogType.Info
        )
        {
            try
            {
                switch (type)
                {
                    case LogType.Info:
                        Serilog.Log.Information(
                            message,
                            prozesslaeufe.Item1 != null && prozesslaeufe.Item1 >= 0 ? (int)prozesslaeufe.Item1 : 0
                        );
                        break;
                    case LogType.Warning:
                        Serilog.Log.Warning(
                            message,
                            prozesslaeufe.Item1 != null && prozesslaeufe.Item1 >= 0 ? (int)prozesslaeufe.Item1 : 0
                        );
                        break;
                    case LogType.Error:
                        Serilog.Log.Error(
                            message,
                            prozesslaeufe.Item1 != null && prozesslaeufe.Item1 >= 0 ? (int)prozesslaeufe.Item1 : 0
                        );
                        break;
                    case LogType.Fatal:
                        Serilog.Log.Fatal(
                            message,
                            prozesslaeufe.Item1 != null && prozesslaeufe.Item1 >= 0 ? (int)prozesslaeufe.Item1 : 0
                        );
                        break;
                    default:
                        Serilog.Log.Information(
                            message,
                            prozesslaeufe.Item1 != null && prozesslaeufe.Item1 >= 0 ? (int)prozesslaeufe.Item1 : 0
                        );
                        break;
                }
            } catch (Exception e)
            {
                SemaphoreSlim tmp = DummySem ?? new SemaphoreSlim(1, 1);
                throw HandleErrorCatch(
                    processor,
                    e,
                    "LogMessageLocal",
                    $"Failed logging message to local system!",
                    ref tmp,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// function to log what happens when executing the workflos into the SQL Server
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="message">The logging message to write</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="debug">true if log is only for debug mode</param>
        /// <param name="type">type of log for Windows EventLog</param>
        public void Log(
            Processor processor,
            string message,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            bool? debug = null,
            LogType type = LogType.Info
        )
        {
            bool log = true; // always log

            // except a debug value is set
            if (debug != null)
            {
                log = (bool)debug;
                if (!(bool)debug)  // if debug true log -> else do not log
                    message = "DEBUG: " + message;

                //Log(eventLog, $"Logging with debug value {debug} which sets logging execution {log}", null);
            }

            int eventID = prozesslaeufe.Item1 != null && prozesslaeufe.Item1 >= 0 ? (int)prozesslaeufe.Item1 : 0;

            if (log)
            {
                DateTime logdate = DateTime.Now;

                // generate the json for this log
                string json;
                string cleanMessage = message.Replace("\\", "\\\\").Replace("\n", " ").Replace("\t", " ").Replace(
                                                      "\r", " ").Replace("\"", "\\\"").Replace("'", "\\\"");
                try
                {
                    json = "{" +
                        (prozesslaeufe.Item1 != null ? $"\"ETL_Prozesslaeufe_ID\": {prozesslaeufe.Item1}, " : "") +
                        (prozesslaeufe.Item2 != null ?
                            $"\"ETL_Paket_Prozesslaeufe_ID\": {prozesslaeufe.Item2}, " : "")+
                        (prozesslaeufe.Item3 != null ? "\"ETL_Paketumsetzung_Prozesslaeufe_ID\": " +
                            $"{prozesslaeufe.Item3}, " : "") +
                        (prozesslaeufe.Item4 != null ? "\"ETL_Paketschritt_Prozesslaeufe_ID\": " +
                            $"{prozesslaeufe.Item4}, " : "") +
                        $"\"Meldungstext\": \"{cleanMessage}\", " +
                        $"\"Anlagedatum\": \"{logdate.ToString(JsonDateFormat)}\"" +
                    "}";
                }
                catch
                {
                    json = "{}";
                }

                // write log to eventLog as json
                try
                {
                    LogMessageLocal(processor, json, prozesslaeufe, type);
                }
                catch (Exception e)
                {
                    Task.Run(() => ErrorLog(
                        processor,
                        "Dienst",
                        "Writing log into eventLog failed!",
                        "minor",
                        e,
                        "Log",
                        prozesslaeufe
                    )).Wait();
                }

                // create command to insert log into DB
                string createCommand;
                try
                {
                    createCommand = $"INSERT INTO {LoggingTable} " +
                                    $"(" +
                                    $"    [ETL_Prozesslaeufe_ID]" +
                                    $"    ,[ETL_Paket_Prozesslaeufe_ID]" +
                                    $"    ,[ETL_Paketumsetzung_Prozesslaeufe_ID]" +
                                    $"    ,[ETL_Paketschritt_Prozesslaeufe_ID]" +
                                    $"    ,[Meldungstext]" +
                                    $"    ,[Ist_transferiert]" +
                                    $"    ,[Json_Log]" +
                                    $"    ,[Anlagedatum]" +
                                    $"    ,[Anlage_Nutzer]" +
                                    $"    ,[Letzte_Aenderung]" +
                                    $"    ,[Letzte_Aenderung_Nutzer]" +
                                    $")" +
                                    $"VALUES" +
                                    $"(" +
                                    $"     {(prozesslaeufe.Item1 == null ? "NULL" : prozesslaeufe.Item1.ToString())}" +
                                    $"    ,{(prozesslaeufe.Item2 == null ? "NULL" : prozesslaeufe.Item2.ToString())}" +
                                    $"    ,{(prozesslaeufe.Item3 == null ? "NULL" : prozesslaeufe.Item3.ToString())}" +
                                    $"    ,{(prozesslaeufe.Item4 == null ? "NULL" : prozesslaeufe.Item4.ToString())}" +
                                    $"    ,'{message.Replace("'", "\"")}'" +
                                    $"    ,0" +
                                    $"    ,'{json}'" +
                                    $"    ,'{logdate.ToString(DateFormat)}'" +
                                    $"    ,suser_name()" +
                                    $"    ,'{logdate.ToString(DateFormat)}'" +
                                    $"    ,suser_name()" +
                                    $")";
                }
                catch (Exception e)
                {
                    Task.Run(() => ErrorLog(
                        processor,
                        "Dienst",
                        "Error when creating logging row!",
                        "minor",
                        e,
                        "Log",
                        prozesslaeufe
                    )).Wait();
                    return;
                }

                bool success = false;
                int tries = 20;
                string errorLogMessage = "";

                string wwwdir = BaseDirectory + @"..\www";

                // check if log directory exists and create if not
                if (!Directory.Exists($"{wwwdir}\\..\\logs\\"))
                    Directory.CreateDirectory($"{wwwdir}\\..\\logs\\");

                // check if logfile exists and create if not
                if (!File.Exists($"{wwwdir}\\..\\logs\\" + _loggingFile + ".log"))
                    File.Create($"{wwwdir}\\..\\logs\\" + _loggingFile + ".log").Dispose();

                // try writing log into a local file
                while (!success)
                {
                    if (tries > 0)
                    {
                        // write log into log file
                        try
                        {
                            using (StreamWriter stream = File.AppendText(
                                $"{wwwdir}\\..\\logs\\" + _loggingFile + ".log"))
                            {
                                string output = (prozesslaeufe.Item1 == null ? DBNull.Value.ToString() :
                                                    prozesslaeufe.Item1.ToString()) +
                                                "," + (prozesslaeufe.Item2 == null ? DBNull.Value.ToString() :
                                                       prozesslaeufe.Item2.ToString()) +
                                                "," + (prozesslaeufe.Item3 == null ? DBNull.Value.ToString() :
                                                       prozesslaeufe.Item3.ToString()) + "," +
                                                "," + (prozesslaeufe.Item4 == null ? DBNull.Value.ToString() :
                                                       prozesslaeufe.Item4.ToString()) + "," +
                                                "\"" + message.Replace("\"", "\'") + "\"," +
                                                logdate.ToString();
                                stream.WriteLine(
                                    output.Replace(Environment.NewLine, " ")
                                );
                            }
                            success = true;
                            LogMessageLocal(processor, $"Writing Log successfull after {20 - tries} tries!", prozesslaeufe);
                        }
                        catch (Exception e)
                        {
                            tries--;
                            errorLogMessage = e.ToString();
                            Task.Delay(500).Wait();
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                if (success == false)
                    Task.Run(() => ErrorLog(
                        processor,
                        "Dienst",
                        $"Error writing log to log file after 20 tries! " +
                        $"({$"{wwwdir}\\..\\logs\\" + _loggingFile}.log ({errorLogMessage}))",
                        "minor",
                        null,
                        "Log",
                        prozesslaeufe
                    )).Wait();

                // write log to Server
                try
                {
                    processor.DbHelper.ExecuteCommandDIZ(processor, createCommand, prozesslaeufe);
                }
                catch (Exception e)
                {
                    LogMessageLocal(processor, "Writing LOG to DB Failed", prozesslaeufe, LogType.Error);
                    LogMessageLocal(processor, createCommand, prozesslaeufe, LogType.Error);
                    Task.Run(() => ErrorLog(
                        processor,
                        "Dienst",
                        "Writing log to DB failed!",
                        "minor",
                        e,
                        "Log",
                        prozesslaeufe
                    )).Wait();
                }
            }
        }

        /// <summary>
        /// Logs an error into file, system logging and SQL Server
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="fehlertyp">Dienst, SQL or Workflow</param>
        /// <param name="message">Errortext</param>
        /// <param name="schweregrad">minor or major</param>
        /// <param name="e">catched exception (optional)</param>
        /// <param name="prozedur">name of procedure that catched the error (optional)</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="fehlernummer">results from SQL Procedure (optional)</param>
        /// <param name="fehlerzeile">results from SQL Procedure (optional)</param>
        /// <param name="fehlerquelle">source of exception but can optionally be added manually (optional)</param>
        /// <param name="fehlerstatus">results from SQL Procedure (optional)</param>
        /// <param name="fehlerobjekt">Data ID to investigate error element (optional)</param>
        public void ErrorLog(
            Processor processor,
            string fehlertyp,
            string message,
            string schweregrad,
            Exception? e,
            string prozedur,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            int? fehlernummer = null,
            int? fehlerzeile = null,
            string fehlerquelle = "",
            string fehlerstatus = "",
            string fehlerobjekt = ""
        )
        {
            DateTime logdate = DateTime.Now;

            string wwwdir = BaseDirectory + @"..\www";

            // generate JSON for error log
            string json;
            try
            {
                string fehlerq = fehlerquelle != "" ? fehlerquelle :
                                 (e != null ? (e.Source != null ?
                                 $"\"Fehlerquelle\": \"{e.Source}\", " : "") : "");

                json = "{" +
                    (prozesslaeufe.Item1 != null ? $"\"ETL_Prozesslaeufe_ID\": {prozesslaeufe.Item1}, " : "") +
                    (prozesslaeufe.Item2 != null ? $"\"ETL_Paket_Prozesslaeufe_ID\": {prozesslaeufe.Item2}, " : "") +
                    (prozesslaeufe.Item3 != null ? "\"ETL_Paketumsetzung_Prozesslaeufe_ID\": " +
                        $"{prozesslaeufe.Item3}, " : "") +
                    (prozesslaeufe.Item4 != null ? "\"ETL_Paketschritt_Prozesslaeufe_ID\": " +
                        $"{prozesslaeufe.Item4}, " : "") +
                    $"\"Fehlertyp\": \"{fehlertyp}\", " +
                    "\"Meldungstext\": \"" + message.Replace("\\", "\\\\").Replace(
                        "\n", " ").Replace("\r", " ").Replace("'", "\\\"") + "\", " +
                    (e != null ? (e.Message != null ? "\"Fehlertext\": \"" +
                        e.Message.ToString().Replace("\\", "\\\\").Replace("\n", " ").Replace(
                            "\r", " ").Replace("\"", "\\\"").Replace("'", "\\\"") + "\", " : "") : "") +
                    (fehlernummer != null ? $"\"Fehlernummer\": {fehlernummer}, " : "") +
                    (fehlerzeile != null ? $"\"Fehlerzeile\": {fehlerzeile}, " : "") +
                    $"\"Schweregrad\": \"{schweregrad}\", " +
                    (e != null ? (e.StackTrace != null ? "\"Stacktrace\": \"" +
                        e.StackTrace.ToString().Replace("\\", "\\\\").Replace("\n", " ").Replace("\r", " ").Replace(
                            "'", "\\\"") + "\", " : "") : "") +
                    (prozedur != "" ? $"\"Prozedur\": \"{prozedur}\", " : "") +
                    fehlerq +
                    (fehlerstatus != "" ? $"\"Fehlerstatus\": \"{fehlerstatus}\", " : "") +
                    (fehlerobjekt != "" ? $"\"Fehlerobjekt\": \"{fehlerobjekt}\", " : "") +
                    $"\"Anlagedatum\": \"{logdate.ToString(JsonDateFormat)}\"" +
                "}";
            }
            catch
            {
                json = "{}";
            }

            // write log to eventLog as JSON
            try
            {
                LogMessageLocal(processor, json, prozesslaeufe, LogType.Error);
            }
            catch (Exception ex)
            {
                LogMessageLocal(
                    processor,
                    $"!!! ERROR: writing ErrorLog into eventLog failed !!!\n\n{ex}",
                    prozesslaeufe,
                    LogType.Error
                );
            }

            bool success = false;
            int tries = 10;

            // check if log directory exists and create if not
            if (!Directory.Exists($"{wwwdir}\\..\\logs\\"))
                Directory.CreateDirectory($"{wwwdir}\\..\\logs\\");

            // check if logfile exists and create if not
            if (!File.Exists($"{wwwdir}\\..\\logs\\" + _errorLoggingFile))
                File.Create($"{wwwdir}\\..\\logs\\" + _errorLoggingFile).Dispose();

            // try writing error log to local file
            while (!success)
            {
                if (tries > 0)
                {
                    // write log into log file
                    try
                    {
                        using (StreamWriter stream = File.AppendText(
                            $"{wwwdir}\\..\\logs\\" + _errorLoggingFile))
                        {
                            stream.WriteLine(
                                (prozesslaeufe.Item1 == null ? DBNull.Value.ToString() :
                                    prozesslaeufe.Item1.ToString()) + "," +
                                (prozesslaeufe.Item2 == null ? DBNull.Value.ToString() :
                                    prozesslaeufe.Item2.ToString()) + "," +
                                (prozesslaeufe.Item3 == null ? DBNull.Value.ToString() :
                                    prozesslaeufe.Item3.ToString()) + "," +
                                (prozesslaeufe.Item4 == null ? DBNull.Value.ToString() :
                                    prozesslaeufe.Item4.ToString()) + "," +
                                $"{fehlertyp}," +
                                $"{message}," +
                                (e != null ? (e.Message != null ? fehlerzeile.ToString() : "") : "") + "," +
                                (fehlernummer == null ? DBNull.Value.ToString() : fehlernummer.ToString()) + "," +
                                (fehlerzeile == null ? DBNull.Value.ToString() : fehlerzeile.ToString()) + "," +
                                $"{schweregrad}," +
                                (e != null ? (e.StackTrace ?? DBNull.Value.ToString()) : "") + "," +
                                $"{prozedur}," +
                                $"{fehlerquelle}," +
                                $"{fehlerstatus}," +
                                $"{fehlerobjekt}," +
                                logdate.ToString()
                            );
                        }
                        success = true;
                        LogMessageLocal(processor, $"Writing Log successfull after {10 - tries} tries!", prozesslaeufe);
                    }
                    catch
                    {
                        tries--;
                    }
                }
                else
                {
                    break;
                }
            }

            if (success == false)
                LogMessageLocal(
                    processor, "!!! ERROR: Writing Error to Logfile failed !!!", prozesslaeufe, LogType.Error);

            // create error log insert command
            string createCommand = "";
            string commandMessage;
            if (e != null)
            {
                if (e.Message == null)
                {
                    commandMessage = ",NULL";
                }
                else
                {
                    commandMessage = $",'{e.Message.Replace("'", "\"")} ";
                    if (e != e.GetBaseException())
                    {
                        commandMessage += $"({e.GetBaseException().Message.Replace("'", "\"")})'";
                    } else
                    {
                        commandMessage += "'";
                    }
                }
            }
            else
            {
                commandMessage = ",NULL";
            }
            try
            {
                createCommand += $"INSERT INTO {ErrorLoggingTable}";
                createCommand += $"(";
                createCommand += $"     ETL_Prozesslaeufe_ID";
                createCommand += $"    ,ETL_Paket_Prozesslaeufe_ID";
                createCommand += $"    ,ETL_Paketumsetzung_Prozesslaeufe_ID";
                createCommand += $"    ,ETL_Paketschritt_Prozesslaeufe_ID";
                createCommand += $"    ,Fehlertyp";
                createCommand += $"    ,Meldungstext";
                createCommand += $"    ,Fehlertext";
                createCommand += $"    ,Fehlernummer";
                createCommand += $"    ,Fehlerzeile";
                createCommand += $"    ,Schweregrad";
                createCommand += $"    ,Stacktrace";
                createCommand += $"    ,Prozedur";
                createCommand += $"    ,Fehlerquelle";
                createCommand += $"    ,Fehlerstatus";
                createCommand += $"    ,Fehlerobjekt";
                createCommand += $"    ,Ist_transferiert";
                createCommand += $"    ,Json_Log";
                createCommand += $"    ,Anlagedatum";
                createCommand += $"    ,Anlage_Nutzer";
                createCommand += $"    ,Letzte_Aenderung";
                createCommand += $"    ,Letzte_Aenderung_Nutzer";
                createCommand += $")";
                createCommand += $"VALUES";
                createCommand += $"(";
                createCommand += $"     " + (prozesslaeufe.Item1 == null ? "NULL" : prozesslaeufe.Item1.ToString());
                createCommand += $"    ," + (prozesslaeufe.Item2 == null ? "NULL" : prozesslaeufe.Item2.ToString());
                createCommand += $"    ," + (prozesslaeufe.Item3 == null ? "NULL" : prozesslaeufe.Item3.ToString());
                createCommand += $"    ," + (prozesslaeufe.Item4 == null ? "NULL" : prozesslaeufe.Item4.ToString());
                createCommand += $"    ,'{fehlertyp}'";
                createCommand += $"    ,'{message.Replace("'", "\"")}'";
                createCommand += $"    {commandMessage}";
                createCommand += (fehlernummer != null ? $",'{fehlernummer}'" : ",NULL");
                createCommand += (fehlerzeile != null ? $",'{fehlerzeile}'" : ",NULL");
                createCommand += $",'{schweregrad}'";
                createCommand += (e != null ? (e.StackTrace == null ? ",NULL" : ($",'{e.StackTrace}'")) : ",NULL");
                createCommand += (prozedur != "" ? ($",'{prozedur}'") : ",NULL");
                createCommand += (e != null ? (e.Source == null ? ",NULL" : ($",'{e.Source}'")) : ",NULL");
                createCommand += (fehlerstatus != "" ? ($",'{fehlerstatus}'") : ",NULL");
                createCommand += (fehlerobjekt != "" ? ($",'{fehlerobjekt}'") : ",NULL");
                createCommand += ",0";
                createCommand += $",'{json}'";
                createCommand += $",'{logdate.ToString(DateFormat)}'";
                createCommand += ",suser_name()";
                createCommand += $",'{logdate.ToString(DateFormat)}'";
                createCommand += ",suser_name()";
                createCommand += ")";
            }
            catch (Exception ex)
            {
                LogMessageLocal(
                    processor,
                    $"!!! ERROR: Creating Error Log Command failed !!!\n\n{ex}",
                    prozesslaeufe,
                    LogType.Error
                );
                return;
            }

            // write log to Server
            try
            {
                processor.DbHelper.ExecuteCommandDIZ(processor, createCommand, prozesslaeufe);
            }
            catch (Exception ex)
            {
                LogMessageLocal(
                    processor, $"!!! ERROR: Writing Error to Server failed !!!\n\n{ex}", prozesslaeufe, LogType.Error);
                LogMessageLocal(processor, createCommand, prozesslaeufe, LogType.Error);
                return;
            }
        }

        /// <summary>
        /// converts a list of Workflows to a string to represent workflows as list of IDs and print them to system log
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="list">list of Workflows to convert</param>
        /// <param name="name">displayname of list</param>
        public void OutputList(Processor processor, List<Workflow> list, string name)
        {
            string liste = $"{name}: ";

            foreach (Workflow item in list)
            {
                liste += $"{item.GetID()}, ";
            }

            LogMessageLocal(processor, "DEBUG: " + liste, _dummyTuple);
            Task.Run(() => Log(processor, liste, _dummyTuple)).Wait();
        }

        /// <summary>
        /// converts a dictionary of int keys and int values to a string to log and returns it
        /// </summary>
        /// <param name="map">Dictionary to convert</param>
        /// <param name="name">display name of map</param>
        /// <returns>string containing the converted dictionary</returns>
        public static string GetMappingString(Dictionary<int, int> map, string name)
        {
            string liste = $"{name}: ";

            foreach (var item in map)
            {
                liste += $"{item.Key}: {item.Value}, ";
            }

            return liste;
        }

        /// <summary>
        /// writes the initial Log into the DB with many NULL values that can be updated later
        /// (one of the prozesslaeufe tables)
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="table">tablename to write Log into</param>
        /// <param name="ids">list of mappings of ID columns with their name and value</param>
        /// <param name="anforderungszeitpunkt">DateTime of initialization of thing to log</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>ID if creation was successfull or -1 if any error occurs</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public int InitializeLogging(
            Processor processor,
            string table,
            List<Tuple<string, int>> ids,
            DateTime anforderungszeitpunkt,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            int eventID = prozesslaeufe.Item1 != null ? (int)prozesslaeufe.Item1 : 0;

            // open connection to DB
            using SqlConnection connection = new(processor.DbHelper.BaseConnectionsString.ConnectionString);
            connection.Open();

            DateTime now = DateTime.Now;

            string columnNames = "";
            string columnValues = "";
            string jsonIDs = "";

            // read the column names, values and generate the json part of the IDs
            try
            {
                foreach (Tuple<string, int> mapping in ids)
                {
                    columnNames += $",[{mapping.Item1}]";
                    columnValues += $",{mapping.Item2}";
                    jsonIDs += $"\"{mapping.Item1}\": {mapping.Item2},";
                }
            }
            catch (Exception e)
            {
                connection.Close();

                throw new ETLException(
                    processor,
                    "Extracting column names and values and json failed!",
                    "InitializeLogging",
                    e,
                    prozesslaeufe
                );
            }

            // read the writing DB user
            string userName;
            try
            {
                SqlCommand user = new("SELECT suser_name();", connection);
                userName = user.ExecuteScalar()?.ToString() ?? "";
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    "Extracting the user name failed!",
                    "minor",
                    e,
                    "InitializeLogging",
                    prozesslaeufe
                )).Wait();
                userName = "UNKNOWN";
            }

            // generate the json string of the Log
            string json;
            try
            {
                json = "{" +
                        jsonIDs +
                        $"\"Anforderungszeitpunkt\": \"{anforderungszeitpunkt.ToString(JsonDateFormat)}\"," +
                        $"\"Ist_gestartet\": 0," +
                        $"\"Ist_abgeschlossen\": 0," +
                        $"\"Ist_transferiert\": 0," +
                        $"\"Anlagedatum\": \"{now.ToString(JsonDateFormat)}\"," +
                        $"\"Anlage_Nutzer\": \"{userName}\"," +
                        $"\"Letzte_Aenderung\": \"{now.ToString(JsonDateFormat)}\"," +
                        $"\"Letzte_Aenderung_Nutzer\": \"{userName}\"" +
                    "}";
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    "Generating Log JSON failed!",
                    "minor",
                    e,
                    "InitializeLogging",
                    prozesslaeufe
                )).Wait();
                json = "{}";
            }

            // generate the inserting SQL command
            string insertCommand;
            try
            {
                insertCommand = $"INSERT INTO {table}" +
                                $"(" +
                                $"     Anforderungszeitpunkt" +
                                $"    ,Startzeitpunkt" +
                                $"    ,Endzeitpunkt" +
                                $"    ,Ist_gestartet" +
                                $"    ,Ist_abgeschlossen" +
                                $"    ,Ist_transferiert" +
                                $"    ,Json_Log" +
                                $"    ,Anlagedatum" +
                                $"    ,Anlage_Nutzer" +
                                $"    ,Letzte_Aenderung" +
                                $"    ,Letzte_Aenderung_Nutzer" +
                                $"    {columnNames}" +  // ids
                                $")" +
                                $"VALUES" +
                                $"(" +
                                $"    '{anforderungszeitpunkt.ToString(DateFormat)}'" +
                                $"    ,NULL" +
                                $"    ,NULL" +
                                $"    ,0" +
                                $"    ,0" +
                                $"    ,0" +
                                $"    ,'{json}'" +
                                $"    ,'{now.ToString(DateFormat)}'" +
                                $"    ,'{userName}'" +
                                $"    ,'{now.ToString(DateFormat)}'" +
                                $"    ,'{userName}'" +
                                $"    {columnValues}" +  // ids
                                ") " +
                                "SELECT SCOPE_IDENTITY()";
            }
            catch (Exception e)
            {
                connection.Close();

                throw new ETLException(
                    processor,
                    "Generating the SQL Command failed",
                    "InitializeLogging",
                    e,
                    prozesslaeufe
                );
            }

            // insert data into Logging table
            int id;
            try
            {
                SqlCommand cmd = new(insertCommand, connection);
                // insert data into DB and get the automatic ID
                id = int.Parse(cmd.ExecuteScalar().ToString() ?? throw new ETLException("No commandresult"));
            }
            catch (Exception e)
            {
                connection.Close();

                LogMessageLocal(processor, insertCommand, prozesslaeufe);

                throw new ETLException(
                    processor,
                    "Inserting Log into DB failed!",
                    "InitializeLogging",
                    e,
                    prozesslaeufe
                );
            }

            return id;
        }

        /// <summary>
        /// Updates given attributes in a given logging table based on unique identification columns
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="table">tablename to write Log into</param>
        /// <param name="id">ID of prozesslaeufe table to updatae</param>
        /// <param name="idName">column name of ID</param>
        /// <param name="changes">List of changes (columnname, new value)</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>0 if no errors occured and -1 if any error occured</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public void UpdateLog(
            Processor processor,
            string table,
            int id,
            string idName,
            List<Tuple<string, object>> changes,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            // check if any updates need to be written
            if (changes.Count == 0)
            {
                LogMessageLocal(processor, "[UpdateLog] No changes to update in DB!", prozesslaeufe);
            }
            else
            {
                using SqlConnection connection = new(processor.DbHelper.BaseConnectionsString.ConnectionString);
                connection.Open();

                // generate the statements to change items in DB
                string changeStatement;
                try
                {
                    changeStatement = GetChangeStatement(processor, changes, prozesslaeufe);
                }
                catch (Exception e)
                {
                    if (e is ETLException)
                    {
                        throw new ETLException();
                    }
                    else
                    {
                        throw new ETLException(
                            processor,
                            "Creating Change Statement failed",
                            "UpdateLog",
                            e,
                            prozesslaeufe
                        );
                    }
                }

                // generate the SQL update statement
                string command;
                try
                {
                    command = $"UPDATE {table} " +
                              $"SET {changeStatement} " +
                              $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                              $"Letzte_Aenderung_Nutzer = suser_name() " +
                              $"WHERE {idName} = {id}";
                }
                catch (Exception e)
                {
                    throw new ETLException(
                        processor,
                        "Generating the SQL update statement failed!",
                        "UpdateLog",
                        e,
                        prozesslaeufe
                    );
                }

                // execute the update command
                try
                {
                    SqlCommand cmd = new(command, connection);
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    throw new ETLException(
                        processor,
                        "Executing the update command failed!",
                        "UpdateLog",
                        e,
                        prozesslaeufe
                    );
                }

                // read the updated line from DB
                System.Data.DataTable updated;
                string getTableRows = $"SELECT * FROM {table} WHERE {idName} = {id};";
                try
                {
                    updated = new System.Data.DataTable("updated");
                    SqlDataAdapter adapter = new(getTableRows, connection);
                    adapter.Fill(updated);  // should only fill one line
                }
                catch (Exception e)
                {
                    throw new ETLException(
                        processor,
                        "Reading the updated line from DB",
                        "UpdateLog",
                        e,
                        prozesslaeufe
                    );
                }

                // read the column names of logging table
                string[] columnNames;
                try
                {
                    columnNames = updated.Columns.Cast<DataColumn>()
                                             .Select(x => x.ColumnName)
                                             .ToArray();
                }
                catch (Exception e)
                {
                    throw new ETLException(
                        processor,
                        "Reading table columns failed!",
                        "UpdateLog",
                        e,
                        prozesslaeufe
                    );
                }

                // generate the new Json_Log
                string updateJson;
                try
                {
                    updateJson = GenerateJson(processor, updated.Rows[0], columnNames, prozesslaeufe);
                }
                catch (Exception e)
                {
                    if (e is ETLException)
                    {
                        throw;
                    }
                    else
                    {
                        throw new ETLException(
                            processor,
                            $"Generating the updated Json Log failed ({getTableRows})",
                            "UpdateLog",
                            e,
                            prozesslaeufe
                        );
                    }
                }

                // generate the SQL update command for updating the JSON entry
                string updateCommand;
                try
                {
                    updateCommand = $"UPDATE {table} " +
                                    $"SET Json_Log = {updateJson} " +
                                    $"WHERE {idName} = {id};";
                }
                catch (Exception e)
                {
                    throw new ETLException(
                        processor,
                        "Generating the SQL update command failed!",
                        "UpdateLog",
                        e,
                        prozesslaeufe
                    );
                }

                // execute the update command
                try
                {
                    SqlCommand updateCMD = new(updateCommand, connection);
                    updateCMD.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    throw new ETLException(
                        processor,
                        "Executing the update command failed!",
                        "UpdateLog",
                        e,
                        prozesslaeufe
                    );
                }
            }
        }

        /// <summary>
        /// Generates a string that matches the Json format and adds the needed columns from logging table
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="update">DataRow that includes tha updated data</param>
        /// <param name="columnNames">Name of columns to log (should match those of update</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>json string with updated data</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private static string GenerateJson(
            Processor processor,
            DataRow update,
            string[] columnNames,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            string updateJson = "'{";
            try
            {
                int updateCounter = 0;
                foreach (string column in columnNames)
                {
                    if (column != "Json_Log")
                    {
                        string colType = update[column].GetType().ToString();
                        var value = update[column];

                        string valString = "";

                        valString = colType switch
                        {
                            "System.Int64" => value.ToString() ?? "",
                            "System.Boolean" => (bool)value == true ? "1" : "0",
                            "System.DateTime" => $"\"{((DateTime)value).ToString(JsonDateFormat)}\"",
                            _ => $"\"{value}\"",
                        };
                        if (updateCounter == columnNames.Length - 1)
                        {
                            updateJson += $"\"{column}\": {valString}";
                        }
                        else
                        {
                            updateJson += $"\"{column}\": {valString},";
                        }
                    }

                    updateCounter++;
                }
                updateJson += "}'";

                return updateJson;
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    "Generating the Json Log failed",
                    "GenerateJson",
                    e,
                    prozesslaeufe
                );
            }
        }

        /// <summary>
        /// add an entry to Zeitplan_Ausfuehrung and return the ID of the new process
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="timeplanID">ID of timeplan of Workflow</param>
        /// <param name="workflowID">ID of workflow</param>
        /// <param name="paketeID">ID of master package of workflow</param>
        /// <param name="anforderungszeitpunkt">time of request</param>
        /// <param name="datasourceID">ID of data source</param>
        /// <returns>Adds an entry to DB and returns the automatic ID or -1 if any error occurs</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        public static int AddZeitplanAusfuehrung(
            Processor processor,
            int timeplanID,
            int workflowID,
            int paketeID,
            DateTime anforderungszeitpunkt,
            int datasourceID
        )
        {
            // open connection to DB
            using SqlConnection connection = new(processor.DbHelper.BaseConnectionsString.ConnectionString);
            connection.Open();

            // generate SQL insert command
            DateTime now = DateTime.Now;
            string command;
            try
            {
                command = $"INSERT INTO pc.ETL_Zeitplan_Ausfuehrungen" +
                          $"(" +
                          $"     ETL_Zeitplaene_ID" +
                          $"    ,ETL_Workflow_ID" +
                          $"    ,ETL_Pakete_ID" +
                          $"    ,Anforderungszeitpunkt" +
                          $"    ,Datenherkunft_ID" +
                          $"    ,Anlagedatum" +
                          $"    ,Anlage_Nutzer" +
                          $"    ,Letzte_Aenderung" +
                          $"    ,Letzte_Aenderung_Nutzer" +
                          $")" +
                          $"VALUES" +
                          $"(" +
                          $"     {timeplanID}" +
                          $"    ,{workflowID}" +
                          $"    ,{paketeID}" +
                          $"    ,'{anforderungszeitpunkt.ToString(DateFormat)}'" +
                          $"    ,{datasourceID}" +
                          $"    ,'{now.ToString(DateFormat)}'" +
                          $"    ,suser_name()" +
                          $"    ,'{now.ToString(DateFormat)}'" +
                          $"    ,suser_name()" +
                          $") " +
                          $"SELECT SCOPE_IDENTITY()";
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    "Generating the SQL Command failed",
                    "AddZeitplanAusfuehrung",
                    e
                );
            }

            // insert data into Logging table and get ID
            int id;
            try
            {
                SqlCommand cmd = new(command, connection);
                // insert data into DB and get the automatic ID
                id = int.Parse(cmd.ExecuteScalar().ToString() ?? throw new ETLException("No commandresult"));
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    "Inserting Log into DB failed!",
                    "AddZeitplanAusfuehrung",
                    e
                );
            }

            return id;
        }

        /// <summary>
        /// updates a given timeplan execution in DB -> automatically updates all necessary attributes
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="id">ETL_Zeitplan_Ausfuehrungen_ID</param>
        /// <param name="changes">List of changes to make on given entry</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <exception cref="ETLException">in case of any error</exception>
        public void UpdateZeitplanAusfuehrung(
            Processor processor,
            int id,
            List<Tuple<string, object>> changes,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            // check if any changes need to be written
            if (changes.Count == 0)
            {
                LogMessageLocal(processor, "[UpdateLog] No changes to update in DB!", prozesslaeufe);
            }
            else
            {
                using SqlConnection connection = new(processor.DbHelper.BaseConnectionsString.ConnectionString);
                connection.Open();

                string changeStatement;
                try
                {
                    changeStatement = GetChangeStatement(processor, changes, prozesslaeufe);
                }
                catch (Exception e)
                {
                    if (e is ETLException)
                    {
                        throw new ETLException();
                    }
                    else
                    {
                        throw new ETLException(
                            processor,
                            "Creating Change Statement failed",
                            "UpdateZeitplanAusfuehrung",
                            e,
                            prozesslaeufe
                        );
                    }
                }

                // generate the SQL update statement
                string updateCommand;
                try
                {
                    updateCommand = $"UPDATE pc.ETL_Zeitplan_Ausfuehrungen " +
                                    $"SET {changeStatement} " +
                                    $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                    $"Letzte_Aenderung_Nutzer = suser_name() " +
                                    $"WHERE ETL_Zeitplan_Ausfuehrungen_ID = {id};";
                }
                catch (Exception e)
                {
                    throw new ETLException(
                        processor,
                        "Generating the SQL update statement failed!",
                        "UpdateZeitplanAusfuehrung",
                        e,
                        prozesslaeufe
                    );
                }

                // execute the update command
                try
                {
                    new SqlCommand(updateCommand, connection).ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    throw new ETLException(
                        processor,
                        "Executing the update command failed!",
                        "UpdateZeitplanAusfuehrung",
                        e,
                        prozesslaeufe
                    );
                }
            }
        }

        /// <summary>
        /// generates a string that can be inserted into an update statement to update values given in changes with
        /// corresponding values
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="changes">list that maps the changed attribute name to the new value</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <returns>statement that represents the changes to execute in SQL (only the SET part)</returns>
        /// <exception cref="ETLException">in case of any error</exception>
        private static string GetChangeStatement(
            Processor processor,
            List<Tuple<string, object>> changes,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            // generate the statements to change items in DB
            string setChange = "";

            try
            {
                foreach (Tuple<string, object> change in changes)
                {
                    string t = change.Item2.GetType().ToString();
                    string item2 = change.Item2.GetType().ToString() switch
                    {
                        "System.Boolean" => (bool)change.Item2 == true ? "1" : "0",
                        "System.DateTime" => $"'{((DateTime)change.Item2).ToString(DateFormat)}'",
                        _ => change.Item2.ToString() ?? "",
                    };
                    setChange += $"{change.Item1} = {item2}, ";
                }
            }
            catch (Exception e)
            {
                throw new ETLException(
                    processor,
                    "Generating the change statement failed!",
                    "GetChangeStatement",
                    e,
                    prozesslaeufe
                );
            }

            return setChange;
        }

        /// <summary>
        /// Updates the Zeitplan_Ausfuehrung and sets all processes to ausgefuhert
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="exception">catched exception</param>
        /// <param name="procedure">calling function of safe exit</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        public void SafeExit(
            Processor processor,
            Exception exception,
            string procedure,
            Tuple<int?, int?, int?, int?> prozesslaeufe
        )
        {
            Task.Run(() => ErrorLog(
                processor,
                "Dienst",
                "Manjor Incident happend -> Stop Service",
                "major",
                exception,
                procedure,
                prozesslaeufe
            )).Wait();

            // set all Zeitplan_Ausfuehrungen and prozesslauefe to Ausgefuehrt = 1 if not already 1
            string updateCommand = $"UPDATE pc.ETL_Zeitplan_Ausfuehrungen " +
                                   $"SET Ausgefuehrt = 1, " +
                                   $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                   $"Letzte_Aenderung_Nutzer = suser_name() " +
                                   $"WHERE Ausgefuehrt = 0;";

            string updateCommandProzesslaeufe = $"UPDATE logging.ETL_Prozesslaeufe " +
                                                $"SET Ist_abgeschlossen = 1, " +
                                                $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                                $"Letzte_Aenderung_Nutzer = suser_name() " +
                                                $"WHERE Ist_abgeschlossen = 0;";

            string updateCommandPaketProzesslaeufe = $"UPDATE logging.ETL_Paket_Prozesslaeufe " +
                                                     $"SET Ist_abgeschlossen = 1, " +
                                                     $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                                     $"Letzte_Aenderung_Nutzer = suser_name() " +
                                                     $"WHERE Ist_abgeschlossen = 0;";

            string updateCommandRelatProzesslaeufe = $"UPDATE logging.ETL_Paketumsetzung_Prozesslaeufe " +
                                                     $"SET Ist_abgeschlossen = 1, " +
                                                     $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                                     $"Letzte_Aenderung_Nutzer = suser_name() " +
                                                     $"WHERE Ist_abgeschlossen = 0;";

            string updateCommandStepProzesslaeufe = $"UPDATE logging.ETL_Paketschritt_Prozesslaeufe " +
                                                    $"SET Ist_abgeschlossen = 1, " +
                                                    $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                                    $"Letzte_Aenderung_Nutzer = suser_name() " +
                                                    $"WHERE Ist_abgeschlossen = 0;";

            try
            {
                using (SqlConnection connection = new(processor.DbHelper.BaseConnectionsString.ConnectionString))
                {
                    connection.Open();
                    new SqlCommand(updateCommand, connection).ExecuteNonQuery();
                    new SqlCommand(updateCommandProzesslaeufe, connection).ExecuteNonQuery();
                    new SqlCommand(updateCommandPaketProzesslaeufe, connection).ExecuteNonQuery();
                    new SqlCommand(updateCommandRelatProzesslaeufe, connection).ExecuteNonQuery();
                    new SqlCommand(updateCommandStepProzesslaeufe, connection).ExecuteNonQuery();
                    connection.Close();
                }
                Task.Run(() =>
                {
                    Log(
                        processor,
                        "Updated finsihed flag for Zeitplan_Ausfuehrungen and Prozesslaeufe after Service Stop finished!",
                        prozesslaeufe
                    );
                }).Wait();
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    "Updating finsihed flag for Zeitplan_Ausfuehrungen and Prozesslaeufe after Service Stop failed!",
                    "major",
                    e,
                    "SafeExit",
                    prozesslaeufe
                )).Wait();
                Environment.Exit(44);
            }

            // updating endtime of zeitplanausführungen and processes where not set yet
            updateCommand = $"UPDATE pc.ETL_Zeitplan_Ausfuehrungen " +
                            $"SET Endzeitpunkt = '{DateTime.Now.ToString(DateFormat)}', " +
                            $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                            $"Letzte_Aenderung_Nutzer = suser_name() " +
                            $"WHERE Endzeitpunkt IS NULL AND Startzeitpunkt IS NOT NULL;";

            updateCommandProzesslaeufe = $"UPDATE logging.ETL_Prozesslaeufe " +
                                         $"SET Endzeitpunkt = '{DateTime.Now.ToString(DateFormat)}', " +
                                         $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                         $"Letzte_Aenderung_Nutzer = suser_name() " +
                                         $"WHERE Endzeitpunkt IS NULL AND Startzeitpunkt IS NOT NULL;";

            updateCommandPaketProzesslaeufe = $"UPDATE logging.ETL_Paket_Prozesslaeufe " +
                                              $"SET Endzeitpunkt = '{DateTime.Now.ToString(DateFormat)}', " +
                                              $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                              $"Letzte_Aenderung_Nutzer = suser_name() " +
                                              $"WHERE Endzeitpunkt IS NULL AND Startzeitpunkt IS NOT NULL;";

            updateCommandRelatProzesslaeufe = $"UPDATE logging.ETL_Paketumsetzung_Prozesslaeufe " +
                                              $"SET Endzeitpunkt = '{DateTime.Now.ToString(DateFormat)}', " +
                                              $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                              $"Letzte_Aenderung_Nutzer = suser_name() " +
                                              $"WHERE Endzeitpunkt IS NULL AND Startzeitpunkt IS NOT NULL;";

            updateCommandStepProzesslaeufe = $"UPDATE logging.ETL_Paketschritt_Prozesslaeufe " +
                                             $"SET Endzeitpunkt = '{DateTime.Now.ToString(DateFormat)}', " +
                                             $"Letzte_Aenderung = '{DateTime.Now.ToString(DateFormat)}', " +
                                             $"Letzte_Aenderung_Nutzer = suser_name() " +
                                             $"WHERE Endzeitpunkt IS NULL AND Startzeitpunkt IS NOT NULL;";

            try
            {
                using (SqlConnection connection = new(processor.DbHelper.BaseConnectionsString.ConnectionString))
                {
                    connection.Open();
                    new SqlCommand(updateCommand, connection).ExecuteNonQuery();
                    new SqlCommand(updateCommandProzesslaeufe, connection).ExecuteNonQuery();
                    new SqlCommand(updateCommandPaketProzesslaeufe, connection).ExecuteNonQuery();
                    new SqlCommand(updateCommandRelatProzesslaeufe, connection).ExecuteNonQuery();
                    new SqlCommand(updateCommandStepProzesslaeufe, connection).ExecuteNonQuery();
                    connection.Close();
                }
                Task.Run(() =>
                {
                    Log(
                        processor,
                        "Updated endtime of Zeitplan_Ausfuehrungen and Prozesslaeufe after Service Stop finished!",
                        prozesslaeufe
                    );
                }).Wait();
            }
            catch (Exception e)
            {
                Task.Run(() => ErrorLog(
                    processor,
                    "Dienst",
                    "Updating endtime of Zeitplan_Ausfuehrungen after Service Stop failed!",
                    "major",
                    e,
                    "SafeExit",
                    prozesslaeufe
                )).Wait();
                Environment.Exit(44);
            }

            Task.Run(() =>
            {
                Log(
                    processor,
                    "ETL Service needs to be stopped!",
                    prozesslaeufe
                );
            }).Wait();

            // clean memory from unused objects
                LogMessageLocal(processor, $"Total Memory before cleaning: {GC.GetTotalMemory(false)}", prozesslaeufe);
            for (int i = 0; i <= GC.MaxGeneration; i++)
            {
                LogMessageLocal(processor, $"Clean Generation: {i}", prozesslaeufe);
                GC.Collect(i);
                LogMessageLocal(processor, $"Total Memory: {GC.GetTotalMemory(false)}", prozesslaeufe);
            }

            // send mail to inform for stop of service
            SendStopMail(
                processor,
                "Dienst beendet!",
                $"Der Dienst {processor.Servicename} wurde unerwartet auf Grund eines Major Incidents beendet!",
                _dummyTuple
            );

            Environment.Exit(17);
        }

        /// <summary>
        /// searches for palceholders within a command and replaces them with the corresponding value or throws NYI
        /// Exception if placeholder was not implemented yet
        /// </summary>
        /// <param name="processor">global processor for steering</param>
        /// <param name="workflow">to extract workflow information (IDs)</param>
        /// <param name="command">string to replace placeholders in</param>
        /// <param name="prozesslaeufe">includes the 4 possible prozesslaeufeIDs (each can be null)</param>
        /// <param name="debug">set to true to get debug output</param>
        /// <exception cref="NYIException">in case of not known placeholder</exception>
        public string ReplacePlaceholder(
            Processor processor,
            Workflow workflow,
            string command,
            Tuple<int?, int?, int?, int?> prozesslaeufe,
            bool debug = false
        )
        {
            try
            {
                // detect all placeholders in command
                MatchCollection placeholders = MyRegex().Matches(command);

                // analyze each placeholder and replace it with corresponding value
                foreach (Match placeholder in placeholders.Cast<Match>())
                {
                    string holder = placeholder.Groups[1].Value;

                    string value;
                    switch (holder)
                    {
                        case "Belegungszeit":
                            string cmd = $"SELECT t1.Anforderungszeitpunkt AS Anforderungszeitpunkt " +
                                         $"FROM pc.ETL_Zeitplan_Ausfuehrungen t1 " +
                                         $"LEFT JOIN Logging.ETL_Prozesslaeufe t2 " +
                                         $"  ON t1.ETL_Zeitplan_Ausfuehrungen_ID = t2.ETL_Zeitplan_Ausfuehrungen_ID " +
                                         $"WHERE t2.ETL_Prozesslaeufe_ID = {workflow.GetProzesslaeufeID()}";

                            System.Data.DataTable timepoint = processor.DbHelper.GetDataTableFromQuery(
                                                                                    processor, cmd, prozesslaeufe);
                            value = $"'{timepoint.Rows[0]["Anforderungszeitpunkt"]}'";
                            break;
                        case "Uebernahme_von":
                            value = workflow.GetTakeoverTime().Item1.ToString("yyyyMMdd");
                            break;
                        case "Uebernahme_bis":
                            value = workflow.GetTakeoverTime().Item2.ToString("yyyyMMdd");
                            break;
                        case "Workflow_ID":
                            value = workflow.GetID().ToString();
                            break;
                        case "Prozesslaeufe_ID":
                            value = workflow.GetProzesslaeufeID().ToString();
                            break;
                        case "Zeitplan_ID":
                            value = workflow.GetZeitplanAusfuehrungenID().ToString();
                            break;
                        case "ETL_Prozesslaeufe_ID":
                            value = prozesslaeufe.Item1.ToString() ?? "";
                            break;
                        case "ETL_Paket_Prozesslaeufe_ID":
                            value = prozesslaeufe.Item2.ToString() ?? "";
                            break;
                        case "ETL_Paketumsetzung_Prozesslaeufe_ID":
                            value = prozesslaeufe.Item3.ToString() ?? "";
                            break;
                        case "ETL_Paketschritt_Prozesslaeufe_ID":
                            value = prozesslaeufe.Item4.ToString() ?? "";
                            break;
                        case "Debug":
                            value = debug.ToString();
                            break;
                        default:
                            throw new NYIException(
                                processor,
                                "Found a placeholder that was not implemented yet!",
                                "ReplacePlaceholder",
                                prozesslaeufe
                            );
                    }

                    command = command.Replace($"##{holder}##", value);
                }

                return command;
            }
            catch (Exception e)
            {
                throw HandleErrorCatch(
                    processor,
                    e,
                    "ReplacePlaceholder",
                    "Replacing all placeholders in given string failed!",
                    ref DummySem,
                    prozesslaeufe
                );
            }
        }

        [GeneratedRegex(@"(?:##\s*|\s*##\s*)([^#]+)(?:\s*##|\s*##)")]
        private static partial Regex MyRegex();
    }
}
