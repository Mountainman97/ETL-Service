# ETL-Service

A Service that executes ETL processes in a mult-threaded way to improve data collection and integration. This service is developed in C#.NET to mainly be executed on a Windows Server. Additionally we build a monitoring R/Shiny application to see latest executions of workflows.

The Service itself executes ETL processes in an automated and highly parallelized manner that is easy to configure and extend without adding code. The configuration and trace is done using a MS SQL Database.

## Structure

- `/code`: ETL Service
  - `/DIZService.Core`: Main logic of service
  - `/DIZService.Tests`: Testframework to check correctness of code (work in progress)
  - `/DIZService.Worker`: Service to execute (uses the logic of Core)
  - `/www`: files needed to send result mails etc.
- `/app`: R/Shiny Monitoring
- `/res`: SQL-Files to setup your MS SQL DB for Service usage

## Requirements

- Installed .NET Runtime 8.0.20
- Installed .NET SDK 8.0.414
- .NET Pakete:
  - **AdysTech.CredentialManager** (2.6.0): load credentials from windows crtedential store
  - **ClosedXML** (0.105.0): working with Excel Files
  - **coverlet.collector** (6.0.4): line, branch and method coverage
  - **GenericParsing** (1.6.0): working with CSV Files
  - **InterSystems.Data.IRISClient** (2.6.0): access to Intersystems DB
  - **Mircosoft.Data.SqlClient** (6.1.1): access to MSSQL DB
  - **Microsoft.Extensions.Hosting**, **Microsoft.Extensions.Hosting.WindowsServices**, **Microsoft.Extensions.EventLog** (9.0.8): Windows Logging
  - **Microsoft.NET.Test.Sdk** (17.14.1): .NET Testframework
  - **NodaTimes** (3.2.2): for time datatypes
  - **Npgsql** (9.0.3): access to PostgreSQL
  - **Oracle.ManagedDataAccess.Core** (23.9.1): access to Oracle DB
  - **Serilog.\*** (4.3.0): OS independed logging
  - **System.Drawing.Common** (9.0.8): for drawing $\rightarrow$ TBD: Replace by package to draw OS independed
  - **xunit** (2.9.3): Testingframework
  - **xunit.runner.visualstudio** (3.1.4): for testing inside of VS
- Environment Variables for execution stage
  - `<ServiceName>_Stage`: Identifier for Service Version (e.g., `PROD`, `TEST`, `DEV`)
  - `<ServiceName>_datasource`: Combination of server and Server Instance
  - `<ServiceName>_user`: Database user to use in target
  - `<ServiceName>_pwd`: Password of database user in target
  - `<ServiceName>_db`: Database to use in target
  - If not set use: `setx <ServiceName>_... "Value" /M` (e.g., setx ETLService_DEV_user "Your-User" /M)

## Needed Code-Adjustments

To run the code successfully for your use-case, you need to adjust 3 functions in `DIZCode.Helper` (`SendInfoMail`, `SendStopMail`, `SendResultMail`). You need to replace all `TBD`s to your needed Emails and in line 473 the name of your mail credentials saved in windows credential manager.

## Build and Installation

### Windows

#### Build, Create and Start Service

`code\src\DIZService> .\build_and_create_service.ps1 -ServiceName "<Servicename>" -Stage "<Stage>"`

#### Restart Service

`code\src\DIZService> .\restart_service.ps1 -ServiceName "<Servicename>"`

### Linux

**IMPORTANT:** Installation on linux not tested yet!

#### Build, Create and Start Service

`code\src\DIZService> .\create_and_start_service.ps1 "<Servicename>" "<Path-to-app-dll>" "<user>" "<publish-dir>" "<datasource>" "<db>" "<user>" "<pwd>" "<Stage>"`

#### Restart Service

`code\src\DIZService> .\restart_service.sh "<Servicename>"`
