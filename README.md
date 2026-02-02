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
- `/res`: SQL-Files to setup your MS SQL DB for Service usage (name of the directory is the DB-Schema)

## Overview

- [Structure](#structure)
- [Service](#service)
  - [Requirements](#requirements)
  - [Needed Code-Adjustments](#needed-code-adjustments)
  - [Build and Installation](#build-and-installation)
- [R/Shiny Monitoring](#rshiny-monitoring)
  - [Requirements](#requirements-rshiny)

## Service

### Requirements

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

### Needed Code-Adjustments

To run the code successfully for your use-case, you need to adjust 3 functions in `DIZCode.Helper` (`SendInfoMail`, `SendStopMail`, `SendResultMail`). You need to replace all `TBD`s to your needed Emails and in line 473 the name of your mail credentials saved in windows credential manager.

### Build and Installation

#### Windows

##### Build, Create and Start Service

`code\src\DIZService> .\build_and_create_service.ps1 -ServiceName "<Servicename>" -Stage "<Stage>"`

##### Restart Service

`code\src\DIZService> .\restart_service.ps1 -ServiceName "<Servicename>"`

#### Linux

**IMPORTANT:** Installation on linux not tested yet!

##### Build, Create and Start Service

`code\src\DIZService> .\create_and_start_service.ps1 "<Servicename>" "<Path-to-app-dll>" "<user>" "<publish-dir>" "<datasource>" "<db>" "<user>" "<pwd>" "<Stage>"`

##### Restart Service

`code\src\DIZService> .\restart_service.sh "<Servicename>"`

## R/Shiny Monitoring

### Example Footage

<img width="1160" height="512" alt="Baum" src="https://github.com/user-attachments/assets/bcdf8bbc-8776-46da-9772-ba7c16e15214" />

<img width="1885" height="834" alt="MIE26_P1" src="https://github.com/user-attachments/assets/9089089b-32b1-4a12-9878-6184e9ef82a3" />

<img width="1729" height="824" alt="MIE26_P3" src="https://github.com/user-attachments/assets/68750a26-de9a-4fa1-b262-c838fc13e874" />

### Execution

You need access data for your [MS SQL Database](https://www.microsoft.com/de-de/sql-server) that runs the service. This data needs to be added in `/app/.Renviron`. The App can be started using [Docker](https://www.docker.com/) by building image with `docker build -t etl .` and after that running `docker compose up -d`. When configured correctly the app should be available via Port 4444.

### Requirements R/Shiny

The following R-Packages are needed:
- shiny (1.10.0)
- shinymanager (1.0.410)
- tidyverse (2.0.0)
- visNetwork (2.1.2)
- DBI (1.2.3)
- odbc (1.3.5)
- bslib (0.9.0)
- shinyWidgets (0.9.0)
- hms (1.1.3)
- timevis (2.1.0)
- glue (1.6.2)
- htmlwidgets (1.6.4)
- data.tree (1.1.0)
- shinyTree (0.3.1)
