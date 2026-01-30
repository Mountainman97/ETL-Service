library(shiny)
library(shinymanager)
library(tidyverse)
library(visNetwork)
library(DBI)
library(odbc)
library(bslib)
library(DT)
library(shinyWidgets)
library(hms)
library(timevis)
library(glue)
library(htmlwidgets)
library(data.tree)
library(shinyTree)
library(shinybusy)


ui <- page_navbar(
  title = "ETL App",
  header = add_busy_bar(color = "cyan"),
  theme = bs_theme(bg = "#002B36", fg = "#EEE8D5"),
  tags$head(
    tags$style(HTML("
    .no-process {
      font-size: 20px;
      font-weight: 500;
      color: #6c757d;
      padding: 15px;
      border: 1px dashed #ced4da;
      background-color: #f8f9fa;
      border-radius: 6px;
      text-align: center;
      margin-top: 20px;
    }

    .scroll-container::-webkit-scrollbar {
    width: 8px;
  }
  .scroll-container::-webkit-scrollbar-thumb {
    background-color: #bbb;
    border-radius: 4px;
  }

  .modal-footer .btn.btn-default {
    background-color: #EBEBEB;
    color: black;
    border: none;
  }

   html, body {
      overflow-y: hidden !important;
    }

  "))
  ),

  # GLOBAL CSS OVERRIDES for Full-Screen Modals
  tags$head(
    tags$style(HTML("
      /* Force every modal-dialog to fill the viewport */
      .modal-dialog {
        width: 90vw !important;
        max-width: 90vw !important;
        height: 90vh !important;
        margin: 1.5vh auto !important;
        padding: 0 !important;
      }

      /* Force the modal-content itself to fill */
      .modal-content {
        height: 90vh !important;
        border: none !important;
        border-radius: 0 !important;
        display: flex;
        flex-direction: column;
      }

      /* Let the modal body scroll if content overflows */
      .modal-body {
        flex: 1 1 auto !important;
        overflow-y: auto !important;       /* enable vertical scroll */
        max-height: calc(100vh - 120px) !important; /* subtract header/footer height */
        padding: 1rem !important;
}

      /* Keep header/footer fixed */
      .modal-header, .modal-footer {
        flex: 0 0 auto !important;
      }
    "))
  ),

tags$head(
  tags$style(HTML("
      .duration-rect:hover {
        background-color: #e6e6e6;
        cursor: help;
      }

      .duration-rect2:hover {
        background-color: #e6e6e6;
        cursor: help;
      }

      .ausfuerungszeiten:hover {
        background-color: #e6e6e6;
        cursor: help;
      }

      .planned_workflows:hover {
        cursor: help;
      }

      .laufend_workflows: hover {
        background-color: #e6e6e6;
        cursor: help;
      }

      .ETL_Prozesslaeufe_ID: hover {
       cursor: help;
      }

    "))
),

tags$style(HTML("

   /* Timeline background grey */
    .timevis {
      background-color: #F9F9F9 !important;
    }

  .vis-time-axis .vis-text {
    color: #000000 !important;
  }

   /* Timeline items (the content boxes) */
    .vis-item {
      background-color: #23383e !important;
      color: white !important;
      border: 1px solid #23383e !important;
      border-radius: 6px;
      padding: 4px 8px;
    }

")),

tags$style(HTML("
    li.level-Workflow  > .jstree-anchor { color: #B4F4B0 !important; }
    li.level-Paket     > .jstree-anchor { color: #FFD09F !important; }
    li.level-Umsetzung > .jstree-anchor { color: #CAAEFF !important; }
    li.level-Schritt   > .jstree-anchor { color: #C2F2F2 !important; }
  ")),

tags$head(
  tags$style(HTML("
    /* overlay inside modal */
    #sql_overlay {
      display: none;
      position: absolute;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0,0,0,0.85);
      color: #fff;
      z-index: 2000; /* above modal content */
      padding: 20px;
      overflow: auto;
    }
    #sql_overlay.visible { display: block; }
    #sql_overlay .sql-box { background: #111; padding: 16px; border-radius: 6px; color: #eee; }
    #sql_overlay .sql-footer { margin-top: 12px; }
  ")),

  tags$script(HTML("
    Shiny.addCustomMessageHandler('showSQL', function(x){
      $('#sql_overlay').addClass('visible');
      // scroll overlay to top in case
      $('#sql_overlay').scrollTop(0);
    });
    Shiny.addCustomMessageHandler('hideSQL', function(x){
      $('#sql_overlay').removeClass('visible');
    });
  "))
),


nav_panel(
  title = "Prozesse",

  # Tab-Container statt Cards
  tabsetPanel(
    id = "prozess_tabs",
    type = "tabs",  # default; kann auch "pills" sein

    tabPanel(
      title = span("Letzte Ausführung jedes Workflows",
                   style = "color: #EBEBEB;"),
      value = "letzte_ausführung",
      div(
        class = "scroll-container",
        style = "max-height: calc(100vh - 100px); overflow-y: auto; overflow-x: hidden; padding: 0; margin: 0;",
        uiOutput("workflow_buttons_letzte_ausfuehrung")
      )
    ),

    # 1. Tab
    tabPanel(
      title = span(
        "Laufende Datenübernahmen",
        style = "color: #EBEBEB;"
      ),
      value = "laufend",
      uiOutput("workflow_buttons_laufend")
    ),

    # 2. Tab
    tabPanel(
      title = span("Tagesaktuelle Datenübernahmen",
                   style = "color: #EBEBEB;"),
      value = "exec",
      div(
        class = "scroll-container",
        style = "max-height: calc(100vh - 100px); overflow-y: auto; overflow-x: hidden; padding: 0; margin: 0;",
        uiOutput("workflow_buttons_exec")
      )
    ),

    # 3. Tab
    tabPanel(
      title = span("Nächste Datenübernahme",
                   style = "color: #EBEBEB;"),
      value = "planned",
      uiOutput("workflow_buttons_planned")

    )

  )
),
nav_panel(
  title = "Workflows",
  uiOutput("wf_selector"),
  visNetworkOutput("etl_panel")
),

)

server <- function(input, output, session) {


  readRenviron(".Renviron")

  # DIZTEST
  connection <-
    DBI::dbConnect(odbc::odbc(),
                   driver =Sys.getenv("driver") ,
                   Server =Sys.getenv("server"),
                   database = Sys.getenv("database"),
                   uid = Sys.getenv("uid"),
                   pwd = Sys.getenv("pwd")
    )

  query.laufende_workflows <- "
  SELECT [ETL_Zeitplan_Ausfuehrungen_ID]
      ,[ETL_Zeitplaene_ID]
      ,[ETL_Workflow_ID]
      ,[ETL_Pakete_ID]
      ,[Anforderungszeitpunkt]
      ,[Startzeitpunkt]
      ,[Ausfuehrungsstartzeitpunkt]
      ,[Endzeitpunkt]
      ,[Ausgefuehrt]
      ,[Erfolgreich]
      ,[Datenherkunft_ID]
      ,[Anlagedatum]
      ,[Anlage_Nutzer]
      ,[Letzte_Aenderung]
      ,[Letzte_Aenderung_Nutzer]
  FROM [DIZ_NET].[pc].[ETL_Zeitplan_Ausfuehrungen]
  WHERE Ausgefuehrt = 0 AND Startzeitpunkt IS NOT NULL
  "

  laufende_workflows = DBI::dbGetQuery(connection, query.laufende_workflows)

  #laufende_workflows$ETL_Prozesslaeufe_ID <- as.character(laufende_workflows$ETL_Prozesslaeufe_ID)

  # query tagesaktuelle Ausgeführte Workflow------
  query.exec_workflows <- "SELECT p.[ETL_Prozesslaeufe_ID]
      ,p.[ETL_Zeitplan_Ausfuehrungen_ID]
      ,p.[ETL_Workflow_ID]
     ,w.Workflowname
      ,p.[Anforderungszeitpunkt]
      ,p.[Startzeitpunkt]
      ,p.[Ausfuehrungsstartzeitpunkt]
      ,p.[Ausfuehrungsendzeitpunkt]
      ,p.[Endzeitpunkt]
      ,p.[Ist_gestartet]
      ,p.[Ist_abgeschlossen]
      ,p.[Erfolgreich]
      ,p.[Parallelsperre]
      ,p.[Ist_transferiert]
      ,p.[Json_Log]
      ,p.[Anlagedatum]
      ,p.[Anlage_Nutzer]
      ,p.[Letzte_Aenderung]
      ,p.[Letzte_Aenderung_Nutzer]
  FROM [DIZ_NET].[Logging].[ETL_Prozesslaeufe] p
  JOIN DIZ_NET.pc.ETL_Workflow w
    ON w.ETL_Workflow_ID = p.ETL_Workflow_ID
  WHERE CAST(p.[Ausfuehrungsstartzeitpunkt] AS date)
      >= CAST(GETDATE() AS date) AND p.[Ausfuehrungsendzeitpunkt] IS NOT NULL
  ORDER BY p.[Ausfuehrungsstartzeitpunkt] DESC"

  # Ausgeführte Workflows
  exec_workflows <- DBI::dbGetQuery(connection, query.exec_workflows)

  exec_workflows =
    exec_workflows %>%
    dplyr::mutate(
      Gesamtdauer = as_hms(round(Endzeitpunkt - Startzeitpunkt)),
      Ausfuehrungsdauer = as_hms(round(Ausfuehrungsendzeitpunkt - Ausfuehrungsstartzeitpunkt))
    )

  exec_workflows$ETL_Prozesslaeufe_ID <- as.character(exec_workflows$ETL_Prozesslaeufe_ID)


  #----Letzte Ausgeführte Workflows----
  query.letzte_ausgefuehrte_workflows <- "
  -- Step 1: find the max Endzeitpunkt per workflow
WITH MaxEnd AS (
  SELECT
    ETL_Workflow_ID,
    MAX(Endzeitpunkt) AS MaxEndzeitpunkt
  FROM [DIZ_NET].[Logging].[ETL_Prozesslaeufe]
  GROUP BY ETL_Workflow_ID
)

-- Step 2: join to get the detail rows
SELECT
  p.[ETL_Prozesslaeufe_ID],
  p.[ETL_Zeitplan_Ausfuehrungen_ID],
  p.[ETL_Workflow_ID],
  w.Workflowname,
  p.[Anforderungszeitpunkt],
  p.[Startzeitpunkt],
  p.[Ausfuehrungsstartzeitpunkt],
  p.[Ausfuehrungsendzeitpunkt],
  p.[Endzeitpunkt],
  p.[Ist_gestartet],
  p.[Ist_abgeschlossen],
  p.[Erfolgreich],
  p.[Parallelsperre],
  p.[Ist_transferiert],
  p.[Json_Log],
  p.[Anlagedatum],
  p.[Anlage_Nutzer],
  p.[Letzte_Aenderung],
  p.[Letzte_Aenderung_Nutzer]
FROM [DIZ_NET].[Logging].[ETL_Prozesslaeufe] AS p
JOIN MaxEnd AS m
  ON p.[ETL_Workflow_ID] = m.ETL_Workflow_ID
 AND p.[Endzeitpunkt]   = m.MaxEndzeitpunkt
JOIN [DIZ_NET].[pc].[ETL_Workflow] AS w
  ON w.[ETL_Workflow_ID] = p.[ETL_Workflow_ID]
ORDER BY p.[ETL_Workflow_ID]
"

letzte_ausgefuehrte_workflows = DBI::dbGetQuery(connection, query.letzte_ausgefuehrte_workflows)
letzte_ausgefuehrte_workflows$ETL_Prozesslaeufe_ID = as.character(letzte_ausgefuehrte_workflows$ETL_Prozesslaeufe_ID)

letzte_ausgefuehrte_workflows =
  letzte_ausgefuehrte_workflows %>%
  dplyr::mutate(
    Gesamtdauer = as_hms(round(Endzeitpunkt - Startzeitpunkt)),
    Ausfuehrungsdauer = as_hms(round(Ausfuehrungsendzeitpunkt - Ausfuehrungsstartzeitpunkt))
  )

output$workflow_buttons_letzte_ausfuehrung <- renderUI({
  buttons <- lapply(seq_len(nrow(letzte_ausgefuehrte_workflows)), function(i) {
    start_end <- sprintf(
      "<div class='ausfuerungszeiten'
      title='Startzeitpunkt: %s | Endzeitpunkt: %s'
      style='display:inline-block;
        padding:5px 10px;
        border:2px solid #CCCCCC;
        border-radius:5px;
        background-color:#F9F9F9;'>
         <span style='margin-right:15px;'><i class='fa fa-clock-o'></i> %s</span>
         <span><i class='fa fa-hourglass-half'></i> %s</span>
       </div>",
      letzte_ausgefuehrte_workflows$Startzeitpunkt[i],
      letzte_ausgefuehrte_workflows$Endzeitpunkt[i],
      letzte_ausgefuehrte_workflows$Startzeitpunkt[i],
      letzte_ausgefuehrte_workflows$Endzeitpunkt[i]
    )

    durations <- sprintf(
      "<div class='duration-rect' title='Gesamtdauer: %s | Ausfuehrungsdauer: %s' style='display:inline-block; padding:5px 10px; border:2px solid #CCCCCC; border-radius:5px; background-color:#F9F9F9;'>
         <span style='margin-right:15px;'>%s</span>
         <span>%s</span>
       </div>",
      letzte_ausgefuehrte_workflows$Gesamtdauer[i],
      letzte_ausgefuehrte_workflows$Ausfuehrungsdauer[i],
      letzte_ausgefuehrte_workflows$Gesamtdauer[i],
      letzte_ausgefuehrte_workflows$Ausfuehrungsdauer[i]
    )

    badge <- sprintf(
      "<div class='ETL_Prozesslaeufe_ID' title='ETL_Prozesslaeufe_ID'>
        <span style='display:inline-block;
                  width:25px;
                  height:25px;
                  color:white;
                  font-size:15px;
                  text-align:center;
                  line-height:20px;
                  margin-left:10px;'>
         %s
     </span>
     </div>",
     letzte_ausgefuehrte_workflows$ETL_Prozesslaeufe_ID[i]
    )

    full_label <- HTML(paste0(
      "<strong>Workflow ", letzte_ausgefuehrte_workflows$ETL_Workflow_ID[i], ": ", letzte_ausgefuehrte_workflows$Workflowname[i],
      "</strong>  ", start_end, " ", durations, badge
    ))

    col <- if_else(letzte_ausgefuehrte_workflows$Erfolgreich[i] == TRUE, "success", "danger")

    btn <- actionBttn(
      inputId = paste0("workflow_btn_", letzte_ausgefuehrte_workflows$ETL_Prozesslaeufe_ID[i]),
      label   = full_label,
      style   = "stretch",
      color   = col,
      size    = "lg"
    )
  })
  do.call(fluidRow, buttons)
})

#--query.workflows_error----
workflows_error <- reactive({
  req(process_id())

  query.workflows_error <- glue_sql("

  SELECT
 -- p.[ETL_Prozesslaeufe_ID]
  --  ,p.[ETL_Workflow_ID]
  --  ,w.Workflowname
  --  ,CAST(p.Erfolgreich AS INT) AS Erfolgreich
      f.ETL_Fehlermeldungen_ID
	  ,pp.ETL_Paketschritte_ID
	  ,s.Schrittname
	  ,f.[Fehlertyp]
	  ,f.[Meldungstext]
	  ,f.[Fehlertext]
	  ,f.[Schweregrad]
	  ,f.[Prozedur]
	  ,f.[Fehlerquelle]
	  FROM [DIZ_NET].[Logging].[ETL_Fehlermeldungen] f
	JOIN	[DIZ_NET].[Logging].[ETL_Prozesslaeufe] p
		ON  p.[ETL_Prozesslaeufe_ID] = f.[ETL_Prozesslaeufe_ID]
  JOIN DIZ_NET.pc.ETL_Workflow w
    ON w.ETL_Workflow_ID = p.ETL_Workflow_ID
		LEFT JOIN [DIZ_NET].[Logging].[ETL_Paketschritt_Prozesslaeufe] pp
			ON pp.ETL_Paketschritt_Prozesslaeufe_ID = f.ETL_Paketschritt_Prozesslaeufe_ID
			LEFT JOIN DIZ_NET.pc.ETL_Paketschritte s ON s.ETL_Paketschritte_ID = pp.ETL_Paketschritte_ID
		WHERE p.[Erfolgreich] = 0 AND p.ETL_Prozesslaeufe_ID = {process_id()}
  ", .con = connection)

  DBI::dbGetQuery(connection, query.workflows_error)

})

# query etl for buttons--------
etl <- reactive({
  req(workflow_id(), process_id())
  query.etl <- glue_sql(
    "
  DECLARE @Workflow_ID       AS INT = {workflow_id()};
  DECLARE @Prozesslaeufe_ID  AS INT = {process_id()};

WITH
    ist AS (
        SELECT ModulID              = w.ETL_Workflow_ID
              ,ProzessID            = w.ETL_Prozesslaeufe_ID
              ,Bezeichner           = CONCAT('W', w.ETL_Workflow_ID)
              ,Level			        	= CAST('Workflow' AS varchar(50))
              ,Parent_ModulID       = CAST(' ' AS varchar(100))
              ,PK                   = CAST(CONCAT('W', w.ETL_Workflow_ID) AS varchar(100))
              ,Parent_PK            = CAST(' ' AS varchar(100))
              ,Erfolgreich          = w.Erfolgreich
              ,Startzeitpunkt       = w.Startzeitpunkt
              ,Exec_Startzeitpunkt  = w.Ausfuehrungsstartzeitpunkt
              ,Exec_Endzeitpunkt    = w.Ausfuehrungsendzeitpunkt
              ,Endzeitpunkt         = w.Endzeitpunkt
        FROM DIZ_NET.Logging.ETL_Prozesslaeufe AS w
        WHERE w.ETL_Workflow_ID      = {workflow_id()}
          AND w.ETL_Prozesslaeufe_ID = {process_id()}

        UNION ALL

        SELECT ModulID              = n.ETL_Pakete_ID
              ,ProzessID            = n.ETL_Paket_Prozesslaeufe_ID
              ,Bezeichner           = CONCAT('P', n.ETL_Pakete_ID)
              ,Level				        = CAST('Paket' AS varchar(50))
              ,Parent_ModulID       = CAST(CONCAT('W', w.ETL_Workflow_ID)  AS varchar(100))
              ,PK                   = CAST(CONCAT(CONCAT('W', w.ETL_Workflow_ID), '_', CONCAT('P', n.ETL_Pakete_ID)) AS varchar(100))
              ,Parent_PK            = CAST(CONCAT('W', w.ETL_Workflow_ID) AS varchar(100))
              ,Erfolgreich          = n.Erfolgreich
              ,Startzeitpunkt       = n.Startzeitpunkt
              ,Exec_Startzeitpunkt  = n.Ausfuehrungsstartzeitpunkt
              ,Exec_Endzeitpunkt    = n.Ausfuehrungsendzeitpunkt
              ,Endzeitpunkt         = n.Endzeitpunkt
        FROM DIZ_NET.Logging.ETL_Paket_Prozesslaeufe AS n
        JOIN DIZ_NET.Logging.ETL_Prozesslaeufe AS w
            ON w.ETL_Prozesslaeufe_ID = n.ETL_Prozesslaeufe_ID
        JOIN DIZ_NET.pc.ETL_Workflow wf
            ON wf.ETL_Workflow_ID = w.ETL_Workflow_ID
               AND (wf.ETL_Pakete_ID = n.ETL_Pakete_ID OR wf.ETL_Fallback_Pakete_ID = n.ETL_Pakete_ID)
        JOIN ist AS t
            ON CONCAT('W', w.ETL_Workflow_ID) = t.Bezeichner
        WHERE w.ETL_Workflow_ID      = {workflow_id()}
          AND w.ETL_Prozesslaeufe_ID = {process_id()}

        UNION ALL

        SELECT ModulID              = parent.ETL_Pakete_ID
              ,ProzessID            = parent.ETL_Paket_Prozesslaeufe_ID
              ,Bezeichner           = CONCAT('P', parent.ETL_Pakete_ID)
              ,Level			        	= CAST('Paket' AS varchar(50))
              ,Parent_ModulID       = t.Bezeichner
              ,PK                   = CAST(CONCAT(t.PK, '_', CONCAT('P', parent.ETL_Pakete_ID)) AS varchar(100))
              ,Parent_PK            = t.PK
              ,Erfolgreich          = parent.Erfolgreich
              ,Startzeitpunkt       = parent.Startzeitpunkt
              ,Exec_Startzeitpunkt  = parent.Ausfuehrungsstartzeitpunkt
              ,Exec_Endzeitpunkt    = parent.Ausfuehrungsendzeitpunkt
              ,Endzeitpunkt         = parent.Endzeitpunkt
        FROM DIZ_NET.Logging.ETL_Paket_Prozesslaeufe AS parent
        JOIN DIZ_NET.pc.ETL_Paket_Abhaengigkeiten AS a
            ON a.Vorlauf_ETL_Pakete_ID = parent.ETL_Pakete_ID
               AND ETL_Workflow_ID = {workflow_id()}
               AND a.Ist_aktiv = 1
        JOIN ist AS t
            ON CONCAT('P', a.ETL_Pakete_ID) = t.Bezeichner
        WHERE parent.ETL_Prozesslaeufe_ID = {process_id()}

        UNION ALL

        SELECT ModulID              = n.ETL_Paket_Umsetzungen_ID
              ,ProzessID            = n.ETL_Paketumsetzung_Prozesslaeufe_ID
              ,Bezeichner           = CONCAT('R', n.ETL_Paket_Umsetzungen_ID)
              ,Level			        	= CAST('Umsetzung' AS varchar(50))
              ,Parent_ModulID       = t.Bezeichner
              ,PK                   = CAST(CONCAT(t.PK, '_', CONCAT('R', n.ETL_Paket_Umsetzungen_ID)) AS varchar(100))
              ,Parent_PK            = t.PK
              ,Erfolgreich          = n.Erfolgreich
              ,Startzeitpunkt       = n.Startzeitpunkt
              ,Exec_Startzeitpunkt  = n.Ausfuehrungsstartzeitpunkt
              ,Exec_Endzeitpunkt    = n.Ausfuehrungsendzeitpunkt
              ,Endzeitpunkt         = n.Endzeitpunkt
        FROM DIZ_NET.Logging.ETL_Paketumsetzung_Prozesslaeufe AS n
        JOIN DIZ_NET.pc.ETL_Pakete_Paketumsetzungen AS a
            ON a.ETL_Paket_Umsetzungen_ID = n.ETL_Paket_Umsetzungen_ID
               AND ETL_Workflow_ID = {workflow_id()}
               AND a.Ist_aktiv = 1
        JOIN ist AS t
            ON CONCAT('P', a.ETL_Pakete_ID) = t.Bezeichner
        WHERE n.ETL_Prozesslaeufe_ID = {process_id()}

        UNION ALL

        SELECT ModulID              = n.ETL_Paketschritte_ID
              ,ProzessID            = n.ETL_Paketschritt_Prozesslaeufe_ID
              ,Bezeichner           = CONCAT('S', n.ETL_Paketschritte_ID)
              ,Level			         	= CAST('Schritt' AS varchar(50))
              ,Parent_ModulID       = t.Bezeichner
              ,PK                   = CAST(CONCAT(t.PK, '_', CONCAT('S', n.ETL_Paketschritte_ID)) AS varchar(100))
              ,Parent_PK            = t.PK
              ,Erfolgreich          = n.Erfolgreich
              ,Startzeitpunkt       = n.Startzeitpunkt
              ,Exec_Startzeitpunkt  = n.Ausfuehrungsstartzeitpunkt
              ,Exec_Endzeitpunkt    = n.Ausfuehrungsendzeitpunkt
              ,Endzeitpunkt         = n.Endzeitpunkt
        FROM DIZ_NET.Logging.ETL_Paketschritt_Prozesslaeufe AS n
        JOIN DIZ_NET.pc.ETL_Paketumsetzungen_Paketschritte AS a
            ON a.ETL_Paketschritte_ID = n.ETL_Paketschritte_ID
               AND ETL_Workflow_ID = {workflow_id()}
               AND a.Ist_aktiv = 1
        JOIN ist AS t
            ON CONCAT('R', a.ETL_Paket_Umsetzungen_ID) = t.Bezeichner
        WHERE n.ETL_Prozesslaeufe_ID = {process_id()}
    ),
    soll AS (
        SELECT ModulID          = w.ETL_Workflow_ID
              ,Bezeichner       = CONCAT('W', w.ETL_Workflow_ID)
              ,Parent_ModulID   = CAST(' ' AS varchar(100))
              ,Level			     	= CAST('Workflow' AS varchar(50))
              ,PK               = CAST(CONCAT('W', w.ETL_Workflow_ID) AS varchar(100))
              ,Parent_PK        = CAST('' AS varchar(100))
              ,Modulname        = CAST(Workflowname  AS varchar(100))
        FROM DIZ_NET.pc.ETL_Workflow AS w
        WHERE w.ETL_Workflow_ID = {workflow_id()}

        UNION ALL

        SELECT ModulID          = n.ETL_Pakete_ID
              ,Bezeichner       = CONCAT('P', n.ETL_Pakete_ID)
              ,Parent_ModulID   = CAST(CONCAT('W', w.ETL_Workflow_ID)  AS varchar(100))
              ,Level			    	= CAST('Paket' AS varchar(50))
              ,PK               = CAST(CONCAT(CONCAT('W', w.ETL_Workflow_ID), '_', CONCAT('P', n.ETL_Pakete_ID)) AS varchar(100))
              ,Parent_PK        = CAST(CONCAT('W', w.ETL_Workflow_ID) AS varchar(100))
              ,Modulname        = CAST(Paketname  AS varchar(100))
        FROM DIZ_NET.pc.ETL_Pakete AS n
        JOIN DIZ_NET.pc.ETL_Workflow AS w
            ON w.ETL_Pakete_ID = n.ETL_Pakete_ID
               OR w.ETL_Fallback_Pakete_ID = n.ETL_Pakete_ID
        JOIN soll AS t
            ON CONCAT('W', w.ETL_Workflow_ID) = t.Bezeichner
        WHERE w.ETL_Workflow_ID = {workflow_id()}

        UNION ALL

        SELECT ModulID          = child.ETL_Pakete_ID
              ,Bezeichner       = CONCAT('P', child.ETL_Pakete_ID)
              ,Parent_ModulID   = t.Bezeichner
              ,Level				= CAST('Paket' AS varchar(50))
              ,PK               = CAST(CONCAT(t.PK, '_', CONCAT('P', child.ETL_Pakete_ID)) AS varchar(100))
              ,Parent_PK        = t.PK
              ,Modulname        = CAST(child.Paketname  AS varchar(100))
        FROM DIZ_NET.pc.ETL_Pakete AS parent
        JOIN DIZ_NET.pc.ETL_Paket_Abhaengigkeiten AS a
            ON a.ETL_Pakete_ID = parent.ETL_Pakete_ID
               AND ETL_Workflow_ID = {workflow_id()}
               AND a.Ist_aktiv = 1
        JOIN DIZ_NET.pc.ETL_Pakete AS child
            ON child.ETL_Pakete_ID = a.Vorlauf_ETL_Pakete_ID
        JOIN soll AS t
            ON CONCAT('P', a.ETL_Pakete_ID) = t.Bezeichner

        UNION ALL

        SELECT ModulID          = n.ETL_Paket_Umsetzungen_ID
              ,Bezeichner       = CONCAT('R', n.ETL_Paket_Umsetzungen_ID)
              ,Parent_ModulID   = t.Bezeichner
              ,Level			    	= CAST('Umsetzung' AS varchar(50))
              ,PK               = CAST(CONCAT(t.PK, '_', CONCAT('R', n.ETL_Paket_Umsetzungen_ID)) AS varchar(100))
              ,Parent_PK        = t.PK
              ,Modulname        = CAST(Umsetzungsname  AS varchar(100))
        FROM DIZ_NET.pc.ETL_Paket_Umsetzungen AS n
        JOIN DIZ_NET.pc.ETL_Pakete_Paketumsetzungen AS a
            ON a.ETL_Paket_Umsetzungen_ID = n.ETL_Paket_Umsetzungen_ID
               AND ETL_Workflow_ID = {workflow_id()}
               AND a.Ist_aktiv = 1
        JOIN soll AS t
            ON CONCAT('P', a.ETL_Pakete_ID) = t.Bezeichner

        UNION ALL

        SELECT ModulID          = n.ETL_Paketschritte_ID
              ,Bezeichner       = CONCAT('S', n.ETL_Paketschritte_ID)
              ,Parent_ModulID   = t.Bezeichner
              ,Level				    = CAST('Schritt' AS varchar(50))
              ,PK               = CAST(CONCAT(t.PK, '_', CONCAT('S', n.ETL_Paketschritte_ID)) AS varchar(100))
              ,Parent_PK        = t.PK
              ,Modulname        = CAST(Schrittname  AS varchar(100))
        FROM DIZ_NET.pc.ETL_Paketschritte AS n
        JOIN DIZ_NET.pc.ETL_Paketumsetzungen_Paketschritte AS a
            ON a.ETL_Paketschritte_ID = n.ETL_Paketschritte_ID
               AND ETL_Workflow_ID = {workflow_id()}
               AND a.Ist_aktiv = 1
        JOIN soll AS t
            ON CONCAT('R', a.ETL_Paket_Umsetzungen_ID) = t.Bezeichner
    )
SELECT
i.ProzessID
,s.ModulID
,s.Level
,s.Bezeichner
,s.Parent_ModulID
,s.PK
,s.Parent_PK
,s.Modulname
,schritte.Befehl AS SQL_Query
,CASE WHEN i.Erfolgreich IS NULL THEN 2 ELSE i.Erfolgreich END AS Erfolgreich
,i.Startzeitpunkt
,i.Exec_Startzeitpunkt
,i.Exec_Endzeitpunkt
,i.Endzeitpunkt
--,error.Fehlertext
--,error. ...
--SELECT COUNT(DISTINCT s.PK), COUNT(*)
FROM soll AS s
LEFT JOIN ist AS i
ON s.PK = i.PK

LEFT JOIN pc.ETL_Workflow AS workflows
ON s.Bezeichner LIKE 'W%' AND s.ModulID = workflows.ETL_Workflow_ID
LEFT JOIN pc.ETL_Pakete AS pakete
ON s.Bezeichner LIKE 'P%' AND s.ModulID = pakete.ETL_Pakete_ID
LEFT JOIN pc.ETL_Paket_Umsetzungen AS umsetzungen
ON s.Bezeichner LIKE 'R%' AND s.ModulID = umsetzungen.ETL_Paket_Umsetzungen_ID
LEFT JOIN pc.ETL_Paketschritte AS schritte
ON s.Bezeichner LIKE 'S%' AND s.ModulID = schritte.ETL_Paketschritte_ID
", .con = connection)

  DBI::dbGetQuery(connection, query.etl)
})


# Observe the reactive and print it
observe({
  req(etl())           # only run once etl() is non‐NULL
  cat("=== ETL() fired at", Sys.time(), "===\n")
  print(etl())         # will show your data.frame in the console
  cat("\n")
})


# 1) Load all distinct workflow IDs once
unique_ids <- reactive({
  dbGetQuery(
    connection,
    "SELECT [Workflowname], [ETL_Workflow_ID] FROM [DIZ_NET].[pc].[ETL_Workflow]"
  )

})

# 2) Render the selectInput *once* when those IDs arrive
output$wf_selector <- renderUI({
  df <- unique_ids()
  req(df)

  selectInput(
    inputId  = "wf_id",
    label    = "Wähle Workflow:",
    choices  = setNames(
      df$ETL_Workflow_ID,
      paste0(df$ETL_Workflow_ID, " - ", df$Workflowname)
    )
  )
})

# Reaktive Variable für die Workflow Auswahl
selected_wf_id <- reactive({
  req(input$wf_id)
  as.integer(input$wf_id)  # or as.character(), depending on your type
})

unique_process_ids <- reactive({
  df <- dbGetQuery(
    connection,
    "SELECT DISTINCT [ETL_Prozesslaeufe_ID] FROM [DIZ_NET].[Logging].[ETL_Prozesslaeufe]"
  )
  df$ETL_Prozesslaeufe_ID
})


# Reaktive Datenabfrage für ETL Tree im Nav_Panel, ermöglicht das Filtern der einzelnen Workflows
wf_data <- reactive({
  req(selected_wf_id())
  wf_id_val <- selected_wf_id()

  query <-  glue_sql("
WITH tree AS (
SELECT id = w.ETL_Workflow_ID, [label] = CONCAT('W', w.ETL_Workflow_ID), Level = CAST('Workflow' AS varchar(50)) ,parent = CAST(' ' AS varchar(100)), modulname = CAST(Workflowname  AS varchar(100))
FROM [DIZ_NET].[pc].[ETL_Workflow] AS w
WHERE w.ETL_Workflow_ID = {wf_id*}

UNION ALL

SELECT id = n.ETL_Pakete_ID, [label] = CONCAT('P', n.ETL_Pakete_ID),Level = CAST('Paket' AS varchar(50)), parent = CAST(CONCAT('W', w.ETL_Workflow_ID)  AS varchar(100)), modulname = CAST(Paketname  AS varchar(100))
FROM DIZ_NET.pc.ETL_Pakete AS n
JOIN DIZ_NET.pc.ETL_Workflow AS w
    ON w.ETL_Pakete_ID = n.ETL_Pakete_ID
JOIN tree AS t
    ON CONCAT('W', w.ETL_Workflow_ID) = t.[label]
WHERE w.ETL_Workflow_ID = {wf_id*}

UNION ALL

SELECT id = a.Vorlauf_ETL_Pakete_ID, [label] = CONCAT('P', a.Vorlauf_ETL_Pakete_ID),Level = CAST('Paket' AS varchar(50)) , parent = t.[label], modulname = CAST(Paketname  AS varchar(100))
FROM DIZ_NET.pc.ETL_Pakete AS n
JOIN DIZ_NET.pc.ETL_Paket_Abhaengigkeiten AS a
    ON a.ETL_Pakete_ID = n.ETL_Pakete_ID AND ETL_Workflow_ID = {wf_id*} AND a.Ist_aktiv = 1
JOIN tree AS t
    ON CONCAT('P', a.ETL_Pakete_ID) = t.[label]

UNION ALL

SELECT id = n.ETL_Paket_Umsetzungen_ID, [label] = CONCAT('R', n.ETL_Paket_Umsetzungen_ID),Level = CAST('Umsetzung' AS varchar(50)), parent = t.[label], modulname = CAST(Umsetzungsname  AS varchar(100))
FROM DIZ_NET.pc.ETL_Paket_Umsetzungen AS n
JOIN DIZ_NET.pc.ETL_Pakete_Paketumsetzungen AS a
    ON a.ETL_Paket_Umsetzungen_ID = n.ETL_Paket_Umsetzungen_ID AND a.Ist_aktiv = 1 AND ETL_Workflow_ID = {wf_id*}
JOIN tree AS t
    ON CONCAT('P', a.ETL_Pakete_ID) = t.[label]

UNION ALL

SELECT id = n.ETL_Paketschritte_ID, [label] = CONCAT('S', n.ETL_Paketschritte_ID), Level = CAST('Schritt' AS varchar(50)), parent = t.[label], modulname = CAST(Schrittname  AS varchar(100))
FROM DIZ_NET.pc.ETL_Paketschritte AS n
JOIN DIZ_NET.pc.ETL_Paketumsetzungen_Paketschritte AS a
    ON a.ETL_Paketschritte_ID = n.ETL_Paketschritte_ID AND a.Ist_aktiv = 1 AND ETL_Workflow_ID = {wf_id*}
JOIN tree AS t
    ON CONCAT('R', a.ETL_Paket_Umsetzungen_ID) = t.[label]
	)

SELECT * FROM tree;",

wf_id = selected_wf_id(),
.con  = connection
  )

  dbGetQuery(connection, query)

})

timeline <- reactive({
  req(input$prozess_id_selected)

  query.timeline_raw =
    "SELECT pl.ETL_Prozesslaeufe_ID AS Prozess_ID
      ,pl.ETL_Workflow_ID AS Modul_ID
      ,'W' AS Modul_Level
      ,CONCAT('W', pl.ETL_Workflow_ID) AS Modul_ID_Name
      ,w.Workflowname AS Modulname
      ,pl.Anforderungszeitpunkt
      ,pl.Startzeitpunkt
      ,pl.Ausfuehrungsstartzeitpunkt
      ,pl.Ausfuehrungsendzeitpunkt
      ,pl.Endzeitpunkt
      ,pl.Ist_gestartet
      ,pl.Ist_abgeschlossen
      ,pl.Erfolgreich
      ,pl.Parallelsperre
FROM [DIZ_NET].[Logging].[ETL_Prozesslaeufe] pl
JOIN DIZ_NET.pc.ETL_Workflow w
    ON w.ETL_Workflow_ID = pl.ETL_Workflow_ID
WHERE pl.ETL_Prozesslaeufe_ID = ?prozess.id
UNION
SELECT p_pl.ETL_Paket_Prozesslaeufe_ID AS Prozess_ID
      ,p_pl.ETL_Pakete_ID AS Modul_ID
      ,'P' AS Modul_Level
      ,CONCAT('P', p_pl.ETL_Pakete_ID) AS Modul_ID_Name
      ,p.Paketname AS Modulname
      ,p_pl.Anforderungszeitpunkt
      ,p_pl.Startzeitpunkt
      ,p_pl.Ausfuehrungsstartzeitpunkt
      ,p_pl.Ausfuehrungsendzeitpunkt
      ,p_pl.Endzeitpunkt
      ,p_pl.Ist_gestartet
      ,p_pl.Ist_abgeschlossen
      ,p_pl.Erfolgreich
      ,p_pl.Parallelsperre
FROM DIZ_NET.Logging.ETL_Paket_Prozesslaeufe p_pl
JOIN DIZ_NET.pc.ETL_Pakete p
    ON p.ETL_Pakete_ID = p_pl.ETL_Pakete_ID
WHERE p_pl.ETL_Prozesslaeufe_ID = ?prozess.id
UNION
SELECT pu_pl.ETL_Paketumsetzung_Prozesslaeufe_ID AS Prozess_ID
      ,pu_pl.ETL_Paket_Umsetzungen_ID AS Modul_ID
      ,'R' AS Modul_Level
      ,CONCAT('R', pu_pl.ETL_Paket_Umsetzungen_ID) AS Modul_ID_Name
      ,pu.Umsetzungsname AS Modulname
      ,pu_pl.Anforderungszeitpunkt
      ,pu_pl.Startzeitpunkt
      ,pu_pl.Ausfuehrungsstartzeitpunkt
      ,pu_pl.Ausfuehrungsendzeitpunkt
      ,pu_pl.Endzeitpunkt
      ,pu_pl.Ist_gestartet
      ,pu_pl.Ist_abgeschlossen
      ,pu_pl.Erfolgreich
      ,pu_pl.Parallelsperre
FROM DIZ_NET.Logging.ETL_Paketumsetzung_Prozesslaeufe pu_pl
JOIN DIZ_NET.pc.ETL_Paket_Umsetzungen pu
    ON pu.ETL_Paket_Umsetzungen_ID = pu_pl.ETL_Paket_Umsetzungen_ID
WHERE pu_pl.ETL_Prozesslaeufe_ID = ?prozess.id
UNION
SELECT ps_pl.ETL_Paketschritt_Prozesslaeufe_ID AS Prozess_ID
      ,ps_pl.ETL_Paketschritte_ID AS Modul_ID
      ,'S' AS Modul_Level
      ,CONCAT('S', ps_pl.ETL_Paketschritte_ID) AS Modul_ID_Name
      ,s.Schrittname AS Modulname
      ,ps_pl.Anforderungszeitpunkt
      ,ps_pl.Startzeitpunkt
      ,ps_pl.Ausfuehrungsstartzeitpunkt
      ,ps_pl.Ausfuehrungsendzeitpunkt
      ,ps_pl.Endzeitpunkt
      ,ps_pl.Ist_gestartet
      ,ps_pl.Ist_abgeschlossen
      ,ps_pl.Erfolgreich
      ,ps_pl.Parallelsperre
FROM DIZ_NET.Logging.ETL_Paketschritt_Prozesslaeufe ps_pl
JOIN DIZ_NET.pc.ETL_Paketschritte s
    ON s.ETL_Paketschritte_ID = ps_pl.ETL_Paketschritte_ID
WHERE ps_pl.ETL_Prozesslaeufe_ID = ?prozess.id" # ETL_Prozesslaeufe_ID MUSS die selbe sein wie in query.exec_workflows


  sql <- sqlInterpolate(
    connection,
    query.timeline_raw,
    "prozess.id" = input$prozess_id_selected
  )

  DBI::dbGetQuery(connection, sql)
})

#query.planned_workflows---------------
query.planned_workflows <- "SELECT [ETL_Zeitplan_Ausfuehrungen_ID]
      ,[ETL_Zeitplaene_ID]
      ,[ETL_Workflow_ID]
      ,[ETL_Pakete_ID]
      ,[Anforderungszeitpunkt]
      ,[Startzeitpunkt]
      ,[Ausfuehrungsstartzeitpunkt]
      ,[Endzeitpunkt]
      ,[Ausgefuehrt]
      ,[Erfolgreich]
      ,[Datenherkunft_ID]
      ,[Anlagedatum]
      ,[Anlage_Nutzer]
      ,[Letzte_Aenderung]
      ,[Letzte_Aenderung_Nutzer]
  FROM [DIZ_NET].[pc].[ETL_Zeitplan_Ausfuehrungen]
  WHERE Ausgefuehrt = 0
  AND Startzeitpunkt IS NULL"

# Nächste Datenübernahme
planned_workflows <- DBI::dbGetQuery(connection, query.planned_workflows)

output$workflow_buttons_laufend <- renderUI({

  if (nrow(laufende_workflows) == 0) {
    div(class = "no-process", "🔍 Keine laufenden Prozesse")
  } else {

    sorted_laufend_workflows <- laufende_workflows[order(laufende_workflows$Startzeitpunkt), ]

    buttons <- lapply(seq_len(nrow(sorted_laufend_workflows)), function(i) {
      start_end <- sprintf(
        "<div class='laufend_workflows'
           title='Startzeitpunkt: %1$s'
           style='display:inline-block;
           padding:5px 10px;
           border:2px solid #CCCCCC;
            border-radius:5px;'>
          <span style='margin-right:15px;'>
            <i class='fa fa-clock-o'></i> %1$s
          </span>
         </div>",
        laufende_workflows$Startzeitpunkt[i]
      )

      badge <- sprintf(
        "<div class='ETL_Prozesslaeufe_ID' title='ETL_Prozesslaeufe_ID'>
        <span style='display:inline-block;
                  width:25px;
                  height:25px;
                  color:white;
                  font-size:15px;
                  text-align:center;
                  line-height:20px;
                  margin-left:10px;'>
         %s
     </span>
     </div>",
     laufende_workflows$ETL_Prozesslaeufe_ID[i]
      )

      full_label <- HTML(paste0(
        "<strong>Workflow ", laufende_workflows$ETL_Workflow_ID[i], ": ", laufende_workflows$Workflowname[i],
        "</strong>  ", start_end, " ", badge
      ))


      actionBttn(
        inputId = paste0("workflow_btn_", laufende_workflows$ETL_Prozesslaeufe_ID[i]),
        label   = full_label,
        style   = "stretch",
        color   = "default",
        size    = "lg"
      )
    })

    do.call(fluidRow, buttons)
  }
})

# keep track of which process we're on
process_id <- reactiveVal(NULL)

output$workflow_buttons_exec <- renderUI({

  # Sort the data frame by Anforderungszeitpunkt
  # sorted_exec_workflows <- exec_workflows[order(exec_workflows$Ausfuehrungsstartzeitpunkt), ]

  buttons <- lapply(seq_len(nrow(exec_workflows)), function(i) {
    start_end <- sprintf(
      "<div class='ausfuerungszeiten'
      title='Ausfuehrungsstartzeitpunkt: %s | Ausfuehrungsendzeitpunkt: %s'
      style='display:inline-block; padding:5px 10px;
      border:2px solid #CCCCCC;
      border-radius:5px;
      background-color:#F9F9F9;'>
      <span style='margin-right:15px;'><i class='fa fa-clock-o'></i> %s</span>
      <span><i class='fa fa-hourglass-half'></i> %s</span>
       </div>",
      exec_workflows$Ausfuehrungsstartzeitpunkt[i],
      exec_workflows$Ausfuehrungsendzeitpunkt[i],
      exec_workflows$Ausfuehrungsstartzeitpunkt[i],
      exec_workflows$Ausfuehrungsendzeitpunkt[i]
    )

    durations <- sprintf(
      "<div class='duration-rect' title='Gesamtdauer: %s | Ausfuehrungsdauer: %s' style='display:inline-block; padding:5px 10px; border:2px solid #CCCCCC; border-radius:5px; background-color:#F9F9F9;'>
         <span style='margin-right:15px;'>%s</span>
         <span>%s</span>
       </div>",
      exec_workflows$Gesamtdauer[i],
      exec_workflows$Ausfuehrungsdauer[i],
      exec_workflows$Gesamtdauer[i],
      exec_workflows$Ausfuehrungsdauer[i]
    )

    badge <- sprintf(
      "<div class='ETL_Prozesslaeufe_ID' title='ETL_Prozesslaeufe_ID'>
        <span style='display:inline-block;
                  width:25px;
                  height:25px;
                  color:white;
                  font-size:15px;
                  text-align:center;
                  line-height:20px;
                  margin-left:10px;'>
         %s
     </span>
     </div>",
     exec_workflows$ETL_Prozesslaeufe_ID[i]
    )

    full_label <- HTML(paste0(
      "<strong>Workflow ", exec_workflows$ETL_Workflow_ID[i], ": ", exec_workflows$Workflowname[i],
      "</strong>  ", start_end, " ", durations, badge
    ))

    col <- if_else(exec_workflows$Erfolgreich[i] == TRUE, "success", "danger")

    btn <- actionBttn(
      inputId = paste0("workflow_btn_", exec_workflows$ETL_Prozesslaeufe_ID[i]),
      label   = full_label,
      style   = "stretch",
      color   = col,
      size    = "lg"
    )
  })
  do.call(fluidRow, buttons)
})


tree_data <- reactive({
  req(etl())   # etl() depends on process_id()

  df <- etl() |>
    dplyr::mutate(
      Parent_PK = dplyr::if_else(Parent_PK == "", "root2", Parent_PK),
      Modulname = dplyr::if_else(is.na(Modulname) | Modulname == "", PK, Modulname)
    ) |>
    dplyr::select(from = Parent_PK, to = PK, Modulname, Level)

  root <- FromDataFrameNetwork(df)

  build <- function(nd) {
    kids <- nd$children
    if (length(kids) == 0) return(structure(list(), stdata = nd$name))
    res <- lapply(kids, build)
    names(res) <- vapply(kids, function(x) {
      lbl <- x$Modulname
      if (is.null(lbl) || is.na(lbl) || lbl == "") x$name else lbl
    }, character(1))
    structure(res, stdata = nd$name)
  }
  build(root)
})

output$tree <- renderTree({
  req(tree_data())
  tree_data()
})
outputOptions(output, "tree", suspendWhenHidden = FALSE)

# encapsulate the *first* modal in a function
show_etl_modal <- function(id) {
  # ensure workflow_id is available
  wf_id <- isolate(workflow_id())
  req(wf_id, id)

  # ensure the active tab is available
  active_tab <- isolate(input$prozess_tabs)
  req(active_tab)

  # pick the right source depending on active tab
  modul_name <- switch(
    active_tab,
    "exec" = {
      exec_workflows %>%
        dplyr::filter(ETL_Prozesslaeufe_ID == id) %>%
        dplyr::pull(Workflowname) %>%
        unique()
    },
    "letzte_ausführung" = {
      letzte_ausgefuehrte_workflows %>%
        dplyr::filter(ETL_Prozesslaeufe_ID == id) %>%
        dplyr::pull(Workflowname) %>%
        unique()
    },
    NA
  )

  # build title safely
  title_txt <- if (!is.na(modul_name) && length(modul_name) > 0) {
    paste("ETL-Prozesslauf:", id, "| Workflow:", wf_id, "-", modul_name)
  } else {
    paste("ETL-Prozesslauf:", id, "| Workflow:", wf_id)
  }

  # show modal only if everything is ready
  showModal(modalDialog(
    title = title_txt,
    tabsetPanel(
      id = "modal_tabs",
      selected = "ETL-Baum",
      tabPanel(
        title = span("ETL-Baum", style = "color: #EBEBEB;"),
        value = "ETL-Baum",
        fluidRow(
          column(
            2,
            div(style="min-height:300px; overflow:auto;",
                shinyTree(
                  "tree",
                  checkbox = FALSE,
                  themeIcons = FALSE,
                  themeDots = FALSE,
                  theme = 'proton',
                  wholerow = TRUE,
                  whole_node = FALSE,
                  stripes = FALSE
                )
            )
          ),
          column(
            10,
            visNetworkOutput("etl", height = "600px")
          )
        )
      ),
      tabPanel(
        title = span("Error", style = "color: #EBEBEB;"),
        value = "Error",
        DTOutput("error_table")
      ),
      tabPanel(
        title = span("Timeline", style = "color: #EBEBEB;"),
        value = "Timeline",
        timevisOutput("timeline")
      )
    ),
    easyClose = TRUE,
    size = "xl",
    footer = modalButton("Close"),
    tags$div(
      id = "sql_overlay",
      tags$div(
        class = "sql-box",
        tags$h4("SQL-Query"),
        verbatimTextOutput("sql_text"),
        tags$div(
          class = "sql-footer",
          actionButton("back_to_overview", "← Zurück"),
          modalButton("Schließen")
        )
      )
    )
  ))
}



show_sql_query <- function(node_id) {
  message("show_sql_query: node_id = ", node_id)

  req(node_id)

  df_node <- etl() %>%
    dplyr::filter(PK == node_id)

  message("Level: ", paste(unique(df_node$Level), collapse = ", "))

  req(unique(df_node$Level) == "Schritt")

  pid <- df_node$ProzessID
  req(pid)
  message("ProzessID: ", pid)


  query <- glue::glue_sql(
    "
    SELECT ETL_Prozesslaeufe_ID, Query
    FROM DIZ_NET.Logging.ETL_SQL_Anfragen
    WHERE ETL_Konfigurationen_ID IS NOT NULL
      AND Query NOT LIKE 'SELECT COUNT(*) FROM%%'
      AND Query NOT LIKE 'SELECT * FROM%%'
      AND ETL_Paketschritt_Prozesslaeufe_ID = {pid}
    ",
    .con = connection
  )

  df_sql <- DBI::dbGetQuery(connection, query)
  message("Retrieved ", nrow(df_sql), " SQL queries.")

  sql_text <- paste(df_sql$Query, collapse = "\n")

  showModal(
    modalDialog(
      title = paste("SQL-Query für Schritt", node_id),
      size = "l",
      easyClose = TRUE,
      tags$div(
        style = "max-height:60vh; overflow-y:auto; padding-right:15px;",
        tags$pre(sql_text)
      ),
      footer = tagList(
        actionButton("back_to_overview", "← Zurück"),
        modalButton("Schließen")
      )
    )
  )
}

## From visNetwork
observeEvent(input$etl_selected_node, {
  show_sql_query(input$etl_selected_node)
})

observeEvent(input$tree, {
  sel <- get_selected(input$tree)   # shinyTree helper
  req(sel, length(sel) > 0)

  message("sel: ", sel)

  clicked_label <- sel[[1]] # this is the label shown in the tree (Modulname)
  req(clicked_label)
  message("Clicked Label: ", clicked_label)
  # Look up the PK from etl() using the label
  df_node <- etl() %>% dplyr::filter(Modulname == clicked_label)
  req(nrow(df_node) > 0)

  node_id <- df_node$PK[1]  # 'to' column in df2 corresponds to PK
  message("Clicked node PK: ", node_id)

  show_sql_query(node_id)
})

observeEvent(input$back_to_etl, {
  updateTabsetPanel(session, "modal_tabs", selected = "ETL-Baum")
})

# --- 0) cache for the built tree (plain R list) ---
tree_cached <- reactiveVal(NULL)
# --- 1) helper: build the shinyTree list structure from FromDataFrameNetwork() result ---
build_tree_from_root <- function(root) {
  if (is.null(root)) return(list())

  build <- function(nd) {
    kids <- nd$children
    if (length(kids) == 0) {
      # leaf node must be an empty list with stdata attached
      return(structure(list(), stdata = nd$name))
    }
    res <- lapply(kids, build)
    names(res) <- vapply(kids, function(x) {
      lbl <- x$Modulname
      if (is.null(lbl) || is.na(lbl) || lbl == "") x$name else lbl
    }, character(1))
    structure(res, stdata = nd$name)
  }

  build(root)
}
# --- 2) keep the cache updated whenever etl() changes ---
observe({
  df <- req(etl())   # IMPORTANT: etl() should reflect the current process_id()

  # safe defensive transforms (avoid pipe-to-anonymous-function patterns)
  df2 <- df %>%
    dplyr::mutate(
      Parent_PK = dplyr::if_else(Parent_PK == "" | is.na(Parent_PK), "root2", Parent_PK),
      Modulname  = dplyr::if_else(is.na(Modulname) | Modulname == "", PK, Modulname)
    ) %>%
    dplyr::select(from = Parent_PK, to = PK, Modulname, Level)

  # FromDataFrameNetwork() expects a data frame with from/to - adapt if necessary
  net_obj <- tryCatch(
    FromDataFrameNetwork(as.data.frame(df2)),
    error = function(e) NULL
  )

  built_tree <- NULL
  if (!is.null(net_obj)) {
    # build the plain list for shinyTree
    built_tree <- build_tree_from_root(net_obj)
  } else {
    built_tree <- list()
  }
  # store plain list (non-reactive) into reactiveVal
  tree_cached(built_tree)
})

# --- 3) render the tree once (binding stays in output) ---
output$tree <- renderTree({
  req(tree_cached())
  tree_cached()
})

outputOptions(output, "tree", suspendWhenHidden = FALSE)

# --- 4) minimal button observers (open ETL modal) ---
observe({
  lapply(exec_workflows$ETL_Prozesslaeufe_ID, function(id) {
    btn <- paste0("workflow_btn_", id)

    observeEvent(input[[btn]], {
      process_id(id)          # set current process so etl() reacts
      show_etl_modal(id)      # open modal

      # AFTER modal DOM is present, force tab selection and update the tree
      session$onFlushed(function() {
        # force ETL-Baum selected (ensure you gave the tabPanel value = "ETL-Baum")
        updateTabsetPanel(session, "modal_tabs", selected = "ETL-Baum")

        # use isolate(tree_cached()) because we're in a non-reactive callback
        try({
          shinyTree::updateTree(session, "tree", isolate(tree_cached()))
        }, silent = TRUE)

        # small JS nudge to ensure jstree redraw (add the JS handler in ui once)
        session$sendCustomMessage("refreshTree", "tree")
      }, once = TRUE)
    }, ignoreInit = TRUE)
  })
})

# --- 5) Zurück from SQL modal: remove SQL modal, reopen ETL modal and refresh tree ---
# back handler (replace your existing observeEvent for back_to_overview)
observeEvent(input$back_to_overview, {
  removeModal()
  show_etl_modal(process_id())

  session$onFlushed(function() {
    # make sure tab has a value = "ETL-Baum" in UI
    tryCatch({
      updateTabsetPanel(session, "modal_tabs", selected = "ETL-Baum")
    }, error = function(e) {
      message("updateTabsetPanel error: ", conditionMessage(e))
    })

    # push cached tree into client (use isolate to avoid reactive evaluation here)
    tc <- isolate(tree_cached())
    if (!is.null(tc) && length(tc) > 0) {
      tryCatch({
        shinyTree::updateTree(session, "tree", tc)
      }, error = function(e) {
        message("updateTree failed: ", conditionMessage(e))
      })
    }

    # Ask client to attempt safe refresh (see JS handler below)
    tryCatch({
      session$sendCustomMessage("safeRefreshWidgets", list(tree = "tree", network = "etl"))
    }, error = function(e) {
      message("safeRefreshWidgets send failed: ", conditionMessage(e))
    })
  }, once = TRUE)
})

observe({
  lapply(letzte_ausgefuehrte_workflows$ETL_Prozesslaeufe_ID, function(id) {
    btn <- paste0("workflow_btn_", id)
    observeEvent(input[[btn]], {
      process_id(id)
      show_etl_modal(id)
    }, ignoreInit = TRUE)
  })
})

#----Extract workflow ID per button-----
workflow_id <- reactive({
  req(process_id(), input$prozess_tabs)

  if (input$prozess_tabs == "exec") {
    exec_workflows %>%
      filter(ETL_Prozesslaeufe_ID == process_id()) %>%
      pull(ETL_Workflow_ID) %>%
      unique()
  } else if (input$prozess_tabs == 'letzte_ausführung') {
    letzte_ausgefuehrte_workflows %>%
      filter(ETL_Prozesslaeufe_ID == process_id()) %>%
      pull(ETL_Workflow_ID) %>%
      unique()
  }
})

# render only the error‐rows for the selected workflow
output$error_table <- DT::renderDT({
  req(workflows_error())
  workflows_error() %>%
    datatable(
      rownames = FALSE,
      options  = list(pageLength = 10, autoWidth = TRUE)
    )
})

#Etl Tree for corresponding button
output$etl <- renderVisNetwork({
  data <- req(etl())

  req(nrow(data) > 0)

  # 1) Farbdefinitionen
  level_colors   <- c(
    Workflow  = "#B4F4B0",
    Paket     = "#FFD09F",
    Umsetzung = "#CAAEFF",
    Schritt   = "#C2F2F2"
  )
  error_color    <- "#ff8d87"
  special_color  <- "#F5F5F5"  # z.B. mittelgrau für Erfolgreich == 2 (nicht ausgeführte Umsetzung,Pakete, Schritte)


  # 2) Daten säubern
  data <- data %>%
    mutate(
      Level       = trimws(as.character(Level)),
      Erfolgreich = as.integer(Erfolgreich)
    )

  req(nrow(data) > 0)

  # 3) Nodes mit case_when einfärben
  nodes <- data %>%
    transmute(
      id               = PK,
      label = paste0(ModulID, "\n", Modulname),
      group = Level,
      color.background = case_when(
        Erfolgreich == 0 ~ error_color,
        Erfolgreich == 1 ~ level_colors[data$Level],
        Erfolgreich == 2 ~ special_color
      ),
      color.border     = case_when(
        Erfolgreich == 0 ~ error_color,
        Erfolgreich == 1 ~ level_colors[data$Level],
        Erfolgreich == 2 ~ special_color
      )
    )

  req(nrow(nodes) > 0)

  # 4) Edges analog einfärben (Ziel-Knoten-Status)
  edges <- data.frame(
    from   = data$Parent_PK,
    to     = data$PK,
    arrows = "to",
    color  = case_when(
      data$Erfolgreich == 0 ~ error_color,
      data$Erfolgreich == 1 ~ level_colors[data$Level],
      data$Erfolgreich == 2 ~ special_color
    )
  )

  req(nrow(edges) > 0)


  visNetwork(nodes, edges, width = "100%", height = "100%") %>%
    # eigene Legende ohne automatische Gruppenzuordnung
    visLegend(
      position = "right",
      useGroups = FALSE,
      addNodes  = rbind(
        data.frame(
          label             = names(level_colors),
          shape             = "box",
          color.background  = unname(level_colors),
          color.border      = unname(level_colors)
        ),
        data.frame(
          label             = c("Fehler", "Nicht ausgeführt"),
          shape             = "box",
          color.background  = c(error_color, special_color),
          color.border      = c(error_color, special_color)
        )
      )
    ) %>%
    visNodes(shape = "box", shadow = TRUE) %>%
    visEdges(smooth = FALSE) %>%
    visOptions(
      collapse         = list(enabled = TRUE),
      manipulation     = FALSE,
      highlightNearest = TRUE,
      nodesIdSelection = FALSE
      #selectedBy       = "label"
    ) %>%
    visHierarchicalLayout(direction = "UD",
                          sortMethod = "directed",
                          levelSeparation     = 150,
                          nodeSpacing         = 200,
                          treeSpacing         = 200,
                          blockShifting       = TRUE,
                          parentCentralization= TRUE) %>%
    visInteraction(dragNodes = FALSE, dragView = TRUE, zoomView = TRUE) %>%
    # enforce repulsion so boxes don’t overlap
    visPhysics(
      solver = "hierarchicalRepulsion",
      hierarchicalRepulsion = list(
        nodeDistance = 200
      ),
      stabilization = list(
        enabled    = TRUE,
        iterations = 1000
      )
    ) %>%
    visEvents(
      select = "function(e) {
        if (e.nodes.length == 1) {
          Shiny.setInputValue('etl_selected_node', e.nodes[0], {priority: 'event'});
        }
      }"
    ) %>%
    onRender(
      jsCode = "
    function(el, x) {
      var network = this;

      // if we've already wired up this element, bail out
      if (el._hasVisResize) return;
      el._hasVisResize = true;

      // build a resize function specific to this 'el'
      el._resizeVis = function() {
        var mb = el.closest('.modal-body');
        if (!mb) return;
        el.style.height = mb.clientHeight + 'px';
        network.redraw();
      };

      // call it once immediately
      setTimeout(el._resizeVis, 0);

      // whenever the window changes size
      window.addEventListener('resize', el._resizeVis);

      // whenever *any* bootstrap modal is shown, if it's this one, resize
      $(document).on('shown.bs.modal', function(evt) {
        if (evt.target.contains(el)) {
          el._resizeVis();
        }
      });
    }
    ",
    # give each widget its own data payload so htmlwidgets re-runs this code
    data = list(id = "etl", t = Sys.time())
    )
})

# Anzeige der Auswahl
output$selected_node <- renderPrint(input$etl_tree)

# Highlight im Netzwerk
observeEvent(input$etl_tree, {
  visNetworkProxy("etl") %>%
    visSelectNodes(id = input$etl_tree)
})

output$workflow_buttons_planned <- renderUI({

  if (nrow(planned_workflows) == 0) {
    div(class = "no-process", "🔍 Keine geplanten Prozesse")
  } else {

    # Sort the data frame by Anforderungszeitpunkt
    # sorted_planned_workflows <- planned_workflows[order(planned_workflows$Anforderungszeitpunkt), ]

    # Use lapply to create a list of buttons:
    buttons <- lapply(seq_len(nrow(planned_workflows)), function(i) {

      formatted_time <- format(
        as.POSIXct(planned_workflows$Anforderungszeitpunkt[i], tz = "UTC"),
        "%Y-%m-%d %H:%M:%S"
      )

      start_end <- sprintf(
        "<div class='planned_workflows'
    title='Anforderungszeitpunkt: %1$s'
    style='display:inline-block; padding:5px 10px;
    border:2px solid #CCCCCC; border-radius:5px;'>
    <span style='margin-right:15px;'>
      <i class='fa fa-clock-o'></i> %1$s
    </span>
  </div>",
  formatted_time
      )


      # Compose the complete label with workflow title and the duration rectangle underneath.
      full_label <- HTML(paste0(
        "<strong>Workflow ", planned_workflows$ETL_Workflow_ID[i], ": ", planned_workflows$Workflowname[i],
        "</strong>  ", start_end))


      actionBttn(
        inputId = paste0("workflow_", planned_workflows$ETL_Workflow_ID[i]),
        label   = full_label,
        style   = "stretch",
        color   = "default",
        size    = "lg"
      )
    })

    do.call(fluidRow, buttons)
  }

})

# for nav panel
output$etl_panel <- renderVisNetwork({
  data <- req(wf_data())
  req(nrow(data) > 0)

  level_colors   <- c(
    Workflow  = "#B4F4B0",
    Paket     = "#FFD09F",
    Umsetzung = "#CAAEFF",
    Schritt   = "#C2F2F2"
  )

  nodes <-
    data %>%
    transmute(id = label,
              label = modulname,
              group = Level,
              color.background = level_colors[Level],
              color.border = level_colors[Level])

  req(nrow(nodes) > 0)

  edges <-
    data.frame(
      from = data$parent,
      to = data$label,
      arrows = rep("to", length(data$parent))
    )

  req(nrow(edges) > 0)


  visNetwork(nodes, edges, width = "100%") %>%
    visLegend(
      useGroups = FALSE,
      addNodes  =
        data.frame(
          label             = names(level_colors),
          shape             = "box",
          color.background  = unname(level_colors),
          color.border      = unname(level_colors)
        )
    ) %>%
    visNodes(shadow = TRUE, shape = "box", size = 25) %>%
    visOptions(
      collapse = list(enabled = TRUE),
      manipulation = FALSE,
      highlightNearest = TRUE,
      nodesIdSelection = TRUE
    ) %>%
    visHierarchicalLayout(
      direction        = "UD",       # top-down
      sortMethod       = "directed", # Kanten-Orientierung beachten
      levelSeparation  = 150,        # Abstand der Hierarchie-Ebenen (vertikal)
      nodeSpacing      = 200,        # Mindestabstand zwischen Nodes (horizontal)
      treeSpacing      = 200,        # Abstand zwischen Subtrees
      blockShifting    = TRUE,       # beugt Zusammenrücken vor
      parentCentralization = TRUE    # Eltern mittig anordnen
    ) %>%
    # add physics so nodes repel each other
    visPhysics(
      solver                 = "hierarchicalRepulsion",
      hierarchicalRepulsion  = list(
        nodeDistance = 200
      )
    )

})

output$timeline <- renderTimevis({
  df <- etl()
  # Don't run until we have data
  req(nrow(df) > 0)

  df <-
    df %>%
    dplyr::filter(!is.na(Startzeitpunkt))

  level_colors   <- c(
    Workflow  = "#B4F4B0",
    Paket     = "#FFD09F",
    Umsetzung = "#CAAEFF",
    Schritt   = "#C2F2F2"
  )

  # Build a timevis-compatible groups data.frame from parent–child edges
  # edges: data.frame with columns `id` and `parent` (parent = NA/"" for roots)
  # content: optional column name in `edges` to use as label; otherwise labels default to id
  # sort_children: sort each parent's children by "content" or "id"
  build_timevis_groups <- function(
    edges,
    id_col = "id",
    parent_col = "parent",
    content_col = NULL,
    sort_children = TRUE,
    child_sort_by = c("content", "id")
  ) {
    stopifnot(is.data.frame(edges))
    if (!all(c(id_col, parent_col) %in% names(edges))) {
      stop("`edges` must contain columns: ", id_col, " and ", parent_col)
    }

    child_sort_by <- match.arg(child_sort_by)

    # Normalize columns
    ids_raw    <- as.character(edges[[id_col]])
    parents_raw <- edges[[parent_col]]
    parents_raw[is.na(parents_raw) | parents_raw == ""] <- NA_character_
    parents_raw <- as.character(parents_raw)

    # Ensure uniqueness of (id, parent) pairs and single-parent property
    key <- paste0(ids_raw, "||", parents_raw)
    if (any(duplicated(key))) stop("Duplicate (id, parent) pairs detected.")
    # Enforce at most one parent per id (tree/forest structure)
    parent_count <- tapply(parents_raw, ids_raw, function(x) length(unique(na.omit(x))))
    if (any(parent_count > 1, na.rm = TRUE)) {
      offenders <- names(parent_count)[parent_count > 1]
      stop("Each id must have at most one parent. Offenders: ", paste(offenders, collapse = ", "))
    }

    # All nodes: union of ids and referenced parents
    nodes <- unique(c(ids_raw, na.omit(parents_raw)))
    nodes <- nodes[!is.na(nodes)]

    # Map id -> parent (single parent)
    parent_of <- setNames(rep(NA_character_, length(nodes)), nodes)
    # For nodes that appear as children, set their parent
    parent_of[ids_raw] <- parents_raw

    # Optional content labels
    if (!is.null(content_col)) {
      if (!content_col %in% names(edges)) stop("content_col not found in `edges`.")
      content_map <- setNames(as.character(edges[[content_col]]), ids_raw)
      # For nodes that only appear as parents, fall back to their id
      content <- setNames(ifelse(nodes %in% names(content_map), content_map[nodes], nodes), nodes)
    } else {
      content <- setNames(nodes, nodes)
    }

    # Build adjacency: parent -> children
    children_list <- split(ids_raw, parents_raw)
    # Ensure every node has an entry (possibly empty)
    for (n in nodes) if (is.null(children_list[[n]])) children_list[[n]] <- character(0)

    # Simple cycle detection and depth computation
    depth <- setNames(rep(NA_integer_, length(nodes)), nodes)

    compute_depth <- local({
      visiting <- new.env(parent = emptyenv())
      f <- function(n) {
        if (!is.na(depth[[n]])) return(depth[[n]])
        if (!is.null(visiting[[n]]) && visiting[[n]]) {
          stop("Cycle detected involving node: ", n)
        }
        visiting[[n]] <- TRUE
        p <- parent_of[[n]]
        d <- if (is.na(p) || !nzchar(p)) 0L else 1L + f(p)
        depth[[n]] <- d
        visiting[[n]] <- FALSE
        d
      }
      f
    })
    invisible(lapply(nodes, compute_depth))

    # Order nodes by depth, then by label for stable display
    ord <- order(depth[nodes], content[nodes], nodes)
    nodes <- nodes[ord]

    # Optionally sort each parent's children
    if (isTRUE(sort_children)) {
      for (p in names(children_list)) {
        kids <- children_list[[p]]
        if (length(kids)) {
          if (child_sort_by == "content") {
            kids <- kids[order(content[kids], kids)]
          } else {
            kids <- sort(kids)
          }
          children_list[[p]] <- kids
        }
      }
    }

    # Assemble nestedGroups list-column
    nested <- lapply(nodes, function(n) {
      kids <- children_list[[n]]
      if (length(kids) == 0) return(NULL)
      as.character(kids)
    })

    data.frame(
      id = nodes,
      content = unname(content[nodes]),
      nestedGroups = I(nested),
      stringsAsFactors = FALSE
    )
  }

  groups <- build_timevis_groups(edges=df,
                                 id_col = "PK",
                                 parent_col = "Parent_PK",
                                 content_col = "Modulname") %>%
    dplyr::left_join(
      df %>% dplyr::select(PK, Level),
      by = c("id" = "PK")
    ) %>%
    dplyr::mutate(
      style = paste0("background-color:", level_colors[Level],"; border:none;")
    )


  # Prepare items
  timeline_data <- df %>%
    dplyr::transmute(
      start   = Startzeitpunkt,
      end     = Endzeitpunkt,
      group   = PK,
      ModulID,
      Modulname,
      Level,
      content = paste0(ModulID, "\n", Modulname)
    )

  # Render timeline
  timevis(data = timeline_data,
          groups = groups,
          options = list(
            groupOrder = htmlwidgets::JS("function(a,b) {return a.content > b.content;}"),
            template = htmlwidgets::JS("
      function(item) {
        return '<div style=\"color:white;\">' + item.content + '</div>';
      }
    ")
          )
  )

})


}



shinyApp(ui = ui, server = server)

