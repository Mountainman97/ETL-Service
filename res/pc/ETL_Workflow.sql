USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Workflow]    Script Date: 04.02.2025 11:39:56 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Workflow](
    [ETL_Workflow_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Zeitplaene_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Pakete_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Fallback_Pakete_ID] [dbo].[ReferenzID] NULL,
    [Workflowname] [dbo].[Kuerzel] NOT NULL,
    [Information] [dbo].[Kurztext] NOT NULL,
    [Uebernahme_von] [dbo].[DatumZeit] NULL,
    [Uebernahme_bis] [dbo].[DatumZeit] NULL,
    [Uebernahme_Tage_Rueckwirkend] [dbo].[Ganzzahl] NULL,
    [Parallelsperre] [dbo].[Flag] NOT NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Workflow_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Workflow_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Workflow_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Workflow_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Workflow] ADD  CONSTRAINT [DF_ETL_Workflow_Parallelsperre]  DEFAULT ((0)) FOR [Parallelsperre]
GO

ALTER TABLE [pc].[ETL_Workflow] ADD  CONSTRAINT [DF_ETL_Workflow_Ist_aktiv]  DEFAULT ((0)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Workflow] ADD  CONSTRAINT [DF__ETL_Workflow__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Workflow] ADD  CONSTRAINT [DF__ETL_Workflow__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Workflow] ADD  CONSTRAINT [DF__ETL_Workflow__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Workflow] ADD  CONSTRAINT [DF__ETL_Workflow__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Workflow]  WITH CHECK ADD  CONSTRAINT [FK__ETL_Workf__ETL_P__1C873BEC] FOREIGN KEY([ETL_Pakete_ID])
REFERENCES [pc].[ETL_Pakete] ([ETL_Pakete_ID])
GO

ALTER TABLE [pc].[ETL_Workflow] CHECK CONSTRAINT [FK__ETL_Workf__ETL_P__1C873BEC]
GO

ALTER TABLE [pc].[ETL_Workflow]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Workflow_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Workflow] CHECK CONSTRAINT [FK_ETL_Workflow_Datenherkunft_ID]
GO

ALTER TABLE [pc].[ETL_Workflow]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Workflow_ETL_Pakete] FOREIGN KEY([ETL_Fallback_Pakete_ID])
REFERENCES [pc].[ETL_Pakete] ([ETL_Pakete_ID])
GO

ALTER TABLE [pc].[ETL_Workflow] CHECK CONSTRAINT [FK_ETL_Workflow_ETL_Pakete]
GO

ALTER TABLE [pc].[ETL_Workflow]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Workflow_Zeitplaene_ID] FOREIGN KEY([ETL_Zeitplaene_ID])
REFERENCES [pc].[ETL_Zeitplaene] ([ETL_Zeitplaene_ID])
GO

ALTER TABLE [pc].[ETL_Workflow] CHECK CONSTRAINT [FK_ETL_Workflow_Zeitplaene_ID]
GO

ALTER TABLE [pc].[ETL_Workflow]  WITH CHECK ADD  CONSTRAINT [CK__ETL_Workflow] CHECK  (([Uebernahme_von] IS NULL AND [Uebernahme_bis] IS NULL AND [Uebernahme_Tage_Rueckwirkend] IS NOT NULL OR [Uebernahme_von] IS NOT NULL AND [Uebernahme_Tage_Rueckwirkend] IS NULL))
GO

ALTER TABLE [pc].[ETL_Workflow] CHECK CONSTRAINT [CK__ETL_Workflow]
GO


