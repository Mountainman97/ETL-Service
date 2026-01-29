USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Zeitplan_Ausfuehrungen]    Script Date: 04.02.2025 11:40:45 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Zeitplan_Ausfuehrungen](
	[ETL_Zeitplan_Ausfuehrungen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
	[ETL_Zeitplaene_ID] [dbo].[ReferenzID] NOT NULL,
	[ETL_Workflow_ID] [dbo].[ReferenzID] NOT NULL,
	[ETL_Pakete_ID] [dbo].[ReferenzID] NOT NULL,
	[Anforderungszeitpunkt] [dbo].[DatumZeit] NOT NULL,
	[Startzeitpunkt] [dbo].[DatumZeit] NULL,
	[Ausfuehrungsstartzeitpunkt] [dbo].[DatumZeit] NULL,
	[Endzeitpunkt] [dbo].[DatumZeit] NULL,
	[Ausgefuehrt] [dbo].[Flag] NOT NULL,
	[Erfolgreich] [dbo].[Flag] NOT NULL,
	[Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
	[Anlagedatum] [dbo].[DatumZeit] NOT NULL,
	[Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
	[Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
	[Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Zeitplan_Ausfuehrungen_ID] PRIMARY KEY CLUSTERED 
(
	[ETL_Zeitplan_Ausfuehrungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Zeitplan_Ausfuehrungen_UC] UNIQUE NONCLUSTERED 
(
	[ETL_Zeitplan_Ausfuehrungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] ADD  CONSTRAINT [DF__ETL_Zeitp__Anfor__3FD07829]  DEFAULT (getdate()) FOR [Anforderungszeitpunkt]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] ADD  CONSTRAINT [DF__ETL_Zeitp__Ausge__40C49C62]  DEFAULT ((0)) FOR [Ausgefuehrt]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] ADD  CONSTRAINT [DF_ETL_Zeitplan_Ausfuehrungen_Erfolgreich]  DEFAULT ((0)) FOR [Erfolgreich]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] ADD  CONSTRAINT [DF__ETL_Zeitplan_Ausfuehrungen__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] ADD  CONSTRAINT [DF__ETL_Zeitplan_Ausfuehrungen__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] ADD  CONSTRAINT [DF__ETL_Zeitplan_Ausfuehrungen__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] ADD  CONSTRAINT [DF__ETL_Zeitplan_Ausfuehrungen__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Zeitplan_Ausfuehrungen_Datenherkunft] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] CHECK CONSTRAINT [FK_ETL_Zeitplan_Ausfuehrungen_Datenherkunft]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Zeitplan_Ausfuehrungen_ETL_Pakete] FOREIGN KEY([ETL_Pakete_ID])
REFERENCES [pc].[ETL_Pakete] ([ETL_Pakete_ID])
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] CHECK CONSTRAINT [FK_ETL_Zeitplan_Ausfuehrungen_ETL_Pakete]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Zeitplan_Ausfuehrungen_Workflow_ID] FOREIGN KEY([ETL_Workflow_ID])
REFERENCES [pc].[ETL_Workflow] ([ETL_Workflow_ID])
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] CHECK CONSTRAINT [FK_ETL_Zeitplan_Ausfuehrungen_Workflow_ID]
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Zeitplan_Ausfuehrungen_Zeitplaene_ID] FOREIGN KEY([ETL_Zeitplaene_ID])
REFERENCES [pc].[ETL_Zeitplaene] ([ETL_Zeitplaene_ID])
GO

ALTER TABLE [pc].[ETL_Zeitplan_Ausfuehrungen] CHECK CONSTRAINT [FK_ETL_Zeitplan_Ausfuehrungen_Zeitplaene_ID]
GO


