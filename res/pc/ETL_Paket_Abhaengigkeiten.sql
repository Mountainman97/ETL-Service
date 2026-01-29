USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Paket_Abhaengigkeiten]    Script Date: 04.02.2025 11:31:30 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Paket_Abhaengigkeiten](
    [ETL_Paket_Abhaengigkeiten_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [ETL_Workflow_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Pakete_ID] [dbo].[ReferenzID] NOT NULL,
    [Vorlauf_ETL_Pakete_ID] [dbo].[ReferenzID] NOT NULL,
    [Information] [dbo].[Kurztext] NOT NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Paket_Abhaengigkeiten_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Paket_Abhaengigkeiten_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Paket_Abhaengigkeiten_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Paket_Abhaengigkeiten_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] ADD  CONSTRAINT [DF_ETL_Paket_Abhaengigkeiten_Ist_aktiv]  DEFAULT ((1)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] ADD  CONSTRAINT [DF__ETL_Paket_Abhaengigkeiten__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] ADD  CONSTRAINT [DF__ETL_Paket_Abhaengigkeiten__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] ADD  CONSTRAINT [DF__ETL_Paket_Abhaengigkeiten__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] ADD  CONSTRAINT [DF__ETL_Paket_Abhaengigkeiten__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten]  WITH CHECK ADD  CONSTRAINT [FK__ETL_Paket__Abhae__7A3223E8] FOREIGN KEY([Vorlauf_ETL_Pakete_ID])
REFERENCES [pc].[ETL_Pakete] ([ETL_Pakete_ID])
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] CHECK CONSTRAINT [FK__ETL_Paket__Abhae__7A3223E8]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten]  WITH CHECK ADD  CONSTRAINT [FK__ETL_Paket__ETL_P__793DFFAF] FOREIGN KEY([ETL_Pakete_ID])
REFERENCES [pc].[ETL_Pakete] ([ETL_Pakete_ID])
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] CHECK CONSTRAINT [FK__ETL_Paket__ETL_P__793DFFAF]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paket_Abhaengigkeiten_Datenherkunft] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] CHECK CONSTRAINT [FK_ETL_Paket_Abhaengigkeiten_Datenherkunft]
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paket_Abhaengigkeiten_ETL_Workflow] FOREIGN KEY([ETL_Workflow_ID])
REFERENCES [pc].[ETL_Workflow] ([ETL_Workflow_ID])
GO

ALTER TABLE [pc].[ETL_Paket_Abhaengigkeiten] CHECK CONSTRAINT [FK_ETL_Paket_Abhaengigkeiten_ETL_Workflow]
GO


