USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Pakete_Paketumsetzungen]    Script Date: 04.02.2025 11:33:21 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Pakete_Paketumsetzungen](
    [ETL_Pakete_Paketumsetzungen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [ETL_Workflow_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Pakete_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Paket_Umsetzungen_ID] [dbo].[ReferenzID] NOT NULL,
    [Paket_Priorisierung] [dbo].[Ganzzahl] NOT NULL,
    [Mandanten_ID] [dbo].[ReferenzID] NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Pakete_Paketumsetzungen] PRIMARY KEY CLUSTERED 
(
    [ETL_Pakete_Paketumsetzungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] ADD  CONSTRAINT [DF_ETL_Pakete_Paketumsetzungen_Ist_aktiv]  DEFAULT ((1)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] ADD  CONSTRAINT [DF_ETL_Pakete_Paketumsetzungen_Anlagedatum]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] ADD  CONSTRAINT [DF_ETL_Pakete_Paketumsetzungen_Anlage_Nutzer]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] ADD  CONSTRAINT [DF_ETL_Pakete_Paketumsetzungen_Letzte_Aenderung]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] ADD  CONSTRAINT [DF_ETL_Pakete_Paketumsetzungen_Letzte_Aenderung_Nutzer]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_Datenherkunft] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] CHECK CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_Datenherkunft]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_ETL_Paket_Umsetzungen] FOREIGN KEY([ETL_Paket_Umsetzungen_ID])
REFERENCES [pc].[ETL_Paket_Umsetzungen] ([ETL_Paket_Umsetzungen_ID])
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] CHECK CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_ETL_Paket_Umsetzungen]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_ETL_Pakete] FOREIGN KEY([ETL_Pakete_ID])
REFERENCES [pc].[ETL_Pakete] ([ETL_Pakete_ID])
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] CHECK CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_ETL_Pakete]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_ETL_Workflow] FOREIGN KEY([ETL_Workflow_ID])
REFERENCES [pc].[ETL_Workflow] ([ETL_Workflow_ID])
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] CHECK CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_ETL_Workflow]
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_Mandanten] FOREIGN KEY([Mandanten_ID])
REFERENCES [conf].[Mandanten] ([Mandanten_ID])
GO

ALTER TABLE [pc].[ETL_Pakete_Paketumsetzungen] CHECK CONSTRAINT [FK_ETL_Pakete_Paketumsetzungen_Mandanten]
GO


