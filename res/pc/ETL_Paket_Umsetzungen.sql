USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Paket_Umsetzungen]    Script Date: 04.02.2025 11:32:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Paket_Umsetzungen](
    [ETL_Paket_Umsetzungen_ID] [dbo].[ReferenzID] NOT NULL,
    [Umsetzungsname] [dbo].[Kuerzel] NULL,
    [Information] [dbo].[Kurztext] NOT NULL,
    [Anzahl_Parallele_Schritte] [dbo].[Ganzzahl] NOT NULL,
    [ETL_Konfigurationen_ID] [dbo].[ReferenzID] NOT NULL,
    [Parallelsperre] [dbo].[Flag] NOT NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Paket_Umsetzungen_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Paket_Umsetzungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Paket_Umsetzungen_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Paket_Umsetzungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen] ADD  CONSTRAINT [DF_ETL_Paket_Umsetzungen_Parallelsperre]  DEFAULT ((0)) FOR [Parallelsperre]
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen] ADD  CONSTRAINT [DF_ETL_Paket_Umsetzungen_Ist_aktiv]  DEFAULT ((1)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen] ADD  CONSTRAINT [DF__ETL_Paket_Umsetzungen__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen] ADD  CONSTRAINT [DF__ETL_Paket_Umsetzungen__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen] ADD  CONSTRAINT [DF__ETL_Paket_Umsetzungen__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen] ADD  CONSTRAINT [DF__ETL_Paket_Umsetzungen__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paket_Umsetzungen_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen] CHECK CONSTRAINT [FK_ETL_Paket_Umsetzungen_Datenherkunft_ID]
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paket_Umsetzungen_ETL_Konfigurationen] FOREIGN KEY([ETL_Konfigurationen_ID])
REFERENCES [pc].[ETL_Konfigurationen] ([ETL_Konfigurationen_ID])
GO

ALTER TABLE [pc].[ETL_Paket_Umsetzungen] CHECK CONSTRAINT [FK_ETL_Paket_Umsetzungen_ETL_Konfigurationen]
GO


