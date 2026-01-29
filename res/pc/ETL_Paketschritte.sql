USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Paketschritte]    Script Date: 04.02.2025 11:36:20 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Paketschritte](
    [ETL_Paketschritte_ID] [dbo].[ReferenzID] NOT NULL,
    [Schrittname] [dbo].[Kuerzel] NULL,
    [Information] [dbo].[Kurztext] NOT NULL,
    [Aufgabentyp] [dbo].[Kuerzel] NOT NULL,
    [Befehlstyp] [dbo].[Kuerzel] NOT NULL,
    [Befehl] [dbo].[Memo] NOT NULL,
    [Zieltabelle] [dbo].[Memo] NOT NULL,
    [Parallelsperre] [dbo].[Flag] NOT NULL,
    [Zeitscheibe] [dbo].[Flag] NOT NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Paketschritte_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Paketschritte_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Paketschritte_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Paketschritte_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX] TEXTIMAGE_ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Paketschritte] ADD  CONSTRAINT [DF_ETL_Paketschritte_Parallelsperre]  DEFAULT ((0)) FOR [Parallelsperre]
GO

ALTER TABLE [pc].[ETL_Paketschritte] ADD  CONSTRAINT [DF_ETL_Paketschritte_Zeitscheibe]  DEFAULT ((0)) FOR [Zeitscheibe]
GO

ALTER TABLE [pc].[ETL_Paketschritte] ADD  CONSTRAINT [DF_ETL_Paketschritte_Ist_aktiv]  DEFAULT ((1)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Paketschritte] ADD  CONSTRAINT [DF__ETL_Paketschritte__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Paketschritte] ADD  CONSTRAINT [DF__ETL_Paketschritte__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paketschritte] ADD  CONSTRAINT [DF__ETL_Paketschritte__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Paketschritte] ADD  CONSTRAINT [DF__ETL_Paketschritte__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paketschritte]  WITH NOCHECK ADD  CONSTRAINT [FK_ETL_Paketschritte_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Paketschritte] CHECK CONSTRAINT [FK_ETL_Paketschritte_Datenherkunft_ID]
GO

ALTER TABLE [pc].[ETL_Paketschritte]  WITH NOCHECK ADD  CONSTRAINT [CK__ETL_Paket__Aufga__6AEFE058] CHECK  (([Aufgabentyp]='SQL' OR [Aufgabentyp]='EXCEL' OR [Aufgabentyp]='CSV'))
GO

ALTER TABLE [pc].[ETL_Paketschritte] CHECK CONSTRAINT [CK__ETL_Paket__Aufga__6AEFE058]
GO

ALTER TABLE [pc].[ETL_Paketschritte]  WITH NOCHECK ADD  CONSTRAINT [CK__ETL_Paket__Befeh__6BE40491] CHECK  (([Befehlstyp]='EXEC' OR [Befehlstyp]='SQL_TARGET' OR [Befehlstyp]='SQL_SOURCE' OR [Befehlstyp]='COPY' OR [Befehlstyp]='TRANSFER' OR [Befehlstyp]='TEST'))
GO

ALTER TABLE [pc].[ETL_Paketschritte] CHECK CONSTRAINT [CK__ETL_Paket__Befeh__6BE40491]
GO

ALTER TABLE [pc].[ETL_Paketschritte]  WITH NOCHECK ADD  CONSTRAINT [CK__ETL_Paket__Ist_a__6442E2C9] CHECK  (([Ist_aktiv]=(1) OR [Ist_aktiv]=(0)))
GO

ALTER TABLE [pc].[ETL_Paketschritte] CHECK CONSTRAINT [CK__ETL_Paket__Ist_a__6442E2C9]
GO


