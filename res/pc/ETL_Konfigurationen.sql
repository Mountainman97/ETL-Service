USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Konfigurationen]    Script Date: 04.02.2025 11:29:15 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Konfigurationen](
    [ETL_Konfigurationen_ID] [dbo].[ReferenzID] NOT NULL,
    [Konfiguration] [dbo].[Kuerzel] NOT NULL,
    [Bezeichnung] [dbo].[Kurztext] NOT NULL,
    [Quell_ETL_Verbindungen_ID] [dbo].[ReferenzID] NOT NULL,
    [Ziel_ETL_Verbindungen_ID] [dbo].[ReferenzID] NOT NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Konfigurationen_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Konfigurationen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Konfigurationen_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Konfigurationen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Konfigurationen] ADD  CONSTRAINT [DF_ETL_Konfigurationen_Ist_aktiv]  DEFAULT ((1)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Konfigurationen] ADD  CONSTRAINT [DF__ETL_Konfigurationen__6EF57B64]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Konfigurationen] ADD  CONSTRAINT [DF__ETL_Konfigurationen__6FE99F9A]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Konfigurationen] ADD  CONSTRAINT [DF__ETL_Konfigurationen__70DDC3D6]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Konfigurationen] ADD  CONSTRAINT [DF__ETL_Konfigurationen__71D1E880]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Konfigurationen]  WITH CHECK ADD  CONSTRAINT [FK__ETL_Konfigurationen__1D7B6025] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Konfigurationen] CHECK CONSTRAINT [FK__ETL_Konfigurationen__1D7B6025]
GO

ALTER TABLE [pc].[ETL_Konfigurationen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Konfigurationen_ETL_Verbindungen] FOREIGN KEY([Quell_ETL_Verbindungen_ID])
REFERENCES [pc].[ETL_Verbindungen] ([ETL_Verbindungen_ID])
GO

ALTER TABLE [pc].[ETL_Konfigurationen] CHECK CONSTRAINT [FK_ETL_Konfigurationen_ETL_Verbindungen]
GO

ALTER TABLE [pc].[ETL_Konfigurationen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Konfigurationen_ETL_Verbindungen1] FOREIGN KEY([Ziel_ETL_Verbindungen_ID])
REFERENCES [pc].[ETL_Verbindungen] ([ETL_Verbindungen_ID])
GO

ALTER TABLE [pc].[ETL_Konfigurationen] CHECK CONSTRAINT [FK_ETL_Konfigurationen_ETL_Verbindungen1]
GO


