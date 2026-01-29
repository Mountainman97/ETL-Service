USE [DIZ_NET]
GO

/****** Object:  Table [Logging].[ETL_Paketumsetzung_Prozesslaeufe]    Script Date: 28.02.2025 15:45:08 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe](
    [ETL_Paketumsetzung_Prozesslaeufe_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [ETL_Prozesslaeufe_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Paket_Prozesslaeufe_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Paket_Umsetzungen_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Konfigurationen_ID] [dbo].[ReferenzID] NULL,
    [Quell_ETL_Verbindungen_ID] [dbo].[ReferenzID] NULL,
    [Ziel_ETL_Verbindungen_ID] [dbo].[ReferenzID] NULL,
    [Mandanten_ID] [dbo].[ReferenzID] NULL,
    [Anforderungszeitpunkt] [dbo].[DatumZeit] NOT NULL,
    [Startzeitpunkt] [dbo].[DatumZeit] NULL,
    [Ausfuehrungsstartzeitpunkt] [dbo].[DatumZeit] NULL,
    [Ausfuehrungsendzeitpunkt] [dbo].[DatumZeit] NULL,
    [Endzeitpunkt] [dbo].[DatumZeit] NULL,
    [Ist_gestartet] [dbo].[Flag] NOT NULL,
    [Ist_abgeschlossen] [dbo].[Flag] NOT NULL,
    [Erfolgreich] [dbo].[Flag] NOT NULL,
    [Parallelsperre] [dbo].[Flag] NOT NULL,
    [Ist_transferiert] [dbo].[Flag] NOT NULL,
    [Json_Log] [dbo].[Memo] NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Paketumsetzung_Prozesslaeufe_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Paketumsetzung_Prozesslaeufe_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Paketumsetzung_Prozesslaeufe_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Paketumsetzung_Prozesslaeufe_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX] TEXTIMAGE_ON [INDEX]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF__ETL_Paketumsetzung_Prozesslaeufe__Anfor__1A34DF26]  DEFAULT (getdate()) FOR [Anforderungszeitpunkt]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF__ETL_Paketumsetzung_Prozesslaeufe__Ist_g__1B29035F]  DEFAULT ((0)) FOR [Ist_gestartet]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF__ETL_Paketumsetzung_Prozesslaeufe__Ist_a__1C1D2798]  DEFAULT ((0)) FOR [Ist_abgeschlossen]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF_ETL_Paketumsetzung_Prozesslaeufe_Erfolgreich]  DEFAULT ((0)) FOR [Erfolgreich]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF_ETL_Paketumsetzung_Prozesslaeufe_Parallellsperre]  DEFAULT ((0)) FOR [Parallelsperre]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF__ETL_Paketumsetzung_Prozesslaeufe__Ist_t__1D114BD1]  DEFAULT ((0)) FOR [Ist_transferiert]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF__ETL_Paketumsetzung_Prozesslaeufe__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF__ETL_Paketumsetzung_Prozesslaeufe__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF__ETL_Paketumsetzung_Prozesslaeufe__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ADD  CONSTRAINT [DF__ETL_Paketumsetzung_Prozesslaeufe__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paketumsetzung_Prozesslaeufe_ETL_Paket_Prozesslaeufe] FOREIGN KEY([ETL_Paket_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Paket_Prozesslaeufe] ([ETL_Paket_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] CHECK CONSTRAINT [FK_ETL_Paketumsetzung_Prozesslaeufe_ETL_Paket_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paketumsetzung_Prozesslaeufe_ETL_Prozesslaeufe] FOREIGN KEY([ETL_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Prozesslaeufe] ([ETL_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] CHECK CONSTRAINT [FK_ETL_Paketumsetzung_Prozesslaeufe_ETL_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paketumsetzung_Prozesslaeufe_Mandanten] FOREIGN KEY([Mandanten_ID])
REFERENCES [conf].[Mandanten] ([Mandanten_ID])
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] CHECK CONSTRAINT [FK_ETL_Paketumsetzung_Prozesslaeufe_Mandanten]
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe]  WITH CHECK ADD  CONSTRAINT [ETL_Paketumsetzung_Prozesslaeufe_Json_Log record should be formatted as JSON] CHECK  ((isjson([Json_Log])=(1)))
GO

ALTER TABLE [Logging].[ETL_Paketumsetzung_Prozesslaeufe] CHECK CONSTRAINT [ETL_Paketumsetzung_Prozesslaeufe_Json_Log record should be formatted as JSON]
GO


