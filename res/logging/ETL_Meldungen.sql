USE [DIZ_NET]
GO

/****** Object:  Table [Logging].[ETL_Meldungen]    Script Date: 04.11.2024 15:39:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [Logging].[ETL_Meldungen](
    [ETL_Meldungen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [ETL_Prozesslaeufe_ID] [dbo].[ReferenzID] NULL,
    [ETL_Paket_Prozesslaeufe_ID] [dbo].[ReferenzID] NULL,
    [ETL_Paketumsetzung_Prozesslaeufe_ID] [dbo].[ReferenzID] NULL,
    [ETL_Paketschritt_Prozesslaeufe_ID] [dbo].[ReferenzID] NULL,
    [Meldungstext] [dbo].[Memo] NOT NULL,
    [Ist_transferiert] [dbo].[Flag] NOT NULL,
    [Json_Log] [dbo].[Memo] NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Meldungen_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Meldungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Meldungen_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Meldungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX] TEXTIMAGE_ON [INDEX]
GO

ALTER TABLE [Logging].[ETL_Meldungen] ADD  CONSTRAINT [DF__ETL_Meldungen__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [Logging].[ETL_Meldungen] ADD  CONSTRAINT [DF__ETL_Meldungen__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [Logging].[ETL_Meldungen] ADD  CONSTRAINT [DF__ETL_Meldungen__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [Logging].[ETL_Meldungen] ADD  CONSTRAINT [DF__ETL_Meldungen__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [Logging].[ETL_Meldungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Meldungen_ETL_Paket_Prozesslaeufe] FOREIGN KEY([ETL_Paket_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Paket_Prozesslaeufe] ([ETL_Paket_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Meldungen] CHECK CONSTRAINT [FK_ETL_Meldungen_ETL_Paket_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Meldungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Meldungen_ETL_Paketschritt_Prozesslaeufe] FOREIGN KEY([ETL_Paketschritt_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Paketschritt_Prozesslaeufe] ([ETL_Paketschritt_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Meldungen] CHECK CONSTRAINT [FK_ETL_Meldungen_ETL_Paketschritt_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Meldungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Meldungen_ETL_Paketumsetzung_Prozesslaeufe] FOREIGN KEY([ETL_Paketumsetzung_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ([ETL_Paketumsetzung_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Meldungen] CHECK CONSTRAINT [FK_ETL_Meldungen_ETL_Paketumsetzung_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Meldungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Meldungen_ETL_Prozesslaeufe] FOREIGN KEY([ETL_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Prozesslaeufe] ([ETL_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Meldungen] CHECK CONSTRAINT [FK_ETL_Meldungen_ETL_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Meldungen]  WITH NOCHECK ADD  CONSTRAINT [ETL_Meldungen_Json_Log record should be formatted as JSON] CHECK  ((isjson([Json_Log])=(1)))
GO

ALTER TABLE [Logging].[ETL_Meldungen] CHECK CONSTRAINT [ETL_Meldungen_Json_Log record should be formatted as JSON]
GO


