USE [DIZ_NET]
GO

/****** Object:  Table [Logging].[ETL_Fehlermeldungen]    Script Date: 04.11.2024 15:39:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [Logging].[ETL_Fehlermeldungen](
    [ETL_Fehlermeldungen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [ETL_Prozesslaeufe_ID] [dbo].[ReferenzID] NULL,
    [ETL_Paket_Prozesslaeufe_ID] [dbo].[ReferenzID] NULL,
    [ETL_Paketumsetzung_Prozesslaeufe_ID] [dbo].[ReferenzID] NULL,
    [ETL_Paketschritt_Prozesslaeufe_ID] [dbo].[ReferenzID] NULL,
    [Fehlertyp] [dbo].[Memo] NOT NULL,
    [Meldungstext] [dbo].[Memo] NOT NULL,
    [Fehlertext] [dbo].[Memo] NULL,
    [Fehlernummer] [dbo].[Kuerzel] NULL,
    [Fehlerzeile] [dbo].[Kuerzel] NULL,
    [Schweregrad] [dbo].[Kuerzel] NOT NULL,
    [Stacktrace] [dbo].[Memo] NULL,
    [Prozedur] [dbo].[Langtext] NULL,
    [Fehlerquelle] [dbo].[Langtext] NULL,
    [Fehlerstatus] [dbo].[Memo] NULL,
    [Fehlerobjekt] [dbo].[Langtext] NULL,
    [Ist_transferiert] [dbo].[Flag] NOT NULL,
    [Json_Log] [dbo].[Memo] NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Fehlermeldungen_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Fehlermeldungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Fehlermeldungen_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Fehlermeldungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX] TEXTIMAGE_ON [INDEX]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] ADD  CONSTRAINT [DF__ETL_Fehlermeldungen__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] ADD  CONSTRAINT [DF__ETL_Fehlermeldungen__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] ADD  CONSTRAINT [DF__ETL_Fehlermeldungen__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] ADD  CONSTRAINT [DF__ETL_Fehlermeldungen__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Fehlermeldungen_ETL_Paket_Prozesslaeufe] FOREIGN KEY([ETL_Paket_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Paket_Prozesslaeufe] ([ETL_Paket_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] CHECK CONSTRAINT [FK_ETL_Fehlermeldungen_ETL_Paket_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Fehlermeldungen_ETL_Paketschritt_Prozesslaeufe] FOREIGN KEY([ETL_Paketschritt_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Paketschritt_Prozesslaeufe] ([ETL_Paketschritt_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] CHECK CONSTRAINT [FK_ETL_Fehlermeldungen_ETL_Paketschritt_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Fehlermeldungen_ETL_Paketumsetzung_Prozesslaeufe] FOREIGN KEY([ETL_Paketumsetzung_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Paketumsetzung_Prozesslaeufe] ([ETL_Paketumsetzung_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] CHECK CONSTRAINT [FK_ETL_Fehlermeldungen_ETL_Paketumsetzung_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Fehlermeldungen_ETL_Prozesslaeufe] FOREIGN KEY([ETL_Prozesslaeufe_ID])
REFERENCES [Logging].[ETL_Prozesslaeufe] ([ETL_Prozesslaeufe_ID])
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] CHECK CONSTRAINT [FK_ETL_Fehlermeldungen_ETL_Prozesslaeufe]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen]  WITH NOCHECK ADD  CONSTRAINT [CK__ETL_Fehle__Fehle__75C27486] CHECK  (([Fehlertyp]='Dienst' OR [Fehlertyp]='SQL' OR [Fehlertyp]='Workflow'))
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] CHECK CONSTRAINT [CK__ETL_Fehle__Fehle__75C27486]
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen]  WITH NOCHECK ADD  CONSTRAINT [ETL_Fehlermeldungen_Json_Log record should be formatted as JSON] CHECK  ((isjson([Json_Log])=(1)))
GO

ALTER TABLE [Logging].[ETL_Fehlermeldungen] CHECK CONSTRAINT [ETL_Fehlermeldungen_Json_Log record should be formatted as JSON]
GO


