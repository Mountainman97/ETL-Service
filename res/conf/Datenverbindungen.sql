USE [DIZ_NET]
GO

/****** Object:  Table [conf].[Datenverbindungen]    Script Date: 05.11.2024 15:04:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [conf].[Datenverbindungen](
    [Datenverbindungen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Datenquellentypen_ID] [dbo].[ReferenzID] NOT NULL,
    [Kommunikationstypen_ID] [dbo].[ReferenzID] NOT NULL,
    [Verbindung] [dbo].[Kuerzel] NOT NULL,
    [Bezeichnung] [dbo].[Kurztext] NOT NULL,
    [Verbindungsarten_ID] [dbo].[ReferenzID] NOT NULL,
    [IP_Adresse] [dbo].[Kuerzel] NULL,
    [Hostname] [dbo].[Kuerzel] NULL,
    [Netzwerkport] [dbo].[Ganzzahl] NULL,
    [Serverinstanz] [dbo].[Kuerzel] NULL,
    [Verbindungszeichenkette] [dbo].[Langtext] NULL,
    [Benutzer] [dbo].[Kurztext] NULL,
    [Kennwort] [dbo].[Kurztext] NULL,
    [Datenbankschema] [dbo].[Kuerzel] NULL,
    [Encoding_Codepage] [dbo].[Kuerzel] NULL,
    [Verbindungs_Timeout] [dbo].[Ganzzahl] NULL,
    [Abfrage_Timeout] [dbo].[Ganzzahl] NULL,
    [Verbindungsversuche] [dbo].[Ganzzahl] NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Datenverbindungen_ID] PRIMARY KEY CLUSTERED 
(
    [Verbindung] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [Datenverbindungen_UC] UNIQUE NONCLUSTERED 
(
    [Datenverbindungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [conf].[Datenverbindungen] ADD  CONSTRAINT [DF__Datenverbindungen__66603565]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [conf].[Datenverbindungen] ADD  CONSTRAINT [DF__Datenverbindungen__6754599E]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [conf].[Datenverbindungen] ADD  CONSTRAINT [DF__Datenverbindungen__68487DD7]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [conf].[Datenverbindungen] ADD  CONSTRAINT [DF__Datenverbindungen__693CA210]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [conf].[Datenverbindungen]  WITH CHECK ADD  CONSTRAINT [FK__Datenverbindungen__1D7B6025] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [conf].[Datenverbindungen] CHECK CONSTRAINT [FK__Datenverbindungen__1D7B6025]
GO

ALTER TABLE [conf].[Datenverbindungen]  WITH CHECK ADD  CONSTRAINT [FK_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [conf].[Datenverbindungen] CHECK CONSTRAINT [FK_Datenherkunft_ID]
GO

ALTER TABLE [conf].[Datenverbindungen]  WITH CHECK ADD  CONSTRAINT [FK_Kommunikationstypen_ID] FOREIGN KEY([Kommunikationstypen_ID])
REFERENCES [conf].[Datenquellentypen] ([Datenquellen_Typ_ID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [conf].[Datenverbindungen] CHECK CONSTRAINT [FK_Kommunikationstypen_ID]
GO

ALTER TABLE [conf].[Datenverbindungen]  WITH CHECK ADD  CONSTRAINT [FK_Verbindungsarten_ID] FOREIGN KEY([Verbindungsarten_ID])
REFERENCES [conf].[Verbindungsarten] ([Verbindungsarten_ID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [conf].[Datenverbindungen] CHECK CONSTRAINT [FK_Verbindungsarten_ID]
GO


