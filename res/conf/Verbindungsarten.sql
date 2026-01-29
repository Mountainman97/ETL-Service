USE [DIZ_NET]
GO

/****** Object:  Table [conf].[Verbindungsarten]    Script Date: 07.11.2024 17:12:29 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [conf].[Verbindungsarten](
    [Verbindungsarten_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Typ] [dbo].[Kuerzel] NOT NULL,
    [Bezeichnung] [dbo].[Kurztext] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Verbindungsarten_ID] PRIMARY KEY CLUSTERED 
(
    [Typ] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [Verbindungsarten_UC] UNIQUE NONCLUSTERED 
(
    [Verbindungsarten_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [conf].[Verbindungsarten] ADD  CONSTRAINT [DF__DOM_Verbindungsarten__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [conf].[Verbindungsarten] ADD  CONSTRAINT [DF__DOM_Verbindungsarten__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [conf].[Verbindungsarten] ADD  CONSTRAINT [DF__DOM_Verbindungsarten__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [conf].[Verbindungsarten] ADD  CONSTRAINT [DF__DOM_Verbindungsarten__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [conf].[Verbindungsarten]  WITH CHECK ADD  CONSTRAINT [FK__Verbindungsarten__1D7B6025] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [conf].[Verbindungsarten] CHECK CONSTRAINT [FK__Verbindungsarten__1D7B6025]
GO


