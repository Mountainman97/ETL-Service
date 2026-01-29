USE [DIZ_NET]
GO

/****** Object:  Table [conf].[Mandantentypen]    Script Date: 07.11.2024 17:12:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [conf].[Mandantentypen](
    [Mandantentypen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Typ] [dbo].[Kuerzel] NOT NULL,
    [Bezeichnung] [dbo].[Kurztext] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Mandantentypen_ID] PRIMARY KEY CLUSTERED 
(
    [Typ] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [FS_Mandantentypen_UC] UNIQUE NONCLUSTERED 
(
    [Mandantentypen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [conf].[Mandantentypen] ADD  CONSTRAINT [DF__Mandantentypen__37703C52]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [conf].[Mandantentypen] ADD  CONSTRAINT [DF__Mandantentypen__3864608B]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [conf].[Mandantentypen] ADD  CONSTRAINT [DF__Mandantentypen__395884C4]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [conf].[Mandantentypen] ADD  CONSTRAINT [DF__Mandantentypen__3A4CA8FD]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [conf].[Mandantentypen]  WITH CHECK ADD  CONSTRAINT [FK__Mandantentypen__1D7B6025] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [conf].[Mandantentypen] CHECK CONSTRAINT [FK__Mandantentypen__1D7B6025]
GO


