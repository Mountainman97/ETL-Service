USE [DIZ_NET]
GO

/****** Object:  Table [conf].[Mandanten]    Script Date: 07.11.2024 17:11:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [conf].[Mandanten](
    [Mandanten_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Mandant] [dbo].[Kuerzel] NOT NULL,
    [Bezeichnung] [dbo].[Kurztext] NOT NULL,
    [Institutskennzeichen] [dbo].[Kuerzel] NULL,
    [Mandanten_Typ_ID] [dbo].[ReferenzID] NOT NULL,
    [Ist_Hauptmandant] [dbo].[Flag] NOT NULL,
    [Mandantenreferenz_ID] [dbo].[ReferenzID] NULL,
    [Projekte_ID] [dbo].[ReferenzID] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Mandanten_ID] PRIMARY KEY CLUSTERED 
(
    [Mandant] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [Mandanten_UC] UNIQUE NONCLUSTERED 
(
    [Mandanten_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [conf].[Mandanten] ADD  CONSTRAINT [DF__Mandanten__6EF57B66]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [conf].[Mandanten] ADD  CONSTRAINT [DF__Mandanten__6FE99F9F]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [conf].[Mandanten] ADD  CONSTRAINT [DF__Mandanten__70DDC3D8]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [conf].[Mandanten] ADD  CONSTRAINT [DF__Mandanten__71D1E811]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [conf].[Mandanten]  WITH CHECK ADD  CONSTRAINT [FK__Mandanten__1D7B6025] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [conf].[Mandanten] CHECK CONSTRAINT [FK__Mandanten__1D7B6025]
GO


