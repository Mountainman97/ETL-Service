USE [DIZ_NET]
GO

/****** Object:  Table [conf].[Datensysteme]    Script Date: 07.11.2024 17:11:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [conf].[Datensysteme](
    [Datensysteme_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Datensystem] [dbo].[Kuerzel] NOT NULL,
    [Bezeichnung] [dbo].[Kurztext] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Datensysteme_ID] PRIMARY KEY CLUSTERED 
(
    [Datensysteme_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [Datensysteme_ID_UC] UNIQUE NONCLUSTERED 
(
    [Datensysteme_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [conf].[Datensysteme] ADD  CONSTRAINT [DF__Datensysteme__6EF57B63]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [conf].[Datensysteme] ADD  CONSTRAINT [DF__Datensysteme__6FE99F9A]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [conf].[Datensysteme] ADD  CONSTRAINT [DF__Datensysteme__70DDC3D1]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [conf].[Datensysteme] ADD  CONSTRAINT [DF__Datensysteme__71D1E804]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [conf].[Datensysteme]  WITH CHECK ADD  CONSTRAINT [FK__Datensysteme__1D7B6025] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [conf].[Datensysteme] CHECK CONSTRAINT [FK__Datensysteme__1D7B6025]
GO


