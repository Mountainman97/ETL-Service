USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Verbindungen]    Script Date: 07.11.2024 17:14:42 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Verbindungen](
    [ETL_Verbindungen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Verbindung] [dbo].[Kuerzel] NOT NULL,
    [Bezeichnung] [dbo].[Kurztext] NOT NULL,
    [Datensysteme_ID] [dbo].[ReferenzID] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Verbindungen_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Verbindungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Verbindungen_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Verbindungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Verbindungen] ADD  CONSTRAINT [DF__ETL_Verbindungen__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Verbindungen] ADD  CONSTRAINT [DF__ETL_Verbindungen__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Verbindungen] ADD  CONSTRAINT [DF__ETL_Verbindungen__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Verbindungen] ADD  CONSTRAINT [DF__ETL_Verbindungen__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Verbindungen]  WITH CHECK ADD  CONSTRAINT [FK__ETL_Verbindungen__1D7B6025] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Verbindungen] CHECK CONSTRAINT [FK__ETL_Verbindungen__1D7B6025]
GO

ALTER TABLE [pc].[ETL_Verbindungen]  WITH CHECK ADD  CONSTRAINT [FK_Datensysteme_ID] FOREIGN KEY([Datensysteme_ID])
REFERENCES [conf].[Datensysteme] ([Datensysteme_ID])
GO

ALTER TABLE [pc].[ETL_Verbindungen] CHECK CONSTRAINT [FK_Datensysteme_ID]
GO


