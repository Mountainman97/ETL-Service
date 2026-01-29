USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Mandanten_Konfigurationen]    Script Date: 04.02.2025 11:29:55 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Mandanten_Konfigurationen](
    [ETL_Mandanten_Konfigurationen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [ETL_Konfigurationen_ID] [dbo].[ReferenzID] NOT NULL,
    [Mandanten_ID] [dbo].[ReferenzID] NOT NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Mandanten_Konfigurationen_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Mandanten_Konfigurationen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Mandanten_Konfigurationen_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Mandanten_Konfigurationen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen] ADD  CONSTRAINT [DF_ETL_Mandanten_Konfigurationen_Ist_aktiv]  DEFAULT ((1)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen] ADD  CONSTRAINT [DF__ETL_Mandanten_Konfigurationen__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen] ADD  CONSTRAINT [DF__ETL_MandantenKonfigurationen__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen] ADD  CONSTRAINT [DF__ETL_MandantenKonfigurationen__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen] ADD  CONSTRAINT [DF__ETL_MandantenKonfigurationen__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Mandanten_Konfigurationen_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen] CHECK CONSTRAINT [FK_ETL_Mandanten_Konfigurationen_Datenherkunft_ID]
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Mandanten_Konfigurationen_ETL_Konfigurationen] FOREIGN KEY([ETL_Konfigurationen_ID])
REFERENCES [pc].[ETL_Konfigurationen] ([ETL_Konfigurationen_ID])
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen] CHECK CONSTRAINT [FK_ETL_Mandanten_Konfigurationen_ETL_Konfigurationen]
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Mandanten_Konfigurationen_Mandanten_ID] FOREIGN KEY([Mandanten_ID])
REFERENCES [conf].[Mandanten] ([Mandanten_ID])
GO

ALTER TABLE [pc].[ETL_Mandanten_Konfigurationen] CHECK CONSTRAINT [FK_ETL_Mandanten_Konfigurationen_Mandanten_ID]
GO


