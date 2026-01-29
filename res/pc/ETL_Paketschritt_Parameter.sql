USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Paketschritt_Parameter]    Script Date: 13.12.2024 16:24:13 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Paketschritt_Parameter](
    [ETL_Paketschritt_Parameter_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Kopfzeile] [dbo].[Flag] NULL,
    [Trennzeichen] [dbo].[Kuerzel] NULL,
    [Zahlenformat] [dbo].[Kuerzel] NULL,
    [Datumsformat] [dbo].[Kuerzel] NULL,
    [Textqualifizierer] [char](1) NULL,
    [Escapecharacter] [char](1) NULL,
    [Leerwert] [dbo].[Kuerzel] NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Paketschritt_Parameter_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Paketschritt_Parameter_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Paketschritt_Parameter_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Paketschritt_Parameter_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter] ADD  CONSTRAINT [DF__ETL_Paketschritt_Parameter_12345]  DEFAULT ((1)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter] ADD  CONSTRAINT [DF__ETL_Paketschritt_Parameter__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter] ADD  CONSTRAINT [DF__ETL_Paketschritt_Parameter__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter] ADD  CONSTRAINT [DF__ETL_Paketschritt_Parameter__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter] ADD  CONSTRAINT [DF__ETL_Paketschritt_Parameter__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paketschritt_Parameter_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter] CHECK CONSTRAINT [FK_ETL_Paketschritt_Parameter_Datenherkunft_ID]
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter]  WITH CHECK ADD  CONSTRAINT [CK__ETL_Paket__Ist_a__12345] CHECK  (([Ist_aktiv]=(1) OR [Ist_aktiv]=(0)))
GO

ALTER TABLE [pc].[ETL_Paketschritt_Parameter] CHECK CONSTRAINT [CK__ETL_Paket__Ist_a__12345]
GO


