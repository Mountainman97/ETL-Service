USE [DIZ_NET]
GO

/****** Object:  Table [conf].[Konfigurationsparameter]    Script Date: 04.11.2024 15:48:15 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [conf].[Konfigurationsparameter](
    [Konfigurationsparameter_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Parametername] [dbo].[Kuerzel] NOT NULL,
    [Parameterbeschreibung] [dbo].[Kurztext] NOT NULL,
    [Parametertyp] [dbo].[Kuerzel] NOT NULL,
    [Parameterwert] [dbo].[Kuerzel] NOT NULL,
    [Minimalwert] [dbo].[Kuerzel] NULL,
    [Maximalwert] [dbo].[ReferenzID] NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Konfigurationsparameter_ID] PRIMARY KEY CLUSTERED 
(
    [Konfigurationsparameter_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [Konfigurationsparameter_UC] UNIQUE NONCLUSTERED 
(
    [Konfigurationsparameter_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [conf].[Konfigurationsparameter] ADD  CONSTRAINT [DF__Konfigurationsparameter__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [conf].[Konfigurationsparameter] ADD  CONSTRAINT [DF__Konfigurationsparameter__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [conf].[Konfigurationsparameter] ADD  CONSTRAINT [DF__Konfigurationsparameter__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [conf].[Konfigurationsparameter] ADD  CONSTRAINT [DF__Konfigurationsparameter__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [conf].[Konfigurationsparameter]  WITH CHECK ADD  CONSTRAINT [FK_Konfigurationsparameter_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [conf].[Konfigurationsparameter] CHECK CONSTRAINT [FK_Konfigurationsparameter_Datenherkunft_ID]
GO

ALTER TABLE [conf].[Konfigurationsparameter]  WITH CHECK ADD CHECK  (([Parametertyp]='Tabelle' OR [Parametertyp]='Bool' OR [Parametertyp]='Ganzzahl' OR [Parametertyp]='Decimal' OR [Parametertyp]='Datum' OR [Parametertyp]='Text'))
GO

ALTER TABLE [conf].[Konfigurationsparameter]  WITH CHECK ADD CHECK  (([Parametertyp]='Tabelle' OR [Parametertyp]='Flag' OR [Parametertyp]='Ganzzahl' OR [Parametertyp]='Decimal' OR [Parametertyp]='Datum' OR [Parametertyp]='Text'))
GO


