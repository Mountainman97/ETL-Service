USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Zeitplaene]    Script Date: 04.11.2024 15:45:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Zeitplaene](
    [ETL_Zeitplaene_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Bezeichnung] [dbo].[Kurztext] NOT NULL,
    [Information] [dbo].[Kurztext] NOT NULL,
    [Anfangsdatum] [dbo].[DatumZeit] NOT NULL,
    [Endedatum] [dbo].[DatumZeit] NULL,
    [Zeitplan_Intervalle_ID] [dbo].[ReferenzID] NOT NULL,
    [Startzeit] [dbo].[Zeit] NULL,
    [Tageswiederholung] [int] NULL,
    [Wochenwiederholung] [int] NULL,
    [An_jedem_Tag] [dbo].[Flag] NOT NULL,
    [In_jedem_Monat] [dbo].[Flag] NOT NULL,
    [Montag] [dbo].[Flag] NOT NULL,
    [Dienstag] [dbo].[Flag] NOT NULL,
    [Mittwoch] [dbo].[Flag] NOT NULL,
    [Donnerstag] [dbo].[Flag] NOT NULL,
    [Freitag] [dbo].[Flag] NOT NULL,
    [Samstag] [dbo].[Flag] NOT NULL,
    [Sonntag] [dbo].[Flag] NOT NULL,
    [Januar] [dbo].[Flag] NOT NULL,
    [Februar] [dbo].[Flag] NOT NULL,
    [Maerz] [dbo].[Flag] NOT NULL,
    [April] [dbo].[Flag] NOT NULL,
    [Mai] [dbo].[Flag] NOT NULL,
    [Juni] [dbo].[Flag] NOT NULL,
    [Juli] [dbo].[Flag] NOT NULL,
    [August] [dbo].[Flag] NOT NULL,
    [September] [dbo].[Flag] NOT NULL,
    [Oktober] [dbo].[Flag] NOT NULL,
    [November] [dbo].[Flag] NOT NULL,
    [Dezember] [dbo].[Flag] NOT NULL,
    [Woche_des_Monats] [dbo].[Ganzzahl] NULL,
    [Monatsletzter] [dbo].[Flag] NOT NULL,
    [Sofort_Ausfuehrung] [dbo].[Flag] NOT NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Zeitplaene_ID] PRIMARY KEY CLUSTERED 
(
    [ETL_Zeitplaene_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Zeitplaene_UC] UNIQUE NONCLUSTERED 
(
    [ETL_Zeitplaene_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Zeitplaene] ADD  CONSTRAINT [DF__ETL_Zeitp__Monat__0E391C95]  DEFAULT ((0)) FOR [Monatsletzter]
GO

ALTER TABLE [pc].[ETL_Zeitplaene] ADD  CONSTRAINT [DF__ETL_Zeitp__Sofor__10216507]  DEFAULT ((0)) FOR [Sofort_Ausfuehrung]
GO

ALTER TABLE [pc].[ETL_Zeitplaene] ADD  CONSTRAINT [DF__ETL_Zeitplaene__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Zeitplaene] ADD  CONSTRAINT [DF__ETL_Zeitplaene__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Zeitplaene] ADD  CONSTRAINT [DF__ETL_Zeitplaene__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Zeitplaene] ADD  CONSTRAINT [DF__ETL_Zeitplaene__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Zeitplaene]  WITH CHECK ADD  CONSTRAINT [FK__ETL_Zeitp__Zeitp__0D44F85C] FOREIGN KEY([Zeitplan_Intervalle_ID])
REFERENCES [conf].[Zeitplan_Intervalle] ([Zeitplan_Intervalle_ID])
ON DELETE CASCADE
GO

ALTER TABLE [pc].[ETL_Zeitplaene] CHECK CONSTRAINT [FK__ETL_Zeitp__Zeitp__0D44F85C]
GO

ALTER TABLE [pc].[ETL_Zeitplaene]  WITH CHECK ADD  CONSTRAINT [CK__ETL_Zeitplaene] CHECK  (([Woche_des_Monats]>=(0) AND [Woche_des_Monats]<(6)))
GO

ALTER TABLE [pc].[ETL_Zeitplaene] CHECK CONSTRAINT [CK__ETL_Zeitplaene]
GO

ALTER TABLE [pc].[ETL_Zeitplaene]  WITH CHECK ADD  CONSTRAINT [CK__ETL_Zeitplaene_rep] CHECK  (([Tageswiederholung]>(0) AND [Wochenwiederholung]=(0) OR [Tageswiederholung]=(0) AND [Wochenwiederholung]>(0) OR [Tageswiederholung]=(0) AND [Wochenwiederholung]=(0)))
GO

ALTER TABLE [pc].[ETL_Zeitplaene] CHECK CONSTRAINT [CK__ETL_Zeitplaene_rep]
GO


