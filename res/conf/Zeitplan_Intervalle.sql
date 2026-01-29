USE [DIZ_NET]
GO

/****** Object:  Table [conf].[Zeitplan_Intervalle]    Script Date: 04.11.2024 15:49:09 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [conf].[Zeitplan_Intervalle](
    [Zeitplan_Intervalle_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Zeitplan_Intervall] [dbo].[Kuerzel] NOT NULL,
    [Zeitplan_Intervallbezeichnung] [dbo].[Kurztext] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Zeitplan_Intervalle_ID] PRIMARY KEY CLUSTERED 
(
    [Zeitplan_Intervalle_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [Zeitplan_Intervalle_UC] UNIQUE NONCLUSTERED 
(
    [Zeitplan_Intervalle_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [conf].[Zeitplan_Intervalle] ADD  CONSTRAINT [DF__Zeitplan_Intervalle__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [conf].[Zeitplan_Intervalle] ADD  CONSTRAINT [DF__Zeitplan_Intervalle__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [conf].[Zeitplan_Intervalle] ADD  CONSTRAINT [DF__Zeitplan_Intervalle__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [conf].[Zeitplan_Intervalle] ADD  CONSTRAINT [DF__Zeitplan_Intervalle__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [conf].[Zeitplan_Intervalle]  WITH CHECK ADD  CONSTRAINT [FK_Zeitplan_Intervalle_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [conf].[Zeitplan_Intervalle] CHECK CONSTRAINT [FK_Zeitplan_Intervalle_Datenherkunft_ID]
GO


