USE [DIZ_NET]
GO

/****** Object:  Table [conf].[Datenherkunft]    Script Date: 07.11.2024 17:11:03 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [conf].[Datenherkunft](
    [Datenherkunft_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Datenherkunft] [dbo].[Kuerzel] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Datenherkunft_ID] PRIMARY KEY CLUSTERED 
(
    [Datenherkunft] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [UQ__Datenherkunft__53EAAE7FFB4BA655] UNIQUE NONCLUSTERED 
(
    [Datenherkunft_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [conf].[Datenherkunft] ADD  CONSTRAINT [DF__Datenherkunft__5535A963]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [conf].[Datenherkunft] ADD  CONSTRAINT [DF__Datenherkunft__571DF1D5]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [conf].[Datenherkunft] ADD  CONSTRAINT [DF__Datenherkunft__5629CD9C]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [conf].[Datenherkunft] ADD  CONSTRAINT [DF__Datenherkunft__5812160E]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO


