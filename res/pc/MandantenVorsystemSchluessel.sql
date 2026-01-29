USE [DIZ_NET]
GO

/****** Object:  Table [pc].[MandantenVorsystemSchluessel]    Script Date: 04.02.2025 11:45:29 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[MandantenVorsystemSchluessel](
    [MandantenVorsystemSchluessel_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [Mandanten_ID] [dbo].[ReferenzID] NULL,
    [Vorsystemschluessel] [dbo].[VorsystemID] NOT NULL,
    [ETL_Verbindungen_ID] [dbo].[ReferenzID] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_Mandanten_Vorsystemschluessel_ID] PRIMARY KEY CLUSTERED 
(
    [MandantenVorsystemSchluessel_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [Mandanten_Vorsystemschluessel_UC] UNIQUE NONCLUSTERED 
(
    [MandantenVorsystemSchluessel_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel] ADD  CONSTRAINT [DF__MandantenVorsystemSchluessel__6EF57B66]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel] ADD  CONSTRAINT [DF__Mandanten_Vorsystemschluessel__6FE99F9F]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel] ADD  CONSTRAINT [DF__MandantenVorsystemSchluessel__70DDC3D8]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel] ADD  CONSTRAINT [DF__MandantenVorsystemSchluessel__71D1E811]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel]  WITH CHECK ADD  CONSTRAINT [FK_Mandanten_Vorsystemschluessel__Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel] CHECK CONSTRAINT [FK_Mandanten_Vorsystemschluessel__Datenherkunft_ID]
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel]  WITH CHECK ADD  CONSTRAINT [FK_MandantenVorsystemSchluessel_ETL_Verbindungen] FOREIGN KEY([ETL_Verbindungen_ID])
REFERENCES [pc].[ETL_Verbindungen] ([ETL_Verbindungen_ID])
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel] CHECK CONSTRAINT [FK_MandantenVorsystemSchluessel_ETL_Verbindungen]
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel]  WITH CHECK ADD  CONSTRAINT [FK_MandantenVorsystemSchluessel_Mandanten] FOREIGN KEY([Mandanten_ID])
REFERENCES [conf].[Mandanten] ([Mandanten_ID])
GO

ALTER TABLE [pc].[MandantenVorsystemSchluessel] CHECK CONSTRAINT [FK_MandantenVorsystemSchluessel_Mandanten]
GO


