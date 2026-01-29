USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Mandanten_Verbindungen]    Script Date: 04.02.2025 11:30:33 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Mandanten_Verbindungen](
	[ETL_Mandanten_Verbindungen_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
	[Mandanten_ID] [dbo].[ReferenzID] NOT NULL,
	[ETL_Verbindungen_ID] [dbo].[ReferenzID] NOT NULL,
	[Datenverbindungen_ID] [dbo].[ReferenzID] NOT NULL,
	[Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
	[Anlagedatum] [dbo].[DatumZeit] NOT NULL,
	[Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
	[Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
	[Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Mandanten_Verbindungen_ID] PRIMARY KEY CLUSTERED 
(
	[ETL_Mandanten_Verbindungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [INDEX],
 CONSTRAINT [ETL_Mandanten_Verbindungen_UC] UNIQUE NONCLUSTERED 
(
	[ETL_Mandanten_Verbindungen_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [INDEX]
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen] ADD  CONSTRAINT [DF__ETL_Mandanten_Verbindungen__6EF57B67]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen] ADD  CONSTRAINT [DF__ETL_Mandanten_Verbindungen__6FE99F9D]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen] ADD  CONSTRAINT [DF__ETL_Mandanten_Verbindungen__70DDC3D9]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen] ADD  CONSTRAINT [DF__ETL_Mandanten_Verbindungen__71D1E812]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Mandanten_Verbindungen_Datenherkunft_ID] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen] CHECK CONSTRAINT [FK_ETL_Mandanten_Verbindungen_Datenherkunft_ID]
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Mandanten_Verbindungen_Datenverbindungen_ID] FOREIGN KEY([Datenverbindungen_ID])
REFERENCES [conf].[Datenverbindungen] ([Datenverbindungen_ID])
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen] CHECK CONSTRAINT [FK_ETL_Mandanten_Verbindungen_Datenverbindungen_ID]
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Mandanten_Verbindungen_ETL_Verbindungen] FOREIGN KEY([ETL_Verbindungen_ID])
REFERENCES [pc].[ETL_Verbindungen] ([ETL_Verbindungen_ID])
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen] CHECK CONSTRAINT [FK_ETL_Mandanten_Verbindungen_ETL_Verbindungen]
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Mandanten_Verbindungen_Mandanten_ID] FOREIGN KEY([Mandanten_ID])
REFERENCES [conf].[Mandanten] ([Mandanten_ID])
GO

ALTER TABLE [pc].[ETL_Mandanten_Verbindungen] CHECK CONSTRAINT [FK_ETL_Mandanten_Verbindungen_Mandanten_ID]
GO


