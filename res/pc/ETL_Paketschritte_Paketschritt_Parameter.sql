USE [DIZ_NET]
GO

/****** Object:  Table [pc].[ETL_Paketschritte_Paketschritt_Parameter]    Script Date: 04.02.2025 11:36:58 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter](
    [ETL_Paketschritte_Paketschritt_Parameter_ID] [dbo].[ReferenzID] IDENTITY(1,1) NOT NULL,
    [ETL_Workflow_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Paketschritte_ID] [dbo].[ReferenzID] NOT NULL,
    [ETL_Paketschritt_Parameter_ID] [dbo].[ReferenzID] NOT NULL,
    [Ist_aktiv] [dbo].[Flag] NOT NULL,
    [Datenherkunft_ID] [dbo].[ReferenzID] NOT NULL,
    [Anlagedatum] [dbo].[DatumZeit] NOT NULL,
    [Anlage_Nutzer] [dbo].[Kurztext] NOT NULL,
    [Letzte_Aenderung] [dbo].[DatumZeit] NOT NULL,
    [Letzte_Aenderung_Nutzer] [dbo].[Kurztext] NOT NULL,
 CONSTRAINT [PK_ETL_Paketschritte_Paketschritt_Parameter] PRIMARY KEY CLUSTERED 
(
    [ETL_Paketschritte_Paketschritt_Parameter_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] ADD  CONSTRAINT [DF_ETL_Paketschritte_Paketschritt_Parameter_Ist_aktiv]  DEFAULT ((1)) FOR [Ist_aktiv]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] ADD  CONSTRAINT [DF_ETL_Paketschritte_Paketschritt_Parameter_Anlagedatum]  DEFAULT (getdate()) FOR [Anlagedatum]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] ADD  CONSTRAINT [DF_ETL_Paketschritte_Paketschritt_Parameter_Anlage_Nutzer]  DEFAULT (suser_name()) FOR [Anlage_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] ADD  CONSTRAINT [DF_ETL_Paketschritte_Paketschritt_Parameter_Letzte_Aenderung]  DEFAULT (getdate()) FOR [Letzte_Aenderung]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] ADD  CONSTRAINT [DF_ETL_Paketschritte_Paketschritt_Parameter_Letzte_Aenderung_Nutzer]  DEFAULT (suser_name()) FOR [Letzte_Aenderung_Nutzer]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paketschritte_Paketschritt_Parameter_Datenherkunft] FOREIGN KEY([Datenherkunft_ID])
REFERENCES [conf].[Datenherkunft] ([Datenherkunft_ID])
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] CHECK CONSTRAINT [FK_ETL_Paketschritte_Paketschritt_Parameter_Datenherkunft]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paketschritte_Paketschritt_Parameter_ETL_Paketschritt_Parameter] FOREIGN KEY([ETL_Paketschritt_Parameter_ID])
REFERENCES [pc].[ETL_Paketschritt_Parameter] ([ETL_Paketschritt_Parameter_ID])
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] CHECK CONSTRAINT [FK_ETL_Paketschritte_Paketschritt_Parameter_ETL_Paketschritt_Parameter]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paketschritte_Paketschritt_Parameter_ETL_Paketschritte] FOREIGN KEY([ETL_Paketschritte_ID])
REFERENCES [pc].[ETL_Paketschritte] ([ETL_Paketschritte_ID])
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] CHECK CONSTRAINT [FK_ETL_Paketschritte_Paketschritt_Parameter_ETL_Paketschritte]
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter]  WITH CHECK ADD  CONSTRAINT [FK_ETL_Paketschritte_Paketschritt_Parameter_ETL_Workflow] FOREIGN KEY([ETL_Workflow_ID])
REFERENCES [pc].[ETL_Workflow] ([ETL_Workflow_ID])
GO

ALTER TABLE [pc].[ETL_Paketschritte_Paketschritt_Parameter] CHECK CONSTRAINT [FK_ETL_Paketschritte_Paketschritt_Parameter_ETL_Workflow]
GO


