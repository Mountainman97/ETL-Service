USE [DIZ_NET]
GO

/****** Object:  Table [Logging].[DB_Log]    Script Date: 07.11.2024 17:13:10 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [Logging].[DB_Log](
    [ID] [bigint] IDENTITY(1,1) NOT NULL,
    [Globale_Prozess_ID] [nvarchar](50) NOT NULL,
    [Prozedur] [nvarchar](100) NOT NULL,
    [Information] [nvarchar](max) NOT NULL,
    [Nutzer] [nvarchar](200) NOT NULL,
    [Zeitpunkt] [datetime] NOT NULL,
 CONSTRAINT [PK_DB_Log_ID] PRIMARY KEY CLUSTERED 
(
    [ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


