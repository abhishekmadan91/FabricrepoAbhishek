CREATE TABLE [dbo].[PrimaryKeyDemoTable] (

	[c1] int NOT NULL, 
	[c2] varchar(100) NULL
);


GO
ALTER TABLE [dbo].[PrimaryKeyDemoTable] ADD CONSTRAINT PK_PrimaryKeyDemoTable_c1 primary key NONCLUSTERED ([c1]);