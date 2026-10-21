SELECT
    CAST(0 AS INT) AS [Id], [Name] INTO [#_{guid}]
FROM [tbl_member] WITH (NOLOCK)
WHERE 1 = 0;

DELETE [mem_1]
OUTPUT [DELETED].[No], [DELETED].[Name] INTO [#_{guid}]
FROM [tbl_member] [mem_1]
WHERE [mem_1].[Age] < {=Age_0};

SELECT
    *
FROM [#_{guid}]

DROP TABLE [#_{guid}]
