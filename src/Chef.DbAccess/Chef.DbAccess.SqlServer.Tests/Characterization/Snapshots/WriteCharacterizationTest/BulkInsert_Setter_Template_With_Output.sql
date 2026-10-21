SELECT
    CAST(0 AS INT) AS [Id] INTO [#_{guid}]
FROM [tbl_member] WITH (NOLOCK)
WHERE 1 = 0;

INSERT INTO [tbl_member]([No], [Name], [Phone], [Age])
OUTPUT [INSERTED].[No] INTO [#_{guid}]
    SELECT tvp.[No], tvp.[Name], tvp.[Phone], tvp.[Age]
    FROM @TableVariable tvp;

SELECT
    *
FROM [#_{guid}]

DROP TABLE [#_{guid}]
