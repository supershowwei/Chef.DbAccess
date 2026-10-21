SELECT
    CAST(0 AS INT) AS [Id] INTO [#_{guid}]
FROM [tbl_member] WITH (NOLOCK)
WHERE 1 = 0;

UPDATE [tbl_member]
SET [Name] = tvp.[Name], [Phone] = tvp.[Phone]
OUTPUT [INSERTED].[No] INTO [#_{guid}]
FROM [tbl_member] t
INNER JOIN @TableVariable tvp
    ON t.[No] = tvp.[No]

INSERT INTO [tbl_member]([Name], [Phone], [No])
OUTPUT [INSERTED].[No] INTO [#_{guid}]
    SELECT tvp.[Name], tvp.[Phone], tvp.[No]
    FROM @TableVariable tvp
    WHERE NOT EXISTS (SELECT
                1
            FROM [tbl_member] t WITH (NOLOCK)
            WHERE t.[No] = tvp.[No]);

SELECT
    *
FROM [#_{guid}]

DROP TABLE [#_{guid}]
