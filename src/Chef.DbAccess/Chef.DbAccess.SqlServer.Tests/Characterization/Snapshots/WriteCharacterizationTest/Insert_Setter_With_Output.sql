SELECT
    CAST(0 AS INT) AS [Id] INTO [#_{guid}]
FROM [tbl_member] WITH (NOLOCK)
WHERE 1 = 0;

INSERT INTO [tbl_member]([Name], [Age])
OUTPUT [INSERTED].[No] INTO [#_{guid}]
    SELECT @Name_0, {=Age_0};

SELECT
    *
FROM [#_{guid}]

DROP TABLE [#_{guid}]

-- Parameters
-- @Name_0 = 'Johnny' (String)
-- @Age_0 = 30 (Int32)
