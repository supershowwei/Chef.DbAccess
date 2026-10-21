SELECT
    CAST(0 AS INT) AS [Id], [Name] INTO [#_{guid}]
FROM [tbl_member] WITH (NOLOCK)
WHERE 1 = 0;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE

UPDATE [tbl_member] WITH (ROWLOCK)
SET [Name] = @Name_0
OUTPUT [INSERTED].[No], [INSERTED].[Name] INTO [#_{guid}]
WHERE [No] = {=Id_0}

IF @@rowcount = 0
    BEGIN
        INSERT INTO [tbl_member]([Name], [No])
        OUTPUT [INSERTED].[No], [INSERTED].[Name] INTO [#_{guid}]
            SELECT @Name_0, {=Id_0}
    END;

SELECT
    *
FROM [#_{guid}]

DROP TABLE [#_{guid}]
