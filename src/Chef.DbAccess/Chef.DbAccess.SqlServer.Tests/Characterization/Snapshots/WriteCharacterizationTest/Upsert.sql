SET TRANSACTION ISOLATION LEVEL SERIALIZABLE

UPDATE [tbl_member] WITH (ROWLOCK)
SET [Name] = @Name_0, [Phone] = @Phone_0
WHERE [No] = {=Id_0}

IF @@rowcount = 0
    BEGIN
        INSERT INTO [tbl_member]([Name], [Phone], [No])
            SELECT @Name_0, @Phone_0, {=Id_0}
    END;
