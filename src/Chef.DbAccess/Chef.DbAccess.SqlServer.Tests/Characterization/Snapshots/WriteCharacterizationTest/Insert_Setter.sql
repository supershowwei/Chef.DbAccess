INSERT INTO [tbl_member]([No], [Name], [Phone], [Age])
    SELECT {=Id_0}, @Name_0, @Phone_0, {=Age_0};

-- Parameters
-- @Id_0 = 1 (Int32)
-- @Name_0 = 'Johnny' (String)
-- @Phone_0 = DbString('0912345678' (String), Length=20, IsAnsi=True, IsFixedLength=False)
-- @Age_0 = 30 (Int32)
