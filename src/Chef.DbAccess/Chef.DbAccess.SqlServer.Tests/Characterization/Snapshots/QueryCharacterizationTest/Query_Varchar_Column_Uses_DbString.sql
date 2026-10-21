SELECT [mem_1].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE [mem_1].[Phone] = @Phone_0;

-- Parameters
-- @Phone_0 = DbString('0912345678' (String), Length=20, IsAnsi=True, IsFixedLength=False)
