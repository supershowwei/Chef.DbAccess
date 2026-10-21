SELECT [mem_1].[Name], [mem_1].[Age]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE [mem_1].[No] <> {=Id_0} AND [mem_1].[No] <> {=Id_1} AND [mem_1].[No] <> {=Id_2};

-- Parameters
-- @Id_0 = 1 (Int32)
-- @Id_1 = 2 (Int32)
-- @Id_2 = 3 (Int32)
