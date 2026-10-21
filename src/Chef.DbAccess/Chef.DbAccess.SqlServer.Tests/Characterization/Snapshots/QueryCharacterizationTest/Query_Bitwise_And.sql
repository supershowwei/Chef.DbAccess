SELECT [mem_1].[Name], [mem_1].[Age]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE ([mem_1].[No] & {=Id_0}) > {=Id_1};

-- Parameters
-- @Id_0 = 1 (Int32)
-- @Id_1 = 0 (Int32)
