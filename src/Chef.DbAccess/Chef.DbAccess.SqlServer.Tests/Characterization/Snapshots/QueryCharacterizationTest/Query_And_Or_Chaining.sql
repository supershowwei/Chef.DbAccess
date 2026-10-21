SELECT [mem_1].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE (([mem_1].[No] = {=Id_0}) OR ([mem_1].[No] = {=Id_1})) AND ([mem_1].[Age] > {=Age_0});

-- Parameters
-- @Id_0 = 1 (Int32)
-- @Id_1 = 2 (Int32)
-- @Age_0 = 18 (Int32)
