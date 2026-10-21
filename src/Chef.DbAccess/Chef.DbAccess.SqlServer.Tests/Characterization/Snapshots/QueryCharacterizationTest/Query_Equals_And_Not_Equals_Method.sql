SELECT [mem_1].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE ([mem_1].[Age] = {=Age_0}) AND ([mem_1].[Name] <> @Name_0);

-- Parameters
-- @Age_0 = 20 (Int32)
-- @Name_0 = 'Johnny' (String)
