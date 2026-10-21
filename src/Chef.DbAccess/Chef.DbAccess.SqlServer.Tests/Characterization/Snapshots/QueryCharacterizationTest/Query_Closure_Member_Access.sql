SELECT [mem_1].[No] AS [Id]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE ([mem_1].[Age] = {=Age_0}) AND ([mem_1].[Name] = @Name_0);

-- Parameters
-- @Age_0 = 18 (Int32)
-- @Name_0 = 'Johnny' (String)
