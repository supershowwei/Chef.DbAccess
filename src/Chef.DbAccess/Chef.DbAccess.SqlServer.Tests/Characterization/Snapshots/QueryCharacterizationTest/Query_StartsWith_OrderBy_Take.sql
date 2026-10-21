SELECT TOP(10) [mem_1].[Name], [mem_1].[Age]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE ([mem_1].[Age] >= {=Age_0}) AND ([mem_1].[Name] LIKE @Name_0 + '%')
ORDER BY [mem_1].[Age] ASC;

-- Parameters
-- @Age_0 = 30 (Int32)
-- @Name_0 = 'J' (String)
