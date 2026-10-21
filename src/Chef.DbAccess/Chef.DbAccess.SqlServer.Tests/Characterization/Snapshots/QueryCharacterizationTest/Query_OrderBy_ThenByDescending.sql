SELECT [mem_1].[Name], [mem_1].[Age]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE [mem_1].[Age] > {=Age_0}
ORDER BY [mem_1].[No] ASC, [mem_1].[Name] DESC;

-- Parameters
-- @Age_0 = 0 (Int32)
