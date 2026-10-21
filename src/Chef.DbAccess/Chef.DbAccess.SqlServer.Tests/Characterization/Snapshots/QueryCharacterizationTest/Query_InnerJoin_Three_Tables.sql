SELECT [mem_1].[No] AS [Id], [mem_1].[Name], [dep_2].[Id], [dep_2].[Name], [mem_3].[No] AS [Id], [mem_3].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[DepartmentId] = [dep_2].[Id]
INNER JOIN [tbl_member] [mem_3] WITH (NOLOCK) ON [mem_1].[ManagerId] = [mem_3].[No]
WHERE [mem_1].[Age] > {=Age_0}
ORDER BY [mem_3].[Name] ASC;

-- Parameters
-- @Age_0 = 20 (Int32)
