SELECT [mem_1].[No] AS [Id], [mem_1].[Name], [dep_2].[Id], [dep_2].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
LEFT JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[No] = [dep_2].[ManagerId]
WHERE [mem_1].[Age] > {=Age_0};

-- Parameters
-- @Age_0 = 20 (Int32)
