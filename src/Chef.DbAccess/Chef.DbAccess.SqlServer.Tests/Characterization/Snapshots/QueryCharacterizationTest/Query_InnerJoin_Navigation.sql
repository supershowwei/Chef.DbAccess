SELECT [mem_1].[No] AS [Id], [mem_1].[Name], [dep_2].[Id], [dep_2].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[DepartmentId] = [dep_2].[Id]
WHERE [dep_2].[Name] LIKE '%' + @Name_0 + '%';

-- Parameters
-- @Name_0 = '業務' (String)
