SELECT [mem_1].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE (([mem_1].[Name] LIKE '%' + @Name_0 + '%') AND ([mem_1].[Name] LIKE '%' + @Name_1)) AND ([mem_1].[Name] NOT LIKE @Name_2 + '%');

-- Parameters
-- @Name_0 = 'e' (String)
-- @Name_1 = 'y' (String)
-- @Name_2 = 'A' (String)
