SELECT [mem_1].[Name], [mem_1].[Age]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE CONTAINS([mem_1].[Name], @Name_0);

-- Parameters
-- @Name_0 = 'Johnny' (String)
