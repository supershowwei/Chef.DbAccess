SELECT [mem_1].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE [mem_1].[Name] > @Name_0;

-- Parameters
-- @Name_0 = 'M' (String)
