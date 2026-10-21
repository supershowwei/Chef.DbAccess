SELECT [mem_1].[Name], [mem_1].[Age]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
ORDER BY [mem_1].[No] ASC
OFFSET 20 ROWS
FETCH NEXT 10 ROWS ONLY;
