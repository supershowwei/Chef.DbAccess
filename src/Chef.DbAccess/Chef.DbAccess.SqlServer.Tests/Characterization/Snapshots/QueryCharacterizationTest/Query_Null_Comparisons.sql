SELECT [mem_1].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE ([mem_1].[Phone] IS NULL) AND ([mem_1].[Birthday] IS NOT NULL);

-- Parameters
-- @Phone_0 = NULL
-- @Birthday_0 = NULL
