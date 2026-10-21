SELECT [mem_1].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE ([mem_1].[Birthday] >= @Birthday_0) AND ([mem_1].[Token] = @Token_0);

-- Parameters
-- @Birthday_0 = 2000-01-02T03:04:05.0000000 (DateTime)
-- @Token_0 = 3f2504e0-4f89-11d3-9a0c-0305e82c3301 (Guid)
