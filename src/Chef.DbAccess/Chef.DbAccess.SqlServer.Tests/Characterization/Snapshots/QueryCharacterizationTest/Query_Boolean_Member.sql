SELECT [mem_1].[Name]
FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE [mem_1].[IsActive] = {=IsActive_0};

-- Parameters
-- @IsActive_0 = True (Boolean)
