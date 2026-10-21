SELECT * FROM
(
SELECT [ms_1].[Name] AS [Name], AVG(CAST([ms_1].[Age] AS DECIMAL(38, 6))) AS [AverageAge]
FROM [tbl_member] [ms_1] WITH (NOLOCK)
WHERE [ms_1].[Name] = @Name_0
GROUP BY ms_1.[Name]
) [__T__];

-- Parameters
-- @Name_0 = 'Johnny' (String)
