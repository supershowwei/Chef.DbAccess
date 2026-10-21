SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [tbl_member] [mem_1] WITH (NOLOCK)
INNER JOIN [tbl_department] [dep_2] WITH (NOLOCK) ON [mem_1].[DepartmentId] = [dep_2].[Id]
WHERE [dep_2].[Name] LIKE '%' + @Name_0 + '%') THEN 1
        ELSE 0
    END AS BIT);

-- Parameters
-- @Name_0 = '業務' (String)
