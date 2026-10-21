SELECT
    CAST(CASE
        WHEN
            EXISTS (SELECT
                    1
                FROM [tbl_member] [mem_1] WITH (NOLOCK)
WHERE [mem_1].[No] = {=Id_0}) THEN 1
        ELSE 0
    END AS BIT);

-- Parameters
-- @Id_0 = 1 (Int32)
