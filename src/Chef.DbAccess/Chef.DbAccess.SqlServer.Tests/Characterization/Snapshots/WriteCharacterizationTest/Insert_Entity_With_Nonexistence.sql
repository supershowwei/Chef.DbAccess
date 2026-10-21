INSERT INTO [tbl_member]([No], [Name], [DepartmentId])
    SELECT {=Id}, @Name, {=DepartmentId}
    WHERE NOT EXISTS (SELECT
                1
            FROM [tbl_member] [mem_1]
            WHERE [mem_1].[No] = {=Id_0});
