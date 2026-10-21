INSERT INTO [tbl_member]([No], [Name], [DepartmentId])
    SELECT tvp.[No], tvp.[Name], tvp.[DepartmentId]
    FROM @TableVariable tvp;
