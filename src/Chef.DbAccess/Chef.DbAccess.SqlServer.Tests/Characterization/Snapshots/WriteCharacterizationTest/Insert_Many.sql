INSERT INTO [tbl_member]([No], [Name], [DepartmentId])
    SELECT {=Id}, @Name, {=DepartmentId};
