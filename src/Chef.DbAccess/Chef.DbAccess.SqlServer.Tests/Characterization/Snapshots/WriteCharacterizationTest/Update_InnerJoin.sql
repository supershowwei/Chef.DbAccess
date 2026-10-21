UPDATE [mem_1]
SET [mem_1].[Age] = {=Age_0}
FROM [tbl_member] [mem_1]
INNER JOIN [tbl_department] [dep_2] ON [mem_1].[DepartmentId] = [dep_2].[Id]
WHERE [dep_2].[Name] LIKE '%' + @Name_0 + '%';
