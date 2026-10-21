DELETE [mem_1]
FROM [tbl_member] [mem_1]
INNER JOIN [tbl_department] [dep_2] ON [mem_1].[DepartmentId] = [dep_2].[Id]
WHERE [dep_2].[Name] = @Name_0;

-- Parameters
-- @Name_0 = '業務部' (String)
