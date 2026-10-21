UPDATE [tbl_member]
SET [Name] = tvp.[Name], [Phone] = tvp.[Phone]
FROM [tbl_member] t
INNER JOIN @TableVariable tvp
    ON t.[No] = tvp.[No];
