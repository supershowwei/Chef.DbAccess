UPDATE [mem_1]
SET [mem_1].[Age] = {=Age_0}, [mem_1].[Phone] = @Phone_0
FROM [tbl_member] [mem_1]
WHERE [mem_1].[No] = {=Id_0};
