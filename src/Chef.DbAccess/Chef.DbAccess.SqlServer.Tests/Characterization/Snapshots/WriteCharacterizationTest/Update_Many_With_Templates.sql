UPDATE [mem_1]
SET [mem_1].[Age] = {=Age}, [mem_1].[Phone] = @Phone
FROM [tbl_member] [mem_1]
WHERE [mem_1].[No] = {=Id};
