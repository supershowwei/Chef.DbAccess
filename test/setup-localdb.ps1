# 在 LocalDB 建立測試資料庫（Club、Advertisement、Member），可以重複執行。
# 已存在的同名資料庫會先刪除，再以 SampleDB.sql 重建。
param(
    [string]$Server = '(localdb)\MSSQLLocalDB'
)

$ErrorActionPreference = 'Stop'

if ($Server -like '(localdb)\*') {
    sqllocaldb start ($Server -replace '^\(localdb\)\\', '') | Out-Null
}

$databases = 'Club', 'Advertisement', 'Member'

foreach ($db in $databases) {
    sqlcmd -S $Server -E -b -Q "IF DB_ID(N'$db') IS NOT NULL BEGIN ALTER DATABASE [$db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$db]; END"
    if ($LASTEXITCODE -ne 0) { throw "刪除資料庫 $db 失敗" }
}

# SampleDB.sql 是 Big5 編碼，以字碼頁 950 讀取
sqlcmd -S $Server -E -b -f 950 -i (Join-Path $PSScriptRoot 'SampleDB.sql') | Out-Null
if ($LASTEXITCODE -ne 0) { throw '執行 SampleDB.sql 失敗' }

Write-Host "已在 $Server 建立測試資料庫：$($databases -join '、')"
