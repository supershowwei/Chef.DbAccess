# 在 LocalDB 建立測試資料庫（Club、Advertisement、Member），可以重複執行。
# 已存在的同名資料庫會先刪除，再以 SampleDB.sql 重建。
# SampleDB.sql 把相容性層級設為 130；指定 -CompatibilityLevel 時改成該層級（例如 100，驗證舊相容性層級的資料庫）。
param(
    [string]$Server = '(localdb)\MSSQLLocalDB',
    [int]$CompatibilityLevel
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

# SampleDB.sql 是 UTF-8 編碼，以字碼頁 65001 讀取
sqlcmd -S $Server -E -b -f 65001 -i (Join-Path $PSScriptRoot 'SampleDB.sql') | Out-Null
if ($LASTEXITCODE -ne 0) { throw '執行 SampleDB.sql 失敗' }

if ($CompatibilityLevel) {
    foreach ($db in $databases) {
        sqlcmd -S $Server -E -b -Q "ALTER DATABASE [$db] SET COMPATIBILITY_LEVEL = $CompatibilityLevel"
        if ($LASTEXITCODE -ne 0) { throw "設定資料庫 $db 的相容性層級失敗" }
    }
}

Write-Host "已在 $Server 建立測試資料庫：$($databases -join '、')"
if ($CompatibilityLevel) { Write-Host "相容性層級：$CompatibilityLevel" }
