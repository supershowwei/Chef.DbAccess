#Requires -Version 7.0
# 比對基準情境的配置數量（§10.3）：讀 BenchmarkDotNet 的 JSON 報表，與 allocation-baseline.json 逐一比對。
# - 增加超過容許誤差、新增或少了情境：失敗（新增、刪除情境時，基準檔要在同一個 commit 一起更新）
# - 減少超過容許誤差：通過，提醒可以更新基準
# - 還沒有基準檔：只記錄，不比對
# 指定 -Output 時，把這次的結果寫成基準檔的格式；要更新基準時用它取代 allocation-baseline.json。
# 有設定 GITHUB_STEP_SUMMARY 時，比對結果另外寫進 job summary。
param(
    [Parameter(Mandatory)]
    [string]$Results,
    [string]$Baseline = (Join-Path $PSScriptRoot 'allocation-baseline.json'),
    [string]$Output
)

$ErrorActionPreference = 'Stop'

# 各執行環境的容許誤差：基準的百分比與位元組數，取較大的值。依 GitHub runner 上兩次執行的差異訂定：
# - .NET 10：小情境最多差 77 B，大情境最多差 0.12%
# - .NET Framework 4.8：配置數量的量測比較粗，小情境最多差 702 B，大情境最多差 1.18%（SqlBulkCopy，每輪只執行一次）
$tolerances = @{
    '.NET 10.0'          = @{ Percent = 0.5; Bytes = 256 }
    '.NET Framework 4.8' = @{ Percent = 3; Bytes = 2048 }
}

function Get-SortedKeys {
    # 傳入一或多個字典，傳回所有鍵的聯集，以序數排序
    $set = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($dictionary in $args) { if ($dictionary) { foreach ($key in $dictionary.Keys) { [void]$set.Add($key) } } }

    $keys = [string[]]@($set)
    [Array]::Sort($keys, [StringComparer]::Ordinal)
    return $keys
}

# 執行環境 → 情境全名 → 每次操作配置的位元組數
$current = @{}

$reports = Get-ChildItem $Results -Filter '*.json'
if (-not $reports) { throw "在 $Results 找不到 BenchmarkDotNet 的 JSON 報表" }

foreach ($report in $reports) {
    foreach ($benchmark in (Get-Content $report.FullName -Raw | ConvertFrom-Json).Benchmarks) {
        if ($benchmark.DisplayInfo -notmatch 'Runtime=(?<runtime>[^,)]+)') { throw "無法從 $($benchmark.DisplayInfo) 判斷執行環境" }

        $runtime = $Matches.runtime
        $name = $benchmark.FullName

        # 情境執行失敗時（例如 GlobalSetup 的結果檢查丟出例外）沒有統計數字，BenchmarkDotNet 仍然會正常結束
        if ($null -eq $benchmark.Statistics -or $null -eq $benchmark.Memory.BytesAllocatedPerOperation) {
            throw "$name（$runtime）沒有量測結果，請看 log"
        }

        if (-not $current.Contains($runtime)) { $current[$runtime] = @{} }
        if ($current[$runtime].Contains($name)) { throw "$name（$runtime）重複出現" }

        $current[$runtime][$name] = [long]$benchmark.Memory.BytesAllocatedPerOperation
    }
}

if ($Output) {
    $ordered = [ordered]@{}
    foreach ($runtime in Get-SortedKeys $current) {
        $ordered[$runtime] = [ordered]@{}
        foreach ($name in Get-SortedKeys $current[$runtime]) { $ordered[$runtime][$name] = $current[$runtime][$name] }
    }

    [IO.File]::WriteAllText($Output, ($ordered | ConvertTo-Json -Depth 3) + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
}

$expected = if (Test-Path $Baseline) { Get-Content $Baseline -Raw | ConvertFrom-Json -AsHashtable } else { $null }

$rows = [Collections.Generic.List[object]]::new()
foreach ($runtime in Get-SortedKeys $current $expected) {
    $actualValues = if ($current.Contains($runtime)) { $current[$runtime] } else { @{} }
    $expectedValues = if ($expected -and $expected.Contains($runtime)) { $expected[$runtime] } else { @{} }

    if ($expected -and -not $tolerances.Contains($runtime)) { throw "沒有設定 $runtime 的容許誤差" }

    foreach ($name in Get-SortedKeys $actualValues $expectedValues) {
        $actual = if ($actualValues.Contains($name)) { [long]$actualValues[$name] } else { $null }
        $before = if ($expectedValues.Contains($name)) { [long]$expectedValues[$name] } else { $null }
        $tolerance = if ($null -ne $before) { [math]::Max($before * $tolerances[$runtime].Percent / 100, $tolerances[$runtime].Bytes) } else { 0 }

        $status =
            if (-not $expected) { '記錄' }
            elseif ($null -eq $before) { '新增' }
            elseif ($null -eq $actual) { '缺少' }
            elseif ($actual -gt $before + $tolerance) { '增加' }
            elseif ($actual -lt $before - $tolerance) { '減少' }
            else { '相同' }

        $rows.Add([pscustomobject]@{
            Runtime = $runtime
            Name    = ($name -replace '^Chef\.DbAccess\.Benchmarks\.', '')
            Before  = $before
            Actual  = $actual
            Status  = $status
        })
    }
}

function Format-Bytes($value) { if ($null -eq $value) { '—' } else { $value.ToString('N0') } }

function Format-Change($row) {
    if ($null -eq $row.Before -or $null -eq $row.Actual) { return '—' }

    $difference = $row.Actual - $row.Before
    if ($difference -eq 0) { return '0' }

    $text = $difference.ToString('+#,0;-#,0')

    if ($row.Before -ne 0) { $text += '（' + ($difference / $row.Before).ToString('+0.###%;-0.###%;0%') + '）' }

    return $text
}

$failures = @($rows | Where-Object Status -in '增加', '新增', '缺少')
$decreases = @($rows | Where-Object Status -eq '減少')

$lines = [Collections.Generic.List[string]]::new()
$lines.Add('## 配置數量比對')
$lines.Add('')

if (-not $expected) {
    $lines.Add("還沒有基準檔 ``$(Split-Path $Baseline -Leaf)``，這次只記錄配置數量，不比對。")
    $lines.Add('')
}
else {
    $lines.Add('容許誤差（取較大的值）：' + ((Get-SortedKeys $tolerances | ForEach-Object { "$_ 為基準的 $($tolerances[$_].Percent)％ 或 $($tolerances[$_].Bytes.ToString('N0')) B" }) -join '；') + '。')
    $lines.Add('')
    if ($failures) { $lines.Add("**$($failures.Count) 個情境不符合基準**：配置增加，或是情境有新增、刪除。確認是預期的變化後，用這次產生的基準檔更新 ``$(Split-Path $Baseline -Leaf)``。"); $lines.Add('') }
    if ($decreases) { $lines.Add("$($decreases.Count) 個情境的配置減少了，可以用這次產生的基準檔更新基準。"); $lines.Add('') }
    if (-not $failures -and -not $decreases) { $lines.Add('所有情境都與基準相同。'); $lines.Add('') }
}

$lines.Add('| 執行環境 | 情境 | 基準（B/op） | 這次（B/op） | 差異 | 結果 |')
$lines.Add('|---|---|---:|---:|---:|---|')

foreach ($row in $rows) {
    $status = if ($row.Status -in '增加', '新增', '缺少') { "**$($row.Status)**" } else { $row.Status }
    $lines.Add("| $($row.Runtime) | $($row.Name) | $(Format-Bytes $row.Before) | $(Format-Bytes $row.Actual) | $(Format-Change $row) | $status |")
}

$lines | Write-Host
if ($env:GITHUB_STEP_SUMMARY) { $lines | Out-File $env:GITHUB_STEP_SUMMARY -Append -Encoding utf8 }

if ($failures) { throw "配置數量比對失敗：$($failures.Count) 個情境不符合基準" }
