# HiveMind full-system campaign runner.
#
# One command, one verdict. The deterministic lane uses a local scripted
# provider and never spends provider credit. Live/UI lanes are declared in the
# report and must be enabled explicitly before they can turn the verdict green.
[CmdletBinding()]
param(
    [ValidateSet('full', 'preflight', 'deterministic')]
    [string]$Mode = 'full',
    [switch]$Live,
    [switch]$Scale
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$orchestrator = Join-Path $PSScriptRoot 'orchestrator.mjs'
if (-not (Test-Path -LiteralPath $orchestrator)) {
    throw "full-system orchestrator missing at $orchestrator"
}

$env:FS_PREFIX = if ($Live) { 'campaign-live-requested' } else { 'campaign' }
if ($Live) {
    Write-Host 'Live lane requested. The current executable slice records it as BLOCKED until the paid lane is enabled; no real provider call is made by this runner.' -ForegroundColor Yellow
}
if ($Scale) {
    Write-Host 'Scale lane requested. The current executable slice records SCALE as NOT_IMPLEMENTED; no live 40-node claim is made.' -ForegroundColor Yellow
}

Push-Location $root
try {
    node $orchestrator
    $exit = $LASTEXITCODE
    Write-Host ("full-system exit code: " + $exit)
    exit $exit
}
finally {
    Pop-Location
}
