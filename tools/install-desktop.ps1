# Install HiveMind per-user (no administrator rights).
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File tools\install-desktop.ps1
#   powershell -ExecutionPolicy Bypass -File tools\install-desktop.ps1 -SkipProvider -NoLaunch
#
# The installer never receives a provider key on its command line. With provider
# setup enabled it prompts with Read-Host -AsSecureString and writes only the
# DPAPI-encrypted provider.key under the data directory.
[CmdletBinding()]
param(
    [string]$InstallRoot = (Join-Path $env:LOCALAPPDATA 'Programs\HiveMind\app'),
    [string]$DataRoot = (Join-Path $env:LOCALAPPDATA 'HiveMind'),
    [string]$WorkspaceRoot = (Join-Path $env:LOCALAPPDATA 'HiveMind\workspace'),
    [ValidateSet('deepseek', 'tokenlab', 'custom', 'none')]
    [string]$Provider = 'deepseek',
    [string]$BaseUrl,
    [string]$WireModel,
    [switch]$SkipProvider,
    [switch]$NoLaunch,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$source = Join-Path $root 'dist\HiveMind'
# The portable zip puts the installer and binaries in the same folder; the
# repository keeps them under dist\HiveMind.
if (-not (Test-Path -LiteralPath (Join-Path $source 'HiveMind.exe'))) {
    $source = $PSScriptRoot
}
$launcher = Join-Path $source 'HiveMind.exe'
$server = Join-Path $source 'hiverouter.exe'
if (-not (Test-Path -LiteralPath $launcher) -or -not (Test-Path -LiteralPath $server)) {
    throw "portable package missing at $source; run tools\build-desktop.ps1 first"
}
if ((Test-Path -LiteralPath $InstallRoot) -and -not $Force) {
    $existing = Get-ChildItem -LiteralPath $InstallRoot -Force -ErrorAction SilentlyContinue
    if ($null -ne $existing -and @($existing).Count -gt 0) {
        throw "install root already exists: $InstallRoot (use -Force to replace the app files)"
    }
}

New-Item -ItemType Directory -Force -Path $InstallRoot | Out-Null
New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
New-Item -ItemType Directory -Force -Path $WorkspaceRoot | Out-Null
Copy-Item -LiteralPath $launcher -Destination $InstallRoot -Force
Copy-Item -LiteralPath $server -Destination $InstallRoot -Force
if (Test-Path -LiteralPath (Join-Path $source 'README.txt')) {
    Copy-Item -LiteralPath (Join-Path $source 'README.txt') -Destination $InstallRoot -Force
}
foreach ($extra in @('set-provider-key.ps1', 'set-provider-profile.ps1', 'hivemind-cli.mjs', 'hivemind.cmd', 'hivemind.ps1', 'hivemind-mcp.mjs', 'CHECKSUMS.txt', 'BUILD.json')) {
    $candidate = Join-Path $root "tools\$extra"
    if (-not (Test-Path -LiteralPath $candidate)) { $candidate = Join-Path $source $extra }
    if (Test-Path -LiteralPath $candidate) {
        Copy-Item -LiteralPath $candidate -Destination (Join-Path $InstallRoot $extra) -Force
    }
}

# Shortcuts: Desktop and Start Menu, current user only.
function New-Shortcut([string]$Path) {
    $dir = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($Path)
    $shortcut.TargetPath = Join-Path $InstallRoot 'HiveMind.exe'
    $shortcut.WorkingDirectory = $InstallRoot
    $shortcut.Description = 'HiveMind local HMARS'
    $shortcut.Save()
}
New-Shortcut (Join-Path ([Environment]::GetFolderPath('Desktop')) 'HiveMind.lnk')
New-Shortcut (Join-Path ([Environment]::GetFolderPath('StartMenu')) 'Programs\HiveMind\HiveMind.lnk')

if (-not $SkipProvider) {
    $profile = @{
        name = $Provider
        api_type = if ($Provider -eq 'deepseek') { 'deepseek' } else { 'tokenlabs' }
        model = 'deepseek-flash'
    }
    if ($Provider -eq 'tokenlab') {
        if ([string]::IsNullOrWhiteSpace($BaseUrl)) { throw 'TokenLab requires -BaseUrl.' }
        if ([string]::IsNullOrWhiteSpace($WireModel)) { throw 'TokenLab requires -WireModel.' }
        $profile.base_url = $BaseUrl.TrimEnd('/')
        $profile.wire_model = $WireModel
    } elseif ($Provider -eq 'custom') {
        if ([string]::IsNullOrWhiteSpace($BaseUrl) -or [string]::IsNullOrWhiteSpace($WireModel)) {
            throw 'Custom provider requires -BaseUrl and -WireModel.'
        }
        $profile.base_url = $BaseUrl.TrimEnd('/')
        $profile.wire_model = $WireModel
    }
    if ($Provider -ne 'none') {
        $json = $profile | ConvertTo-Json -Depth 4
        [IO.File]::WriteAllText((Join-Path $DataRoot 'provider.json'), $json, (New-Object System.Text.UTF8Encoding($false)))
        $secure = Read-Host -AsSecureString 'Provider API key (input hidden; Enter to skip)'
        $bstr = [IntPtr]::Zero
        try {
            $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
            $plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
            if (-not [string]::IsNullOrWhiteSpace($plain)) {
                Add-Type -AssemblyName System.Security
                $entropy = [Text.Encoding]::UTF8.GetBytes('HiveMind.ProviderKey.v1')
                $bytes = [Text.Encoding]::UTF8.GetBytes($plain.Trim())
                $cipher = [Security.Cryptography.ProtectedData]::Protect(
                    $bytes,
                    $entropy,
                    [Security.Cryptography.DataProtectionScope]::CurrentUser
                )
                [IO.File]::WriteAllBytes((Join-Path $DataRoot 'provider.key'), $cipher)
                [Array]::Clear($bytes, 0, $bytes.Length)
            }
        } finally {
            if ($bstr -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
            $plain = $null
        }
    }
}

[IO.File]::WriteAllText(
    (Join-Path $InstallRoot 'install.json'),
    (@{
        installed_at = (Get-Date).ToString('o')
        install_root = $InstallRoot
        data_root = $DataRoot
        workspace_root = $WorkspaceRoot
        source = $source
    } | ConvertTo-Json -Depth 3),
    (New-Object System.Text.UTF8Encoding($false))
)

Write-Host "Installed HiveMind to $InstallRoot" -ForegroundColor Green
Write-Host "Data: $DataRoot" -ForegroundColor Green
Write-Host "Workspace: $WorkspaceRoot" -ForegroundColor Green
Write-Host "Shortcuts: Desktop\\HiveMind.lnk and Start Menu\\Programs\\HiveMind\\HiveMind.lnk" -ForegroundColor Green

if (-not $NoLaunch) {
    $process = Start-Process -FilePath (Join-Path $InstallRoot 'HiveMind.exe') -WorkingDirectory $InstallRoot -PassThru
    Start-Sleep -Seconds 3
    $health = $null
    # The launcher picks the first free port in 8099..8199; probing only 8099
    # reports a false failure whenever that port is occupied.
    for ($i = 0; $i -lt 20 -and $null -eq $health; $i++) {
        foreach ($port in 8099..8199) {
            try {
                $health = Invoke-RestMethod -Uri ("http://127.0.0.1:$port/health") -TimeoutSec 1
                break
            } catch {
                # Try the next candidate port.
            }
        }
        if ($null -eq $health) {
            Start-Sleep -Milliseconds 500
        }
    }
    if ($null -eq $health) {
        Write-Warning 'HiveMind did not answer /health; launch it from the shortcut and inspect %LOCALAPPDATA%\HiveMind.'
    } else {
        Write-Host ("Health: " + ($health | ConvertTo-Json -Compress)) -ForegroundColor Green
    }
}
