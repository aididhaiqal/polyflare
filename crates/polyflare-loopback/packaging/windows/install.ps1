param(
    [Parameter(Mandatory = $true)] [string] $BinaryPath,
    [Parameter(Mandatory = $true)] [string] $UpstreamOrigin
)

$ErrorActionPreference = "Stop"
$ServiceName = "PolyFlareLoopback"
$InstallDirectory = Join-Path $env:LOCALAPPDATA "PolyFlare"
$Destination = Join-Path $InstallDirectory "polyflare-loopback.exe"

& $BinaryPath --upstream-origin $UpstreamOrigin --check-config
if ($LASTEXITCODE -ne 0) { throw "The companion rejected this configuration" }
New-Item -ItemType Directory -Force -Path $InstallDirectory | Out-Null
Copy-Item -Force $BinaryPath $Destination
$Arguments = "`"$Destination`" --windows-service --upstream-origin `"$UpstreamOrigin`" --listen 127.0.0.1:8080"
& sc.exe create $ServiceName binPath= $Arguments start= auto DisplayName= "PolyFlare Loopback Companion"
if ($LASTEXITCODE -ne 0) { throw "Could not create the Windows service" }
& sc.exe description $ServiceName "Loopback bridge for a remotely hosted PolyFlare instance"
& sc.exe start $ServiceName
if ($LASTEXITCODE -ne 0) { throw "The service was installed but did not start" }
$Healthy = $false
for ($Attempt = 0; $Attempt -lt 30; $Attempt++) {
    try {
        $Health = Invoke-RestMethod -TimeoutSec 1 -Uri "http://127.0.0.1:8080/_polyflare-loopback/health"
        if ($Health.status -eq "ok" -and $Health.mode -eq "remote-polyflare-loopback") {
            $Healthy = $true
            break
        }
    } catch {}
    Start-Sleep -Milliseconds 500
}
if (-not $Healthy) { throw "The service was installed but its loopback health check failed" }
