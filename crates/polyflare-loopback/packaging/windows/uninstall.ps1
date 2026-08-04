$ErrorActionPreference = "Stop"
$ServiceName = "PolyFlareLoopback"
$Destination = Join-Path (Join-Path $env:LOCALAPPDATA "PolyFlare") "polyflare-loopback.exe"

& sc.exe stop $ServiceName | Out-Null
Start-Sleep -Seconds 1
& sc.exe delete $ServiceName
if ($LASTEXITCODE -ne 0) { throw "Could not delete the Windows service" }
Remove-Item -Force -ErrorAction SilentlyContinue $Destination
