# FlashDrop: 10,000 Customer High-Concurrency Flash Sale Simulation (Windows PowerShell)
$ErrorActionPreference = "Stop"

$Port = if ($env:PORT) { $env:PORT } else { "8088" }
$TargetUrl = "http://127.0.0.1:$Port"

Write-Host ">>> Building FlashDrop release binary & 10k customer example..." -ForegroundColor Cyan
cargo build --release --bin flashdrop --example simulate_10k_customers

Write-Host ">>> Launching FlashDrop server on port $Port..." -ForegroundColor Cyan
$env:PORT = $Port
$serverProcess = Start-Process -FilePath "target\release\flashdrop.exe" -PassThru -NoNewWindow

try {
    Write-Host ">>> Awaiting server health check at $TargetUrl/health..." -ForegroundColor Yellow
    $healthy = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $response = Invoke-RestMethod -Uri "$TargetUrl/health" -Method Get -TimeoutSec 1
            if ($response.status -eq "UP") {
                Write-Host ">>> Server is UP and healthy!" -ForegroundColor Green
                $healthy = $true
                break
            }
        } catch {
            Start-Sleep -Milliseconds 150
        }
    }

    if (-not $healthy) {
        throw "Server failed to start within timeout window."
    }

    Write-Host ">>> Starting 10,000 customer concurrent load simulation..." -ForegroundColor Cyan
    $env:TARGET_URL = $TargetUrl
    & "target\release\examples\simulate_10k_customers.exe"

    Write-Host ">>> 10,000 customer simulation completed successfully!" -ForegroundColor Green
} finally {
    Write-Host "`n>>> Stopping FlashDrop server..." -ForegroundColor Yellow
    if ($serverProcess -and -not $serverProcess.HasExited) {
        Stop-Process -Id $serverProcess.Id -Force
    }
    Write-Host ">>> Done." -ForegroundColor Green
}
