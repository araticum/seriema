$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Invoke-Stage {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [scriptblock]$Action
    )

    Write-Host ""
    Write-Host "==> $Name"

    $exitCode = 0
    try {
        $exitCode = & $Action
    }
    catch {
        $exitCode = 1
        Write-Host $_.Exception.Message
    }

    if ($exitCode -eq 0) {
        Write-Host "[PASS] $Name"
    }
    else {
        Write-Host "[FAIL] $Name (exit $exitCode)"
    }

    return [pscustomobject]@{
        Name = $Name
        ExitCode = $exitCode
        Passed = ($exitCode -eq 0)
    }
}

function Invoke-PythonStage {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $stdoutPath = [System.IO.Path]::GetTempFileName()
    $stderrPath = [System.IO.Path]::GetTempFileName()
    try {
        $process = Start-Process `
            -FilePath "py" `
            -ArgumentList $Arguments `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath

        if ($process.ExitCode -ne 0) {
            $stderr = Get-Content -Path $stderrPath -Raw
            if ($stderr) {
                Write-Host $stderr.TrimEnd()
            }
        }

        return $process.ExitCode
    }
    finally {
        Remove-Item -Path $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

$results = @()
$results += Invoke-Stage -Name "compileall" -Action {
    Invoke-PythonStage -Arguments @("-3", "-m", "compileall", "alembic", "config.py", "database.py", "engine.py", "main.py", "models.py", "redis_client.py", "schemas.py", "worker.py", "tests")
}
$results += Invoke-Stage -Name "pytest -q" -Action {
    Invoke-PythonStage -Arguments @("-3", "-m", "pytest", "tests", "-q")
}
$results += Invoke-Stage -Name "alembic upgrade head --sql" -Action {
    Invoke-PythonStage -Arguments @("-3", "-m", "alembic", "-c", "alembic.ini", "upgrade", "head", "--sql")
}

Write-Host ""
Write-Host "Pre-release summary"
foreach ($result in $results) {
    $status = if ($result.Passed) { "PASS" } else { "FAIL ($($result.ExitCode))" }
    Write-Host ("{0,-30} {1}" -f $result.Name, $status)
}

if ($results.Passed -contains $false) {
    exit 1
}

exit 0
