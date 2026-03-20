$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if ($env:SERIEMA_API_BASE_URL) {
    $apiBaseUrl = $env:SERIEMA_API_BASE_URL.TrimEnd("/")
}
else {
    $apiBaseUrl = "http://127.0.0.1:8000"
}

$timeoutSeconds = 5
if ($env:SERIEMA_SMOKE_TIMEOUT_SECONDS) {
    try {
        $timeoutSeconds = [int]$env:SERIEMA_SMOKE_TIMEOUT_SECONDS
    }
    catch {
        $timeoutSeconds = 5
    }
}

$adminHeaders = @{}
if ($env:SERIEMA_ADMIN_TOKEN) {
    $adminHeaders["X-Admin-Token"] = $env:SERIEMA_ADMIN_TOKEN
}

function New-StageResult {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][bool]$Passed,
        [Parameter(Mandatory = $true)][int]$ExitCode,
        [Parameter(Mandatory = $true)][string]$Message,
        [bool]$Unavailable = $false
    )

    [pscustomobject]@{
        Name = $Name
        Passed = $Passed
        ExitCode = $ExitCode
        Message = $Message
        Unavailable = $Unavailable
    }
}

function Invoke-Stage {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    Write-Host ""
    Write-Host "==> $Name"

    try {
        $result = & $Action
        if ($null -eq $result) {
            $result = New-StageResult -Name $Name -Passed $true -ExitCode 0 -Message "ok"
        }
        if ($result.Passed) {
            Write-Host "[PASS] $Name"
        }
        else {
            Write-Host "[FAIL] $Name (exit $($result.ExitCode))"
            if ($result.Message) {
                Write-Host $result.Message
            }
        }
        return $result
    }
    catch {
        $message = $_.Exception.Message
        Write-Host "[FAIL] $Name (exit 1)"
        Write-Host $message
        return New-StageResult -Name $Name -Passed $false -ExitCode 1 -Message $message
    }
}

function Invoke-HttpStage {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Path,
        [scriptblock]$Validator = $null,
        [hashtable]$Headers = @{}
    )

    $uri = "{0}{1}" -f $apiBaseUrl, $Path
    try {
        $response = Invoke-WebRequest -Uri $uri -Method GET -TimeoutSec $timeoutSeconds -Headers $Headers -ErrorAction Stop
        $payload = $null
        if ($response.Content) {
            $payload = $response.Content | ConvertFrom-Json -ErrorAction Stop
        }
        if ($Validator) {
            & $Validator $payload
        }
        return New-StageResult -Name $Name -Passed $true -ExitCode 0 -Message "GET $Path -> 200"
    }
    catch {
        $statusCode = $null
        $message = $_.Exception.Message
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        $unavailable = $false
        if (-not $statusCode -and $message) {
            if ($message -match "connection refused|No connection could be made|Unable to connect") {
                $message = "API unavailable at ${apiBaseUrl}: $message"
                $unavailable = $true
            }
        }
        $exitCode = 1
        $detail = if ($statusCode) {
            "GET $Path -> HTTP $statusCode"
        }
        else {
            "GET $Path -> $message"
        }
        return New-StageResult -Name $Name -Passed $false -ExitCode $exitCode -Message $detail -Unavailable $unavailable
    }
}

function Invoke-ProcessCapture {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $stdoutPath = [System.IO.Path]::GetTempFileName()
    $stderrPath = [System.IO.Path]::GetTempFileName()
    try {
        $process = Start-Process `
            -FilePath $FilePath `
            -ArgumentList $Arguments `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath

        $stdout = Get-Content -Path $stdoutPath -Raw
        $stderr = Get-Content -Path $stderrPath -Raw

        if ($stdout) {
            Write-Host $stdout.TrimEnd()
        }
        if ($stderr) {
            Write-Host $stderr.TrimEnd()
        }

        return $process.ExitCode
    }
    finally {
        Remove-Item -Path $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-PreReleaseFallback {
    $shell = $null
    if (Get-Command pwsh -ErrorAction SilentlyContinue) {
        $shell = (Get-Command pwsh).Source
    }
    elseif (Get-Command powershell -ErrorAction SilentlyContinue) {
        $shell = (Get-Command powershell).Source
    }
    else {
        throw "Unable to locate PowerShell executable for fallback"
    }

    $scriptPath = Join-Path $repoRoot "scripts\pre_release_check.ps1"
    $exitCode = Invoke-ProcessCapture -FilePath $shell -Arguments @(
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        $scriptPath
    )

    if ($exitCode -eq 0) {
        return New-StageResult -Name "pre_release_check" -Passed $true -ExitCode 0 -Message "fallback pre-release check passed"
    }

    return New-StageResult -Name "pre_release_check" -Passed $false -ExitCode $exitCode -Message "fallback pre-release check failed with exit code $exitCode"
}

$results = @()

$results += Invoke-Stage -Name "health" -Action {
    $health = Invoke-HttpStage -Name "health" -Path "/health"
    if ($health.Passed) {
        return $health
    }
    return $health
}

$apiUnavailable = $results[0].Unavailable

if (-not $apiUnavailable) {
    $results += Invoke-Stage -Name "health/deps" -Action {
        Invoke-HttpStage -Name "health/deps" -Path "/health/deps" -Headers $adminHeaders -Validator {
            param($payload)
            if (-not $payload) { throw "health/deps returned an empty payload" }
            if ($payload.overall -ne "ok") { throw "health/deps overall status is $($payload.overall)" }
            if ($payload.postgres.status -ne "ok") { throw "postgres dependency is $($payload.postgres.status)" }
            if ($payload.redis.status -ne "ok") { throw "redis dependency is $($payload.redis.status)" }
        }
    }

    $results += Invoke-Stage -Name "metrics/ops" -Action {
        Invoke-HttpStage -Name "metrics/ops" -Path "/metrics/ops" -Headers $adminHeaders -Validator {
            param($payload)
            if (-not $payload.redis_key) { throw "metrics/ops did not include redis_key" }
            if ($null -eq $payload.metrics) { throw "metrics/ops did not include metrics" }
        }
    }

    $results += Invoke-Stage -Name "metrics/queues" -Action {
        Invoke-HttpStage -Name "metrics/queues" -Path "/metrics/queues" -Headers $adminHeaders -Validator {
            param($payload)
            foreach ($field in @("dispatch", "voice", "telegram", "email", "escalation", "dlq")) {
                if ($null -eq $payload.$field) { throw "metrics/queues missing $field" }
            }
        }
    }

    $results += Invoke-Stage -Name "metrics/sla" -Action {
        Invoke-HttpStage -Name "metrics/sla" -Path "/metrics/sla" -Headers $adminHeaders -Validator {
            param($payload)
            if ($null -eq $payload.hours) { throw "metrics/sla missing hours" }
            if ($null -eq $payload.total_incidents) { throw "metrics/sla missing total_incidents" }
            if ($null -eq $payload.incidents_by_status) { throw "metrics/sla missing incidents_by_status" }
        }
    }

    $results += Invoke-Stage -Name "ops/integration/status" -Action {
        Invoke-HttpStage -Name "ops/integration/status" -Path "/ops/integration/status" -Headers $adminHeaders -Validator {
            param($payload)
            foreach ($field in @(
                "enums_ok",
                "fallback_contract_ok",
                "trace_propagation_signal",
                "duplicate_event_signal",
                "ack_flow_signal",
                "escalation_guard_signal",
                "dlq_reporting_signal"
            )) {
                if ($null -eq $payload.$field) { throw "ops/integration/status missing $field" }
            }
        }
    }

    $results += Invoke-Stage -Name "ops/readiness" -Action {
        Invoke-HttpStage -Name "ops/readiness" -Path "/ops/readiness" -Headers $adminHeaders -Validator {
            param($payload)
            if ($null -eq $payload.score) { throw "ops/readiness missing score" }
            if (-not @("green", "yellow", "red").Contains([string]$payload.status)) { throw "ops/readiness status is invalid" }
            if ($null -eq $payload.checks) { throw "ops/readiness missing checks" }
            if ($null -eq $payload.blockers) { throw "ops/readiness missing blockers" }
        }
    }
}
else {
    $results += Invoke-Stage -Name "health/deps" -Action { New-StageResult -Name "health/deps" -Passed $false -ExitCode 1 -Message "skipped because /health was unavailable" }
    $results += Invoke-Stage -Name "metrics/ops" -Action { New-StageResult -Name "metrics/ops" -Passed $false -ExitCode 1 -Message "skipped because /health was unavailable" }
    $results += Invoke-Stage -Name "metrics/queues" -Action { New-StageResult -Name "metrics/queues" -Passed $false -ExitCode 1 -Message "skipped because /health was unavailable" }
    $results += Invoke-Stage -Name "metrics/sla" -Action { New-StageResult -Name "metrics/sla" -Passed $false -ExitCode 1 -Message "skipped because /health was unavailable" }
    $results += Invoke-Stage -Name "ops/integration/status" -Action { New-StageResult -Name "ops/integration/status" -Passed $false -ExitCode 1 -Message "skipped because /health was unavailable" }
    $results += Invoke-Stage -Name "ops/readiness" -Action { New-StageResult -Name "ops/readiness" -Passed $false -ExitCode 1 -Message "skipped because /health was unavailable" }
}

$results += Invoke-Stage -Name "pre_release_check" -Action {
    Invoke-PreReleaseFallback
}

Write-Host ""
Write-Host "Handoff smoke summary"
foreach ($result in $results) {
    $status = if ($result.Passed) { "PASS" } else { "FAIL ($($result.ExitCode))" }
    Write-Host ("{0,-28} {1}" -f $result.Name, $status)
}

if ($results.Passed -contains $false) {
    exit 1
}

exit 0
