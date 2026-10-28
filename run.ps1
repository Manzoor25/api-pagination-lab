# Windows entry point. macOS and Linux can use the Makefile.
#   .\run.ps1 setup
#   .\run.ps1 api
#   .\run.ps1 demo
param(
    [Parameter(Position = 0)]
    [ValidateSet("setup", "api", "demo", "benchmark", "test", "real", "site", "help")]
    [string]$Target = "help"
)

$ErrorActionPreference = "Stop"
$env:PYTHONIOENCODING = "utf-8"
Set-Location $PSScriptRoot
$Python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

function Require-Venv {
    if (-not (Test-Path $Python)) {
        Write-Host "No virtualenv yet. Run: .\run.ps1 setup"
        exit 1
    }
}

switch ($Target) {
    "setup" {
        if (-not (Test-Path $Python)) {
            python -m venv .venv
        }
        & $Python -m pip install --upgrade pip
        & $Python -m pip install -r requirements.txt
        Write-Host "Setup complete. Next: .\run.ps1 api"
    }
    "api" {
        Require-Venv
        & $Python -m mock_api.server
    }
    "demo" {
        Require-Venv
        & $Python demo.py
    }
    "benchmark" {
        Require-Venv
        & $Python benchmark.py --mode both
    }
    "test" {
        Require-Venv
        & $Python -m ruff check .
        & $Python -m pytest
    }
    "real" {
        Require-Venv
        & $Python diagnose.py --preset fdic --samples 3 --page-size 1000
        & $Python fetch.py --adapter fdic --strategy offset --page-size 10000 --rate-limit-rps 2 --out data\landing
        $latest = Get-ChildItem -Directory data\landing\fdic-offset | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($latest) {
            & $Python validate.py --run-dir $latest.FullName --no-dense-ids
        }
    }
    "site" {
        Write-Host "Open site\index.html in a browser, or:"
        Write-Host "  python -m http.server 8080 --directory site"
    }
    default {
        Write-Host "Targets: setup, api, demo, benchmark, test, real, site"
    }
}
