param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $ComposeArgs = @("up", "--build")
)

$ErrorActionPreference = "Stop"

$podmanCommand = Get-Command podman -ErrorAction SilentlyContinue
$podmanExe = if ($podmanCommand) { $podmanCommand.Source } else { $null }

if (-not $podmanExe) {
    $defaultPodmanExe = "C:\Program Files\RedHat\Podman\podman.exe"

    if (-not (Test-Path $defaultPodmanExe)) {
        throw "Podman nao encontrado no PATH nem em '$defaultPodmanExe'."
    }

    $podmanExe = $defaultPodmanExe
    $podmanDir = Split-Path $podmanExe -Parent
    $env:Path = "$env:Path;$podmanDir"
}

if (-not $env:PODMAN_COMPOSE_PROVIDER) {
    $env:PODMAN_COMPOSE_PROVIDER = "podman-compose"
}

$projectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$composeFile = Join-Path $projectRoot "docker-compose.yml"

if (-not (Test-Path $composeFile)) {
    throw "docker-compose.yml nao encontrado em '$projectRoot'."
}

Push-Location $projectRoot
try {
    & $podmanExe compose -f $composeFile @ComposeArgs
}
finally {
    Pop-Location
}
