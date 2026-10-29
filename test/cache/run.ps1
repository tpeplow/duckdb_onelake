param(
    [Parameter(Mandatory = $true)]
    [string]$TempDirectory,
    [string]$Binary = (Join-Path $PSScriptRoot '..\..\build\release\extension\onelake\onelake_cache_storage_test.exe')
)

$ErrorActionPreference = 'Stop'
$binaryPath = (Resolve-Path -LiteralPath $Binary).Path
$path = [System.IO.Path]::GetFullPath($TempDirectory)
if (Test-Path -LiteralPath $path) {
    throw 'The test directory must not already exist.'
}

New-Item -ItemType Directory -Path $path | Out-Null
$acl = Get-Acl -LiteralPath $path
$acl.SetAccessRuleProtection($true, $false)
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acl.SetOwner($user)
$principals = @(
    $user,
    [System.Security.Principal.SecurityIdentifier]::new('S-1-5-18'),
    [System.Security.Principal.SecurityIdentifier]::new('S-1-5-32-544')
)
foreach ($principal in $principals) {
    $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
        $principal, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow'
    ))
}
Set-Acl -LiteralPath $path -AclObject $acl

$junction = "$path.junction"
if (Test-Path -LiteralPath $junction) {
    throw 'The junction fixture must not already exist.'
}
New-Item -ItemType Junction -Path $junction -Target $path | Out-Null
try {
    $rejection = (& $binaryPath $junction 2>&1 | Out-String)
    if ($LASTEXITCODE -eq 0 -or $rejection -notmatch 'existing private spill directory') {
        throw 'The junction fixture was not rejected by cache admission.'
    }
} finally {
    Remove-Item -LiteralPath $junction
}
Write-Output 'Windows junction admission rejection passed'
& $binaryPath $path
$result = $LASTEXITCODE
# Only remove this newly created fixture if it is empty; never recurse.
if ((Test-Path -LiteralPath $path) -and
    @(Get-ChildItem -LiteralPath $path -Force).Count -eq 0) {
    Remove-Item -LiteralPath $path
}
exit $result
