# Thiet lap bien moi truong cho Spark. Chay bang cach dot-source:
#   . .\setup_env.ps1
# Chi anh huong cua so PowerShell hien tai, khong doi cai dat chung cua may.

# Tim JDK 17 (Spark 3.5 khong chay duoc tren Java moi hon 21).
$jdk = Get-ChildItem 'C:\Program Files\Microsoft\' -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -like 'jdk-17*' } |
    Select-Object -First 1

if ($null -eq $jdk) {
    Write-Host "[LOI] Khong tim thay JDK 17. Chay: winget install Microsoft.OpenJDK.17" -ForegroundColor Red
} else {
    $env:JAVA_HOME = $jdk.FullName
    Write-Host "[OK] JAVA_HOME = $env:JAVA_HOME"
}

# winutils.exe cho Hadoop tren Windows (xem huong dan cai trong README).
$env:HADOOP_HOME = "$env:USERPROFILE\hadoop"
if (-not (Test-Path "$env:HADOOP_HOME\bin\winutils.exe")) {
    Write-Host "[LOI] Thieu winutils.exe - xem muc 'Cai dat' trong README.md" -ForegroundColor Red
} else {
    $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
    Write-Host "[OK] HADOOP_HOME = $env:HADOOP_HOME"
}
