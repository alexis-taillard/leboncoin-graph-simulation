# Lance le traitement Spark Streaming sous Windows (PowerShell)
# Usage : .\lancer_spark.ps1   (depuis le dossier du projet)
# Si PowerShell bloque l'exécution du script :
#   powershell -ExecutionPolicy Bypass -File .\lancer_spark.ps1

Set-Location $PSScriptRoot

# Ignore un JAVA_HOME qui pointerait vers un dossier qui n'existe plus
if ($env:JAVA_HOME -and -not (Test-Path $env:JAVA_HOME)) {
    Remove-Item Env:JAVA_HOME
}

# Utiliser le Python courant (et non le raccourci du Microsoft Store)
$env:PYSPARK_PYTHON = "python"

# Charger le package GraphFrames (version adaptée à Spark 3.5 / Scala 2.12)
$env:PYSPARK_SUBMIT_ARGS = "--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 pyspark-shell"

# Hadoop sous Windows : winutils.exe et hadoop.dll doivent être dans %HADOOP_HOME%\bin
if (-not $env:HADOOP_HOME) {
    $env:HADOOP_HOME = "C:\hadoop"
}
$env:PATH += ";$env:HADOOP_HOME\bin"

python graph_streaming.py
