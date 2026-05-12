# From repo root: start the stack and follow api-feed -> Kafka -> Spark streaming logs.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

docker compose up -d --build
docker compose logs -f api-feed kafka spark-streaming
