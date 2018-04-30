#!/bin/bash
set -euxo pipefail

memory=( 128 256 512 1024 1536 2048 3008 )
concurrency=( 1 2 4 8 10 16 )

echo -n "" > results.json

for m in "${memory[@]}"; do
    for c in "${concurrency[@]}"; do
      echo "Deploying with $m memory and $c concurrency ..."

      sls deploy --memory=$m --concurrency=$c

      echo "Running test ..."
      RESULT="$(sls invoke -f enqueue --memory=$m --concurrency=$c)"
      echo -e "{ \"memory\": $m, \"concurrency\": $c, \"rps\": $RESULT }" >> results.json
    done
done

sls remove