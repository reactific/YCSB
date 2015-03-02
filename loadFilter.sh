#!/bin/bash
grep "RxMongo-akka.actor.default-dispatcher-" load.out | \
grep -v 'UTC  DEBUG ' | \
grep -v 'localhost' | \
grep -v "registered as connection handler" | \
grep -v "Handling Reply" | \
sed -e 's/^[0-9].*UTC //' -e 's/DEBUG [^ ]* //' | \
sort -n -k 1 | \
gawk -f time.awk
