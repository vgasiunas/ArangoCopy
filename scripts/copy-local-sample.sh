#!/bin/bash
./arangocopy \
--srcdb bench --srccoll docs --dstdb bench --dstcoll docscopy --threads 4 --batch 1000