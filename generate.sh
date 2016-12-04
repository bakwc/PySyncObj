#!/usr/bin/env bash

echo "REVISION = '$(git rev-parse HEAD)'" > pysyncobj/revision.py
