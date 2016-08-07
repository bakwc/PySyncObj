#!/usr/bin/env bash

cp hooks/post-commit-launcher .git/hooks/post-commit
chmod +x .git/hooks/post-commit
cp hooks/pre-commit-launcher .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
