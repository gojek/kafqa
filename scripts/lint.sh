#!/bin/sh

check_imports() {
  change=$(goimports -l . | wc -l ) 
  if [ $change -ne "0" ]; then 
      echo "Imports Violations found in $change files, use make fix_imports"
      exit 1
  else
      echo "No imports violation found"
  fi
}

$*
