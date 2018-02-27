#!/bin/sh

set -e

cat src/lib.rs | grep '^//!' | sed -E 's#^... ?##'
