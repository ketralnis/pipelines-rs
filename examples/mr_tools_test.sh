#!/bin/sh

set -e

cargo build --example=mr_tools --release

njobs=8

RUN="cargo run --release --example=mr_tools --"

$RUN \
    -M${njobs} -R${njobs} -S200m \
    -m 'sed -e "s/[a-zA-Z-]/X/g"' \
    -r "uniq -c" \
    < /usr/share/dict/words \
    | awk '{print $2 " " $1}' \
    | sort -n

# TODO:
# lot of unwrap/expects, some kosher some not
# way to pass args to sort: --compress-program, -T
# specify field separator -F
# better sort -S default

