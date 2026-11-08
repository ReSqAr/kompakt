#!/usr/bin/env bash
# Emit the crates whose tests a change can affect, as a JSON array.
# A crate's dependants are included; a workspace/tooling change means all.
set -euo pipefail

base="${1:-}"
if [[ -z "$base" || "$base" =~ ^0+$ ]] || ! git rev-parse --verify --quiet "$base" >/dev/null; then
  files=$(git ls-files)
else
  files=$(git diff --name-only "$base"...HEAD)
fi

zslg=0 nvzn=0 btrb=0 cache=0 svg=0 cc=0 all=0
for f in $files; do
  case "$f" in
    Cargo.toml|Cargo.lock|rust-toolchain.toml|deny.toml|.github/*|scripts/*)
      all=1 ;;
    crates/zslg|crates/zslg/*|crates/zslg-*) zslg=1 ;;
    crates/nvzn|crates/nvzn/*|crates/nvzn-*) nvzn=1 ;;
    crates/crossbeam-combinators/*) cc=1 ;;
    # Domain-free workspace libraries (D18): every family builds on them.
    crates/bytesize/*|crates/warn-throttle/*|crates/shutdown/*|crates/framing/*|crates/metrics-kit/*|crates/error-union/*|crates/rocksdb-kit/*|crates/backoff-kit/*|crates/raft-kit/*|crates/pace-kit/*) zslg=1 ;;
    crates/btrb/*) btrb=1 ;;
    crates/objectstorage-cache/*) cache=1 ;;
    crates/svg-charts/*) svg=1 ;;
  esac
done

if [[ "$all" = 1 ]]; then
  zslg=1 nvzn=1 btrb=1 cache=1 svg=1 cc=1
else
  if [[ "$cc" = 1 ]]; then zslg=1; fi
  if [[ "$zslg" = 1 ]]; then nvzn=1; fi
  if [[ "$nvzn" = 1 ]]; then btrb=1; fi
  if [[ "$cache" = 1 ]]; then btrb=1; fi
fi

out=""
# An `if`, not `&&`: a false test would be the function's status, and under
# `set -e` an unchanged crate would end the script.
add() { if [[ "$1" = 1 ]]; then out+="${out:+,}\"$2\""; fi; }
add "$cache" objectstorage-cache
add "$cc" crossbeam-combinators
add "$zslg" zslg
add "$nvzn" nvzn
add "$btrb" btrb
add "$svg" svg-charts
printf 'crates=[%s]\n' "$out"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then printf 'crates=[%s]\n' "$out" >> "$GITHUB_OUTPUT"; fi
