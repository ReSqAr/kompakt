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

zslg=0 nvzn=0 btrb=0 cache=0 all=0
for f in $files; do
  case "$f" in
    Cargo.toml|Cargo.lock|rust-toolchain.toml|deny.toml|.github/*|scripts/*)
      all=1 ;;
    crates/zslg/*) zslg=1 ;;
    crates/nvzn/*) nvzn=1 ;;
    crates/btrb/*) btrb=1 ;;
    crates/objectstorage-cache/*) cache=1 ;;
  esac
done

if [[ "$all" = 1 ]]; then
  zslg=1 nvzn=1 btrb=1 cache=1
else
  if [[ "$zslg" = 1 ]]; then nvzn=1; fi
  if [[ "$nvzn" = 1 ]]; then btrb=1; fi
  if [[ "$cache" = 1 ]]; then btrb=1; fi
fi

out=""
add() { [[ "$1" = 1 ]] && out+="${out:+,}\"$2\""; }
add "$cache" objectstorage-cache
add "$zslg" zslg
add "$nvzn" nvzn
add "$btrb" btrb
printf 'crates=[%s]\n' "$out"
[[ -n "${GITHUB_OUTPUT:-}" ]] && printf 'crates=[%s]\n' "$out" >> "$GITHUB_OUTPUT"
