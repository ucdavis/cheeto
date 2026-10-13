#!/bin/bash
# SessionStart hook: give a linked git worktree its own clone of hpccf-admin-docs.
#
# .worktreeinclude cannot copy the clone: git does not list the files inside a
# nested repository, so Claude Code skips it. This hook clones it from the main
# checkout's copy (local and fast), then points origin back at GitHub.
#
# Does nothing in the main checkout, when the worktree already has the clone,
# or when the main checkout has no clone.

set -euo pipefail

root=${CLAUDE_PROJECT_DIR:-$PWD}
git_dir=$(git -C "$root" rev-parse --absolute-git-dir 2>/dev/null) || exit 0
common_dir=$(git -C "$root" rev-parse --path-format=absolute --git-common-dir)
[[ "$git_dir" == "$common_dir" ]] && exit 0  # main checkout

dest="$root/hpccf-admin-docs"
src="$(dirname "$common_dir")/hpccf-admin-docs"
[[ -e "$dest" ]] && exit 0
[[ -d "$src/.git" ]] || exit 0

url=$(git -C "$src" remote get-url origin)
git clone --quiet "$src" "$dest" >&2
git -C "$dest" remote set-url origin "$url"

# SessionStart stdout becomes session context.
echo "Cloned hpccf-admin-docs into this worktree from $src (branch $(git -C "$dest" branch --show-current))."
