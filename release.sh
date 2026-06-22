#!/usr/bin/env zsh

set -u

bump_rules=(patch minor major prepatch preminor premajor prerelease)

if (( $# < 1 )); then
    print -u2 "usage: $0 vX.Y.Z | (${(j: | :)bump_rules})"
    exit 1
fi

arg=$1
# Accept either an explicit vX.Y.Z (the tag we publish) or a poetry bump rule.
if [[ $arg =~ '^v[0-9]+\.[0-9]+\.[0-9]+$' ]]; then
    bump=${arg#v}                       # poetry wants the bare X.Y.Z, no 'v'
elif (( ${bump_rules[(Ie)$arg]} )); then
    bump=$arg
else
    print -u2 "error: argument must be vX.Y.Z (e.g. v2.0.1) or a bump rule (${bump_rules}); got '${arg}'"
    exit 1
fi

old=$(poetry version -s)
poetry version "$bump"
new=$(poetry version -s)
print "cheeto: bumped ${old} -> ${new}; committing, tagging v${new}, and pushing"

git add pyproject.toml cheeto/__init__.py
git commit -m v$new
git tag v$new
git push --atomic origin $(git rev-parse --abbrev-ref HEAD) v$new
