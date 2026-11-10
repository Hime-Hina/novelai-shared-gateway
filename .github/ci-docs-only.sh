#!/usr/bin/env bash
set -euo pipefail

changes=$(mktemp)
trap 'rm -f -- "$changes"' EXIT
git diff --name-only --no-renames -z "$1" HEAD -- > "$changes" || exit 0
# A rename appears as a deletion and an addition, so both paths are checked.
while IFS= read -r -d '' path; do
  case "$path" in
    # The executor packages scripts/ recursively and the build uses the named files.
    scripts/*|NOTICE.md|web/THIRD_PARTY_NOTICES.md) exit 0 ;;
    docs/operations/test-deployment.md|docs/operations/production-release.md) exit 0 ;;
    docs/*|*.md) ;;
    *) exit 0 ;;
  esac
done < "$changes"
echo 'docs_only=true'
