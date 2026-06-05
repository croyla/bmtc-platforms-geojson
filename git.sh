#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
git pull origin main
# ── 2. Create a new git branch ────────────────────────────────────────────────
BRANCH="data/$(date +%Y-%m-%d)"
# Append a counter if the branch already exists
if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
  echo ""
  echo "Using branch $BRANCH"
  git checkout "$BRANCH"
else
  echo ""
  echo "Creating branch: $BRANCH"
  git checkout -b "$BRANCH"
fi

rm -f *.log
# ── 3. Call run.sh ────────────────────────────────────────────────────────────
echo "Running commands"
python3 platforms-geojson.py 20621 20623 20624 21711 banashankari 15 > banashankari.log &
python3 platforms-geojson.py 21042 38815 bapujinagar 15 > bapujinagar.log &
python3 platforms-geojson.py 21923 21924 domlur 15 > domlur.log &
python3 platforms-geojson.py 21544 21545 23374 39129 23691 jayanagar 15 > jayanagar.log &
python3 platforms-geojson.py 20944 20945 36640 20940 20941 20942 20943 21557 22199 22201 36229 31256 kalasipalya 15 > kalasipalya.log &
python3 platforms-geojson.py 20925 20926 kengeri 15 > kengeri.log &
python3 platforms-geojson.py 20686 20687 35932 kuvempunagar 15 > kuvempunagar.log &
python3 platforms-geojson.py 20921 20922 35931 38880 majestic 15 > majestic.log &
python3 platforms-geojson.py 21166 21167 23640 23667 shantinagar 15 > shantinagar.log &
python3 platforms-geojson.py 21172 21173 35779 shivajinagar 15 > shivajinagar.log &
python3 platforms-geojson.py 20707 37850 35768 21479 32213 20704 35944 silkboard 15 > silkboard.log &
python3 platforms-geojson.py 35395 34878 21267 vijayanagar 15 > vijayanagar.log &
python3 platforms-geojson.py 22641 22640 yelahanka 15 > yelahanka.log &
python3 platforms-geojson.py 21288 24016 36187 22835 22706 22836 21289 yeshwanthpur 15 > yeshwanthpur.log &
wait
# ── 4. Commit generated changes ───────────────────────────────────────────────
echo ""
echo "Committing generated output..."

git add --all

if git diff --cached --quiet; then
  echo "No changes detected after generator run — nothing to commit."
else
  COMMIT_MSG="regenerate data files ($(date +%Y-%m-%d))"
  git commit -m "$COMMIT_MSG"
  echo ""
  echo "Committed on branch '$BRANCH': $COMMIT_MSG"
fi
# ── 5. Push changes and open merge request ────────────────────────────────────
echo "Pushing changes to remote..."
git push origin "$BRANCH"
REPO=$(git remote get-url origin | sed 's|.*github.com[:/]\(.*\)\.git|\1|;s|.*github.com[:/]\(.*\)|\1|')
gh pr create --base new_schema --head "$BRANCH" --title "Automated data files update ($BRANCH)" --body "Automated PR to merge regenerated data files from branch \`$BRANCH\` into main.." --repo "$REPO"
# ── 6. Change working tree back to main branch, delete $BRANCH locally ─────────
git checkout new_schema
git branch -D "$BRANCH"


