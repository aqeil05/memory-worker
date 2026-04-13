#!/usr/bin/env bash
# merge.sh — consolidate fragmented KV keys and seed the active project list
#
# Usage:
#   INTERNAL_SECRET=xxx ./merge.sh
#
# What it does:
#   1. Merges all fragment keys into their canonical names (also sets alias so
#      future stray emails are auto-routed).
#   2. Seeds mem:projects with the canonical project list so new emails are
#      matched against it instead of inventing freeform names.
#
# Safe to re-run — merges are idempotent (deduped on emailDate|fact).

set -euo pipefail

BASE_URL="https://daya-memory-worker.daya-timesheet.workers.dev"
SECRET="${INTERNAL_SECRET:?Set INTERNAL_SECRET env var before running}"

# ── helpers ───────────────────────────────────────────────────────────────────

merge() {
  local from="$1" into="$2"
  printf "  %-58s → %s\n" "\"$from\"" "\"$into\""
  local response http_code body message
  response=$(curl -s -w "\n%{http_code}" \
    -H "Authorization: Bearer $SECRET" \
    --get \
    --data-urlencode "from=$from" \
    --data-urlencode "into=$into" \
    "$BASE_URL/merge-company")
  http_code=$(printf '%s' "$response" | tail -1)
  body=$(printf '%s' "$response" | head -1)
  if [ "$http_code" != "200" ]; then
    printf "    ⚠️  HTTP %s: %s\n" "$http_code" "$body"
  else
    message=$(printf '%s' "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message','ok'))" 2>/dev/null || printf '%s' "$body")
    printf "    ✓ %s\n" "$message"
  fi
}

add_project() {
  local name="$1"
  printf "  Adding: %s\n" "$name"
  local response http_code body
  response=$(curl -s -w "\n%{http_code}" \
    -H "Authorization: Bearer $SECRET" \
    --get \
    --data-urlencode "name=$name" \
    "$BASE_URL/projects/add")
  http_code=$(printf '%s' "$response" | tail -1)
  body=$(printf '%s' "$response" | head -1)
  if [ "$http_code" != "200" ]; then
    printf "    ⚠️  HTTP %s: %s\n" "$http_code" "$body"
  else
    printf "    ✓ done\n"
  fi
}

# ── Step 1: merge fragments ───────────────────────────────────────────────────

echo ""
echo "=== Step 1: Merging fragmented keys ==="
echo ""

echo "[ anabion ]"
merge "anabion 10th floor y tower"                              "anabion"
merge "anabion office"                                          "anabion"
merge "anabion office fitout"                                   "anabion"
merge "anabion office y tower"                                  "anabion"
merge "anabion project"                                         "anabion"
merge "anabion y tower"                                         "anabion"
merge "anabion y tower fitout"                                  "anabion"
merge "anabion y tower office 1002"                             "anabion"
merge "anabion y tower office fitout"                           "anabion"
merge "fitout works for anabion office"                         "anabion"
echo ""

echo "[ daya ]"
merge "daya company"                                            "daya"
merge "daya fitout & carpentry"                                 "daya"
merge "daya interior"                                           "daya"
merge "daya interior design"                                    "daya"
echo ""

echo "[ hikvision ]"
merge "hikvision middle east & africa"                          "hikvision"
merge "hikvision msheireb"                                      "hikvision"
merge "hikvision project"                                       "hikvision"
echo ""

echo "[ huawei ]"
merge "huawei qatar"                                            "huawei"
merge "huawei qatar office modification"                        "huawei"
echo ""

echo "[ malomatia 19th floor ]"
merge "malomatia office - 19th floor"                           "malomatia 19th floor"
merge "malomatia office 19th floor"                             "malomatia 19th floor"
merge "malomatia office 19th floor @ manarat tower"             "malomatia 19th floor"
merge "malomatia office 19th floor level manarat tower"         "malomatia 19th floor"
merge "malomatia office at 19th floor"                          "malomatia 19th floor"
merge "malomatia office, 19th floor manaret tower"              "malomatia 19th floor"
merge "malomatia office, 19th floor, manarat tower, qatar"      "malomatia 19th floor"
merge "meeting room 19th floor"                                 "malomatia 19th floor"
echo ""

echo "[ mcit ]"
merge "mcit 14th floor"                                         "mcit"
merge "mcit 19 floor"                                           "mcit"
merge "mcit 19th floor"                                         "mcit"
merge "mcit operations centre"                                   "mcit"
merge "mcit water leakage 16th 15th 14th floors"                "mcit"
merge "mcit operation center"                                   "mcit"
merge "mcit operation center at al brooq tower"                 "mcit"
merge "mcit operation centre 14th floor al borooq tower"        "mcit"
merge "mcit operations centre 14th floor brooq tower"           "mcit"
echo ""

echo "[ singapore embassy ]"
merge "singapore embassy fit-out work"                          "singapore embassy"
merge "singapore embassy meeting room"                          "singapore embassy"
echo ""

echo "[ schneider msherieb ]"
merge "schneider electric office at msheireb"                   "schneider msherieb"
merge "schneider electric q03 building"                         "schneider msherieb"
merge "schneider electric services"                             "schneider msherieb"
merge "schneider electric services llc"                         "schneider msherieb"
merge "schneider electric services msherieb downtown q03 building level 4" "schneider msherieb"
merge "schneider electric services msherieb level 4"            "schneider msherieb"
echo ""

# ── nabina ───────────────────────────────────────────────────────────────────
echo "[ nabina ]"
merge "nabina ceramic"                                          "nabina"
merge "nabina holding"                                          "nabina"
merge "nabina interiors"                                        "nabina"
merge "nabina interiors co"                                     "nabina"
merge "nabina interiors co / malomatia"                         "nabina"
merge "nabina interiors malomatia"                              "nabina"
merge "nabina interiors office"                                 "nabina"
echo ""

# ── villaggio starlink ────────────────────────────────────────────────────────
echo "[ villaggio starlink ]"
merge "villaggio kiosk"                                         "villaggio starlink"
merge "villaggio kiosk - mall shop"                             "villaggio starlink"
merge "villaggio kiosk - new fitout"                            "villaggio starlink"
merge "villaggio kiosk starlink retail stand"                   "villaggio starlink"
echo ""

# ── qstp kiosk ───────────────────────────────────────────────────────────────
echo "[ qstp kiosk ]"
merge "qstp booth re installation"                              "qstp kiosk"
merge "qstp booth re-installation"                              "qstp kiosk"
merge "qstp booth re-installation (web summit tree stand)"      "qstp kiosk"
merge "qstp booth re-installation rayyan"                       "qstp kiosk"
merge "qstp booth re-installation tech2"                        "qstp kiosk"
merge "qstp kiosk at web summit 2026"                           "qstp kiosk"
merge "qstp stand at qatar web summit"                          "qstp kiosk"
merge "qstp stand for qatar web summit"                         "qstp kiosk"
merge "qstp web summit booth and tree reinstallation"           "qstp kiosk"
merge "qstp web summit booth at qstp rayyan"                    "qstp kiosk"
merge "qstp web summit booth re-installation tech2"             "qstp kiosk"
merge "qstp web summit tree stand at qstp rayyan"               "qstp kiosk"
echo ""

# ── daya workshop ─────────────────────────────────────────────────────────────
echo "[ daya workshop ]"
merge "b3 a05 warehouse"                                        "daya workshop"
merge "b3 series buildings"                                     "daya workshop"
merge "b3-a5-37"                                                "daya workshop"
merge "b3-a5-37,38"                                             "daya workshop"
merge "b3-a5-38"                                                "daya workshop"
merge "b5a538"                                                  "daya workshop"
echo ""

# ── Step 2: seed active project list ─────────────────────────────────────────

echo "=== Step 2: Seeding active project list ==="
echo ""

add_project "nabina"
add_project "anabion"
add_project "daya workshop"
add_project "villaggio starlink"
add_project "qstp kiosk"
add_project "daya"
add_project "hikvision"
add_project "huawei"
add_project "malomatia 19th floor"
add_project "mcit"
add_project "singapore embassy"
add_project "schneider msherieb"

echo ""
echo "=== All done ==="
echo ""
echo "Verify with:"
echo "  curl '$BASE_URL/companies' | python3 -m json.tool"
echo "  curl '$BASE_URL/projects'  | python3 -m json.tool"
