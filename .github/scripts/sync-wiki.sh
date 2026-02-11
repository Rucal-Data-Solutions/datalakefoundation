#!/usr/bin/env bash
set -euo pipefail

REPO="Rucal-Data-Solutions/datalakefoundation"
GITHUB_BASE="https://github.com/${REPO}/blob/main"
WIKI_DIR="$(mktemp -d)"

trap 'rm -rf "$WIKI_DIR"' EXIT

# ── Clone wiki ──────────────────────────────────────────────────────
echo "Cloning wiki..."
git clone "https://x-access-token:${GITHUB_TOKEN}@github.com/${REPO}.wiki.git" "$WIKI_DIR"

# ── Page mapping (source → wiki page) ──────────────────────────────
declare -A PAGE_MAP=(
  ["README.md"]="Home.md"
  ["RELEASE_NOTES.md"]="Release-Notes.md"
  ["LOG_LEVEL_CONFIG.md"]="Log-Level-Configuration.md"
  ["docs/PATTERNS_AND_PRACTICES.md"]="Patterns-and-Practices.md"
  ["docs/configuration/ENTITY_CONFIGURATION.md"]="Entity-Configuration.md"
  ["docs/configuration/METADATA_SOURCES.md"]="Metadata-Sources.md"
  ["docs/processing/PROCESSING_STRATEGIES.md"]="Processing-Strategies.md"
  ["docs/processing/WATERMARKS.md"]="Watermarks.md"
  ["docs/processing/DELETE_INFERENCE.md"]="Delete-Inference.md"
  ["docs/outputs/IO_OUTPUT_MODES.md"]="IO-Output-Modes.md"
)

# ── Clean synced pages ──────────────────────────────────────────────
echo "Cleaning wiki pages..."
for wiki_page in "${PAGE_MAP[@]}" _Sidebar.md _Footer.md; do
  rm -f "${WIKI_DIR}/${wiki_page}"
done

# ── Copy and rename ─────────────────────────────────────────────────
echo "Copying docs..."
for src in "${!PAGE_MAP[@]}"; do
  cp "${src}" "${WIKI_DIR}/${PAGE_MAP[$src]}"
done

# ── Link rewrites ──────────────────────────────────────────────────
echo "Rewriting links..."

# Map known .md filenames to their wiki page name (without .md extension)
declare -A LINK_MAP=(
  ["README.md"]="Home"
  ["RELEASE_NOTES.md"]="Release-Notes"
  ["LOG_LEVEL_CONFIG.md"]="Log-Level-Configuration"
  ["PATTERNS_AND_PRACTICES.md"]="Patterns-and-Practices"
  ["ENTITY_CONFIGURATION.md"]="Entity-Configuration"
  ["METADATA_SOURCES.md"]="Metadata-Sources"
  ["PROCESSING_STRATEGIES.md"]="Processing-Strategies"
  ["WATERMARKS.md"]="Watermarks"
  ["DELETE_INFERENCE.md"]="Delete-Inference"
  ["IO_OUTPUT_MODES.md"]="IO-Output-Modes"
)

for wiki_file in "${WIKI_DIR}"/*.md; do
  # 1. Rewrite inter-doc links: ](any/path/KNOWN_FILE.md) → ](Wiki-Page)
  for filename in "${!LINK_MAP[@]}"; do
    wiki_name="${LINK_MAP[$filename]}"
    escaped=$(echo "$filename" | sed 's/\./\\./g')
    sed -i "s|]([^)]*${escaped})|](${wiki_name})|g" "$wiki_file"
  done

  # 2. Rewrite source-code links: ](../src/...) → ](https://github.com/.../blob/main/src/...)
  sed -i "s|](\.\./src/|](${GITHUB_BASE}/src/|g" "$wiki_file"
  sed -i "s|](src/|](${GITHUB_BASE}/src/|g" "$wiki_file"
done

# ── Generate _Sidebar.md ───────────────────────────────────────────
echo "Generating sidebar..."
cat > "${WIKI_DIR}/_Sidebar.md" << 'EOF'
**Datalake Foundation**

- [[Home]]
- [[Release Notes]]

**Configuration**

- [[Entity Configuration]]
- [[Metadata Sources]]
- [[Log Level Configuration]]

**Processing**

- [[Processing Strategies]]
- [[Watermarks]]
- [[Delete Inference]]

**Outputs**

- [[IO Output Modes]]

**Reference**

- [[Patterns and Practices]]
EOF

# ── Generate _Footer.md ────────────────────────────────────────────
echo "Generating footer..."
cat > "${WIKI_DIR}/_Footer.md" << EOF
---
This wiki is auto-generated from [\`docs/\`](https://github.com/${REPO}/tree/main/docs). To edit, update the source files and push to \`main\`.
EOF

# ── Commit and push ─────────────────────────────────────────────────
cd "$WIKI_DIR"
git add -A

if git diff --cached --quiet; then
  echo "No changes to commit."
else
  git config user.name "github-actions[bot]"
  git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
  git commit -m "Sync wiki from docs/"
  git push
  echo "Wiki updated."
fi
