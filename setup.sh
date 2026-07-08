#!/bin/bash
# LocalAI TV MCP Server — one-time setup
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
echo "📦 Setting up LocalAI TV MCP Server..."

# Create virtualenv
/opt/homebrew/bin/python3.11 -m venv "$DIR/.venv"
"$DIR/.venv/bin/pip" install --quiet --upgrade pip
"$DIR/.venv/bin/pip" install --quiet -r "$DIR/requirements.txt"
echo "✅ Dependencies installed"

# Pull DATABASE_URL from AWS SSM
echo "🔐 Fetching DATABASE_URL from SSM..."
DB_URL=$(aws ssm get-parameter \
  --name "/localaitv/stg/DATABASE_URL" \
  --with-decryption \
  --region ap-south-1 \
  --query "Parameter.Value" \
  --output text 2>/dev/null || echo "")

SQS_URL=$(aws ssm get-parameter \
  --name "/localaitv/stg/BUILD_QUEUE_URL" \
  --with-decryption \
  --region ap-south-1 \
  --query "Parameter.Value" \
  --output text 2>/dev/null || echo "https://sqs.ap-south-1.amazonaws.com/689186650531/laitv-stg-build")

# Write .env
cat > "$DIR/.env" <<EOF
DATABASE_URL=${DB_URL}
AWS_REGION_COMP=ap-south-1
AWS_REGION_S3=ap-south-2
S3_BUCKET_MAIN=news-689186650531-ap-south-2-an
S3_BUCKET_M=localaitv1-689186650531-ap-south-2-an
ECS_CLUSTER=laitv-stg-app-cluster
SQS_BUILD_URL=${SQS_URL}
LOCALAITV_API_URL=https://localaitv.com
EOF

if [ -z "$DB_URL" ]; then
  echo "⚠️  DATABASE_URL not found in SSM — edit .env manually"
else
  echo "✅ .env written with DATABASE_URL from SSM"
fi

# Write Claude Desktop config snippet
CONFIG_DIR="$HOME/Library/Application Support/Claude"
mkdir -p "$CONFIG_DIR"
CONFIG_FILE="$CONFIG_DIR/claude_desktop_config.json"

MCP_ENTRY=$(cat <<JSONEOF
{
  "command": "$DIR/.venv/bin/python",
  "args": ["$DIR/server.py"],
  "env": {
    "DATABASE_URL": "${DB_URL}",
    "AWS_REGION_COMP": "ap-south-1",
    "AWS_REGION_S3": "ap-south-2",
    "S3_BUCKET_MAIN": "news-689186650531-ap-south-2-an",
    "S3_BUCKET_M": "localaitv1-689186650531-ap-south-2-an",
    "ECS_CLUSTER": "laitv-stg-app-cluster",
    "SQS_BUILD_URL": "${SQS_URL}",
    "LOCALAITV_API_URL": "https://localaitv.com"
  }
}
JSONEOF
)

if [ -f "$CONFIG_FILE" ]; then
  echo ""
  echo "📝 Add this to $CONFIG_FILE under 'mcpServers':"
  echo ""
  echo '  "localaitv-admin": '"$MCP_ENTRY"
  echo ""
  echo "(Your existing claude_desktop_config.json was not modified)"
else
  cat > "$CONFIG_FILE" <<EOF
{
  "mcpServers": {
    "localaitv-admin": $MCP_ENTRY
  }
}
EOF
  echo "✅ Claude Desktop config written to $CONFIG_FILE"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "  1. If DATABASE_URL was empty above, edit .env and paste the DB password"
echo "  2. Restart Claude Desktop app"
echo "  3. In Claude, type: 'check pipeline health' and it will use this MCP"
