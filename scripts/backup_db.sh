#!/usr/bin/env bash
# backup_db.sh — PostgreSQL backup and restore for domain_leads.
#
# Usage:
#   ./scripts/backup_db.sh backup              # Create timestamped backup
#   ./scripts/backup_db.sh restore <file.sql>   # Restore from backup file
#   ./scripts/backup_db.sh list                 # List available backups
#
# Backups are stored in ./backups/ as gzipped SQL dumps.
# Keeps the last 10 backups by default (configurable via KEEP_BACKUPS).
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKUP_DIR="${BACKUP_DIR:-$PROJECT_DIR/backups}"
KEEP_BACKUPS="${KEEP_BACKUPS:-10}"

# Database connection — reads from .env or uses defaults
if [ -f "$PROJECT_DIR/.env" ]; then
    # shellcheck disable=SC1091
    source <(grep -E '^(DATABASE_URL|PGHOST|PGPORT|PGUSER|PGDATABASE)=' "$PROJECT_DIR/.env" | sed 's/^/export /')
fi

# Parse DATABASE_URL if set (postgresql://user:pass@host:port/dbname)
if [ -n "${DATABASE_URL:-}" ]; then
    PGUSER="${PGUSER:-$(echo "$DATABASE_URL" | sed -n 's|.*://\([^:@]*\).*|\1|p')}"
    PGHOST="${PGHOST:-$(echo "$DATABASE_URL" | sed -n 's|.*@\([^:/]*\).*|\1|p')}"
    PGPORT="${PGPORT:-$(echo "$DATABASE_URL" | sed -n 's|.*:\([0-9]*\)/.*|\1|p')}"
    PGDATABASE="${PGDATABASE:-$(echo "$DATABASE_URL" | sed -n 's|.*/\([^?]*\).*|\1|p')}"
fi

DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"
DB_USER="${PGUSER:-postgres}"
DB_NAME="${PGDATABASE:-domain_leads}"

# Check if pg_dump is accessible (try docker compose first, then local)
use_docker=false
if docker compose exec -T postgres pg_dump --version >/dev/null 2>&1; then
    use_docker=true
elif ! command -v pg_dump >/dev/null 2>&1; then
    echo "ERROR: pg_dump not found locally and Docker PostgreSQL container not running."
    exit 1
fi

run_pg() {
    local cmd="$1"
    shift
    if [ "$use_docker" = true ]; then
        docker compose exec -T postgres "$cmd" "$@"
    else
        "$cmd" "$@"
    fi
}

do_backup() {
    mkdir -p "$BACKUP_DIR"
    local timestamp
    timestamp="$(date +%Y%m%d_%H%M%S)"
    local filename="domain_leads_${timestamp}.sql.gz"
    local filepath="$BACKUP_DIR/$filename"

    echo "[$(date)] Starting backup of $DB_NAME..."

    if [ "$use_docker" = true ]; then
        docker compose exec -T postgres pg_dump \
            -U "$DB_USER" \
            -d "$DB_NAME" \
            --no-owner \
            --no-privileges \
            --format=plain \
            | gzip > "$filepath"
    else
        pg_dump \
            -h "$DB_HOST" \
            -p "$DB_PORT" \
            -U "$DB_USER" \
            -d "$DB_NAME" \
            --no-owner \
            --no-privileges \
            --format=plain \
            | gzip > "$filepath"
    fi

    local size
    size="$(du -h "$filepath" | cut -f1)"
    echo "[$(date)] Backup created: $filepath ($size)"

    # Prune old backups
    local count
    count=$(find "$BACKUP_DIR" -name "domain_leads_*.sql.gz" -type f | wc -l | tr -d ' ')
    if [ "$count" -gt "$KEEP_BACKUPS" ]; then
        local to_remove=$((count - KEEP_BACKUPS))
        echo "[$(date)] Pruning $to_remove old backup(s)..."
        find "$BACKUP_DIR" -name "domain_leads_*.sql.gz" -type f \
            | sort \
            | head -n "$to_remove" \
            | xargs rm -f
    fi

    echo "[$(date)] Backup complete. $count backup(s) on disk."
}

do_restore() {
    local filepath="$1"
    if [ ! -f "$filepath" ]; then
        echo "ERROR: Backup file not found: $filepath"
        exit 1
    fi

    echo ""
    echo "WARNING: This will DROP and recreate the $DB_NAME database."
    echo "File: $filepath"
    echo ""
    read -r -p "Type 'yes' to confirm: " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Restore cancelled."
        exit 0
    fi

    echo "[$(date)] Restoring $DB_NAME from $filepath..."

    if [ "$use_docker" = true ]; then
        # Drop and recreate database
        docker compose exec -T postgres psql -U "$DB_USER" -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME;"
        docker compose exec -T postgres psql -U "$DB_USER" -d postgres -c "CREATE DATABASE $DB_NAME;"
        docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS citext;"

        # Restore
        gunzip -c "$filepath" | docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" --quiet
    else
        dropdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" --if-exists "$DB_NAME"
        createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$DB_NAME"
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS citext;"

        gunzip -c "$filepath" | psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" --quiet
    fi

    echo "[$(date)] Restore complete."
}

do_list() {
    if [ ! -d "$BACKUP_DIR" ] || [ -z "$(ls -A "$BACKUP_DIR" 2>/dev/null)" ]; then
        echo "No backups found in $BACKUP_DIR"
        return
    fi

    echo "Available backups in $BACKUP_DIR:"
    echo ""
    find "$BACKUP_DIR" -name "domain_leads_*.sql.gz" -type f \
        | sort -r \
        | while read -r f; do
            local size
            size="$(du -h "$f" | cut -f1)"
            echo "  $(basename "$f")  ($size)"
        done
}

# Main
case "${1:-}" in
    backup)
        cd "$PROJECT_DIR"
        do_backup
        ;;
    restore)
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 restore <backup_file.sql.gz>"
            exit 1
        fi
        cd "$PROJECT_DIR"
        do_restore "$2"
        ;;
    list)
        do_list
        ;;
    *)
        echo "Usage: $0 {backup|restore <file>|list}"
        echo ""
        echo "  backup              Create a timestamped database backup"
        echo "  restore <file>      Restore database from a backup file"
        echo "  list                List available backups"
        exit 1
        ;;
esac
