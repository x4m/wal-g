#!/usr/bin/env bash
set -Eeuo pipefail

# Run against an already started disposable RW source. Unlike run_e2e.sh this
# test never uploads WAL itself: archive_command must use this repository.
: "${WALG_BIN:?}" "${POLAR_E2E_HOOKS:?}" "${POLAR_SOURCE_PGDATA:?}"
: "${POLAR_SOURCE_PFS_DATA_PATH:?}" "${WALG_FILE_PREFIX:?dedicated empty FS repository}"
: "${POLAR_E2E_RESULTS_DIR:?}" "${POLAR_E2E_WORK_DIR:?}"
command -v jq >/dev/null
command -v timeout >/dev/null
for required in preflight digest archive_done reset_restore restore_pfs_data_path prepare_restore start_restore stop_restore; do
  [[ -x "$POLAR_E2E_HOOKS/$required" ]] || { echo "missing hook: $required" >&2; exit 2; }
done
mkdir -p "$POLAR_E2E_RESULTS_DIR" "$POLAR_E2E_WORK_DIR" "$WALG_FILE_PREFIX"
export WALG_DELTA_MAX_STEPS=0
export WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE=true
backup_timeout=${POLAR_E2E_BACKUP_TIMEOUT:-300}
archive_timeout=${POLAR_E2E_ARCHIVE_TIMEOUT:-300}
writer_stop="$POLAR_E2E_RESULTS_DIR/writer.stop"
[[ ! -e $writer_stop ]] || { echo 'use a fresh results directory' >&2; exit 2; }
hook() { "$POLAR_E2E_HOOKS/$1" "${@:2}"; }
cleanup() {
  touch "$writer_stop"
  if [[ -n ${writer:-} ]]; then wait "$writer" || true; fi
  hook stop_restore archive_1 || true
  hook stop_restore archive_2 || true
}
trap cleanup EXIT
sql() { hook digest source "$1"; }
backup() {
  WALG_POLARDB_PFS_DATA_PATH="$POLAR_SOURCE_PFS_DATA_PATH" \
    timeout -k 10 "$backup_timeout" "$WALG_BIN" backup-push "$POLAR_SOURCE_PGDATA"
}
digest_sql="SELECT count(*) || ':' || md5(string_agg(payload, ',' ORDER BY i)) FROM walg_archive_e2e"
hook preflight
[[ $("$WALG_BIN" backup-list --json | jq length) == 0 ]]
sql 'CREATE TABLE walg_archive_e2e(i int PRIMARY KEY, payload text NOT NULL); INSERT INTO walg_archive_e2e SELECT i, md5(i::text) FROM generate_series(1,120000) i' >/dev/null
sql "BEGIN; INSERT INTO walg_archive_e2e VALUES (120001, 'rolled back'); ROLLBACK" >/dev/null

names=()
digests=()
for round in 1 2; do
  if [[ $round == 2 ]]; then
    # A committed writer remains active while the hot backup is running.
    (
      while [[ ! -e $writer_stop ]]; do
        sql "UPDATE walg_archive_e2e SET payload=md5(payload) WHERE i<=1000" >/dev/null
        date -u +%FT%TZ >>"$POLAR_E2E_RESULTS_DIR/writer-commits.txt"
        sleep 1
      done
    ) &
    writer=$!
  fi
  backup >"$POLAR_E2E_RESULTS_DIR/backup-$round.log" 2>&1
  if [[ $round == 2 ]]; then
    touch "$writer_stop"
    wait "$writer"
    writer=
    [[ -s "$POLAR_E2E_RESULTS_DIR/writer-commits.txt" ]]
  fi
  "$WALG_BIN" backup-list --json >"$POLAR_E2E_RESULTS_DIR/backups-$round.json"
  [[ $(jq length "$POLAR_E2E_RESULTS_DIR/backups-$round.json") == "$round" ]]
  name=$(jq -r '.[].backup_name' "$POLAR_E2E_RESULTS_DIR/backups-$round.json" | sort)
  if [[ $round == 2 ]]; then name=$(printf '%s\n' "$name" | grep -vx "${names[0]}"); fi
  [[ $name == base_* && $name != *$'\n'* ]]
  names+=("$name")
  # This change is after backup_stop: the target state necessarily needs WAL.
  sql "UPDATE walg_archive_e2e SET payload='after-backup-$round' WHERE i=120000" >/dev/null
  digests+=("$(sql "$digest_sql")")
  printf '%s\n' "${digests[round-1]}" >"$POLAR_E2E_RESULTS_DIR/expected-$round.txt"
  sql "SELECT pg_create_restore_point('walg_archive_round_$round')" >/dev/null
  wal=$(sql 'SELECT pg_walfile_name(pg_current_wal_lsn())')
  sql 'SELECT pg_switch_wal()' >/dev/null
  archived=false
  for ((i=0; i<archive_timeout; i++)); do
    if [[ $(hook archive_done source "$wal") == t ]]; then
      archived=true
      break
    fi
    sleep 1
  done
  [[ $archived == true ]] || { echo "archive_command did not produce $wal.done" >&2; exit 1; }
  sql 'SELECT archived_count, failed_count, last_archived_wal FROM pg_stat_archiver' >"$POLAR_E2E_RESULTS_DIR/archiver-$round.txt"
  if [[ $round == 1 ]]; then
    # Include tar parts, metadata and sentinel: a second backup must not alter any.
    find "$WALG_FILE_PREFIX/basebackups_005" -type f -path "*${names[0]}*" -exec sha256sum '{}' + | sort >"$POLAR_E2E_RESULTS_DIR/first.sha256"
    [[ -s "$POLAR_E2E_RESULTS_DIR/first.sha256" ]]
  else
    sha256sum -c "$POLAR_E2E_RESULTS_DIR/first.sha256" >"$POLAR_E2E_RESULTS_DIR/first-unchanged.txt"
  fi
done

# A competing session must fail before it publishes any backup objects.
sql "SELECT pg_advisory_lock(hashtext('pg_backup')); SELECT pg_sleep(10)" >"$POLAR_E2E_RESULTS_DIR/lock.log" &
locker=$!
locked=false
for i in {1..50}; do
  if [[ $(sql "SELECT EXISTS (SELECT FROM pg_locks WHERE locktype='advisory' AND objid=hashtext('pg_backup')::oid AND granted)") == t ]]; then locked=true; break; fi
  sleep .1
done
[[ $locked == true ]]
if backup >"$POLAR_E2E_RESULTS_DIR/concurrent.log" 2>&1; then
  echo 'competing backup unexpectedly succeeded' >&2
  exit 1
fi
grep -q 'cannot acquire PolarDB backup lock' "$POLAR_E2E_RESULTS_DIR/concurrent.log"
wait "$locker"
[[ $("$WALG_BIN" backup-list --json | jq length) == 2 ]]

for round in 1 2; do
  export POLAR_E2E_TARGET_NAME="walg_archive_round_$round"
  mode="archive_$round"
  restore_dir="$POLAR_E2E_WORK_DIR/$mode"
  mkdir "$restore_dir"
  hook reset_restore "$mode" "$restore_dir"
  root=$(hook restore_pfs_data_path "$mode")
  [[ $root == /*/* && $root != "$POLAR_SOURCE_PFS_DATA_PATH" ]]
  WALG_POLARDB_PFS_DATA_PATH="$root" "$WALG_BIN" backup-fetch "$restore_dir" "${names[round-1]}" >"$POLAR_E2E_RESULTS_DIR/fetch-$round.log" 2>&1
  # prepare_restore MUST configure recovery_target_name and target_action=promote.
  hook prepare_restore "$mode" "$restore_dir"
  hook start_restore "$mode"
  actual=$(hook digest "$mode" "$digest_sql")
  [[ $actual == "${digests[round-1]}" ]]
  [[ $(hook digest "$mode" 'SELECT count(*) FROM walg_archive_e2e WHERE i=120001') == 0 ]]
  [[ $(hook digest "$mode" 'SELECT NOT pg_is_in_recovery()') == t ]]
  printf '%s\n' "$actual" >"$POLAR_E2E_RESULTS_DIR/restored-$round.txt"
  hook stop_restore "$mode"
done
echo 'Automatic archive, two independent backups, unchanged first backup, concurrent rejection and PITR passed'
