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
rounds=2
if [[ ${POLAR_E2E_DELTA:-false} == true ]]; then
  export WALG_DELTA_MAX_STEPS=5 WALG_DELTA_ORIGIN=LATEST
  export WALG_USE_WAL_DELTA=false WALG_FORCE_WAL_DELTA=false
  rounds=3
fi
export WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE=true
backup_timeout=${POLAR_E2E_BACKUP_TIMEOUT:-300}
archive_timeout=${POLAR_E2E_ARCHIVE_TIMEOUT:-300}
recovery_timeout=${POLAR_E2E_RECOVERY_TIMEOUT:-300}
writer_stop="$POLAR_E2E_RESULTS_DIR/writer.stop"
[[ ! -e $writer_stop ]] || { echo 'use a fresh results directory' >&2; exit 2; }
hook() { "$POLAR_E2E_HOOKS/$1" "${@:2}"; }
cleanup() {
  touch "$writer_stop"
  if [[ -n ${fetcher:-} ]]; then
    kill "$fetcher" 2>/dev/null || true
    wait "$fetcher" || true
  fi
  if [[ -n ${writer:-} ]]; then wait "$writer" || true; fi
  hook stop_restore archive_1 || true
  hook stop_restore archive_2 || true
  if [[ $rounds == 3 ]]; then hook stop_restore archive_3 || true; fi
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
if [[ $rounds == 3 ]]; then
  [[ $(sql 'SHOW full_page_writes') == on ]]
  sql 'CREATE TABLE walg_delta_tail AS SELECT i, md5(i::text) AS payload FROM generate_series(1,60000) i; CREATE TABLE walg_delta_recreated AS SELECT 1 AS n; CREATE TABLE walg_delta_removed AS SELECT 1 AS n' >/dev/null
  digest_sql="SELECT ($digest_sql) || ':' || (SELECT count(*) || ':' || md5(string_agg(payload, ',' ORDER BY i)) FROM walg_delta_tail) || ':' || (SELECT sum(n) FROM walg_delta_recreated) || ':' || (to_regclass('walg_delta_removed') IS NULL)"
fi

names=()
digests=()
for ((round=1; round<=rounds; round++)); do
  if [[ $rounds == 3 && $round == 2 ]]; then
    sql 'DROP TABLE walg_delta_removed; DROP TABLE walg_delta_recreated; CREATE TABLE walg_delta_recreated AS SELECT 2 AS n; DELETE FROM walg_delta_tail WHERE i>10000' >/dev/null
    sql 'VACUUM walg_delta_tail' >/dev/null
  elif [[ $rounds == 3 && $round == 3 ]]; then
    sql 'INSERT INTO walg_delta_tail SELECT i, md5(i::text) FROM generate_series(10001,80000) i; TRUNCATE walg_delta_recreated; INSERT INTO walg_delta_recreated VALUES (3)' >/dev/null
  fi
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
  name=
  while IFS= read -r candidate; do
    if [[ " ${names[*]-} " != *" $candidate "* ]]; then
      [[ -z $name ]] || { echo 'multiple new backups' >&2; exit 1; }
      name=$candidate
    fi
  done < <(jq -r '.[].backup_name' "$POLAR_E2E_RESULTS_DIR/backups-$round.json")
  [[ $name == base_* && $name != *$'\n'* ]]
  if [[ $rounds == 3 && $round -gt 1 ]]; then
    [[ $name == *_D_* ]]
    jq -e '.PolarDBFullScanDelta == true' "$WALG_FILE_PREFIX/basebackups_005/${name}_backup_stop_sentinel.json" >/dev/null
    jq -e '[.Files[] | select(.IsIncremented == true)] | length > 0' "$WALG_FILE_PREFIX/basebackups_005/$name/files_metadata.json" >/dev/null
  fi
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
[[ $("$WALG_BIN" backup-list --json | jq length) == "$rounds" ]]

for ((round=1; round<=rounds; round++)); do
  export POLAR_E2E_TARGET_NAME="walg_archive_round_$round"
  mode="archive_$round"
  restore_dir="$POLAR_E2E_WORK_DIR/$mode"
  mkdir "$restore_dir"
  hook reset_restore "$mode" "$restore_dir"
  root=$(hook restore_pfs_data_path "$mode")
  [[ $root == /*/* && $root != "$POLAR_SOURCE_PFS_DATA_PATH" ]]
  WALG_POLARDB_PFS_DATA_PATH="$root" "$WALG_BIN" backup-fetch "$restore_dir" "${names[round-1]}" >"$POLAR_E2E_RESULTS_DIR/fetch-$round.log" 2>&1 &
  fetcher=$!
  staged=false
  # Observe throughout extraction: checking only after success would miss
  # the old implementation, which deleted its local staging tree at the end.
  while kill -0 "$fetcher" 2>/dev/null; do
    if [[ -e "$restore_dir/polar_shared_data" ]]; then staged=true; fi
    sleep .1
  done
  wait "$fetcher"
  fetcher=
  [[ $staged == false && ! -e "$restore_dir/polar_shared_data" ]] || {
    echo 'backup-fetch staged shared data on local disk' >&2
    exit 1
  }
  grep -q 'restored directly through PFSD without local shared-data staging' "$POLAR_E2E_RESULTS_DIR/fetch-$round.log"
  # prepare_restore MUST configure recovery_target_name and target_action=promote.
  hook prepare_restore "$mode" "$restore_dir"
  hook start_restore "$mode"
  # pg_ctl -w can succeed as soon as hot standby accepts connections, before
  # the requested PITR target has been replayed. Compare only after promotion.
  recovered=false
  for ((i=0; i<recovery_timeout; i++)); do
    if [[ $(hook digest "$mode" 'SELECT NOT pg_is_in_recovery()') == t ]]; then
      recovered=true
      break
    fi
    sleep 1
  done
  [[ $recovered == true ]] || { echo "$mode did not finish PITR" >&2; exit 1; }
  actual=$(hook digest "$mode" "$digest_sql")
  printf '%s\n' "$actual" >"$POLAR_E2E_RESULTS_DIR/restored-$round.txt"
  [[ $actual == "${digests[round-1]}" ]] || {
    echo "$mode restored digest differs from expected-$round.txt" >&2
    exit 1
  }
  [[ $(hook digest "$mode" 'SELECT count(*) FROM walg_archive_e2e WHERE i=120001') == 0 ]]
  [[ $(hook digest "$mode" 'SELECT NOT pg_is_in_recovery()') == t ]]
  hook stop_restore "$mode"
done
echo "Automatic archive, $rounds backups (delta=${POLAR_E2E_DELTA:-false}), unchanged first backup, concurrent rejection and PITR passed"
