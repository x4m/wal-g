#!/usr/bin/env bash
set -Eeuo pipefail

# PolarDB/PFSD needs a Linux host, a block device and a running pfsdaemon.  The
# host-specific operations live in hooks; this file owns the reproducible test
# flow and deliberately knows nothing about a particular CI or PBD layout.

required=(WALG_BIN POLAR_E2E_HOOKS POLAR_SOURCE_PGDATA POLAR_SOURCE_PFS_DATA_PATH)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "required variable is not set: ${name}" >&2
    exit 2
  fi
done

for hook in preflight reset_source reset_restore restore_pfs_data_path \
  wait_backup_ready prepare_restore start_restore stop_restore digest; do
  if [[ ! -x "${POLAR_E2E_HOOKS}/${hook}" ]]; then
    echo "missing executable hook: ${POLAR_E2E_HOOKS}/${hook}" >&2
    exit 2
  fi
done

results_dir=${POLAR_E2E_RESULTS_DIR:-/tmp/wal-g-polardb-e2e-results}
work_dir=${POLAR_E2E_WORK_DIR:-/tmp/wal-g-polardb-e2e}
storage_root=${POLAR_E2E_STORAGE_ROOT:-${WALG_FILE_PREFIX:-}}
load_sql=${POLAR_E2E_LOAD_SQL:-"DROP TABLE IF EXISTS walg_e2e; CREATE TABLE walg_e2e(i bigint PRIMARY KEY, payload text NOT NULL); INSERT INTO walg_e2e SELECT i, md5(i::text) FROM generate_series(1, 100000) i;"}
rollback_sql=${POLAR_E2E_ROLLBACK_SQL:-"BEGIN; INSERT INTO walg_e2e VALUES (100001, md5(100001::text)); ROLLBACK;"}
checkpoint_sql=${POLAR_E2E_CHECKPOINT_SQL:-"CHECKPOINT;"}
digest_sql=${POLAR_E2E_DIGEST_SQL:-"SELECT count(*) || ':' || min(i) || ':' || max(i) || ':' || md5(string_agg(md5(payload), '' ORDER BY i)) FROM walg_e2e;"}
rollback_check_sql=${POLAR_E2E_ROLLBACK_CHECK_SQL:-"SELECT count(*) FROM walg_e2e WHERE i = 100001;"}
recovery_check_sql=${POLAR_E2E_RECOVERY_CHECK_SQL:-"SELECT NOT pg_is_in_recovery();"}
read -r -a modes <<<"${POLAR_E2E_MODES:-base_backup direct_pfsd}"

if [[ -z "${storage_root}" ]]; then
  echo "set POLAR_E2E_STORAGE_ROOT or WALG_FILE_PREFIX" >&2
  exit 2
fi
case "${work_dir}" in
  /|/tmp|/var|/var/tmp) echo "POLAR_E2E_WORK_DIR is too broad: ${work_dir}" >&2; exit 2 ;;
esac

mkdir -p "${results_dir}" "${work_dir}"
metrics="${results_dir}/metrics.jsonl"
: >"${metrics}"

run_hook() {
  local hook=$1
  shift
  "${POLAR_E2E_HOOKS}/${hook}" "$@"
}

record_timed() {
  local mode=$1 phase=$2
  shift 2
  local started finished elapsed status=0
  started=$(date +%s)
  "$@" || status=$?
  finished=$(date +%s)
  elapsed=$((finished - started))
  printf '{"mode":"%s","phase":"%s","seconds":%d,"status":%d}\n' \
    "${mode}" "${phase}" "${elapsed}" "${status}" | tee -a "${metrics}"
  return "${status}"
}

repository_for() {
  printf '%s/%s' "${storage_root%/}" "$1"
}

run_mode() {
  local mode=$1
  local restore_dir="${work_dir}/restore-${mode}"
  local repository
  repository=$(repository_for "${mode}")
  rm -rf "${restore_dir}"
  mkdir -p "${restore_dir}"
  unset WALG_POLARDB_PFS_DATA_PATH

  export WALG_FILE_PREFIX="${repository}"
  "${WALG_BIN}" delete everything FORCE --confirm >/dev/null 2>&1 || true

  if [[ "${mode}" == base_backup ]]; then
    record_timed "${mode}" backup "${WALG_BIN}" backup-push
  else
    export WALG_POLARDB_PFS_DATA_PATH="${POLAR_SOURCE_PFS_DATA_PATH}"
    record_timed "${mode}" backup "${WALG_BIN}" backup-push "${POLAR_SOURCE_PGDATA}"
    unset WALG_POLARDB_PFS_DATA_PATH
  fi

  "${WALG_BIN}" backup-list --json >"${results_dir}/backup-list-${mode}.json"
  record_timed "${mode}" wait_backup_ready run_hook wait_backup_ready "${mode}"
  record_timed "${mode}" reset_restore run_hook reset_restore "${mode}" "${restore_dir}"
  if [[ "${mode}" == direct_pfsd ]]; then
    local restore_pfs_data_path
    restore_pfs_data_path=$(run_hook restore_pfs_data_path "${mode}")
    if [[ "${restore_pfs_data_path}" != /*/* ]]; then
      echo "${mode}: restore_pfs_data_path must return /<pbd>/<path>, got '${restore_pfs_data_path}'" >&2
      return 1
    fi
    export WALG_POLARDB_PFS_DATA_PATH="${restore_pfs_data_path}"
  fi
  record_timed "${mode}" fetch "${WALG_BIN}" backup-fetch "${restore_dir}" LATEST
  unset WALG_POLARDB_PFS_DATA_PATH
  record_timed "${mode}" prepare_restore run_hook prepare_restore "${mode}" "${restore_dir}"
  record_timed "${mode}" start_restore run_hook start_restore "${mode}"
  local actual actual_rollback actual_recovery
  actual=$(run_hook digest "${mode}" "${digest_sql}")
  actual_rollback=$(run_hook digest "${mode}" "${rollback_check_sql}")
  actual_recovery=$(run_hook digest "${mode}" "${recovery_check_sql}")
  run_hook stop_restore "${mode}"
  printf '%s\n' "${actual}" >"${results_dir}/digest-${mode}.txt"
  printf '%s\n' "${actual_rollback}" >"${results_dir}/rollback-${mode}.txt"
  printf '%s\n' "${actual_recovery}" >"${results_dir}/recovery-${mode}.txt"
  if [[ "${actual}" != "${expected_digest}" ]]; then
    echo "${mode}: restored digest mismatch: expected '${expected_digest}', got '${actual}'" >&2
    return 1
  fi
  if [[ "${actual_rollback}" != "${expected_rollback}" ]]; then
    echo "${mode}: rollback mismatch: expected '${expected_rollback}', got '${actual_rollback}'" >&2
    return 1
  fi
  if [[ "${actual_recovery}" != "t" ]]; then
    echo "${mode}: restored node was not promoted: got '${actual_recovery}'" >&2
    return 1
  fi
}

run_hook preflight
cleanup() {
  local mode
  for mode in direct_pfsd base_backup source; do
    run_hook stop_restore "${mode}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT
run_hook reset_source
run_hook start_restore source
run_hook digest source "${load_sql}" >/dev/null
run_hook digest source "${rollback_sql}" >/dev/null
run_hook digest source "${checkpoint_sql}" >/dev/null
expected_digest=$(run_hook digest source "${digest_sql}")
expected_rollback=$(run_hook digest source "${rollback_check_sql}")
if [[ "${expected_rollback}" != "0" ]]; then
  echo "source rollback check failed: expected '0', got '${expected_rollback}'" >&2
  exit 1
fi
run_hook stop_restore source
printf '%s\n' "${expected_digest}" >"${results_dir}/digest-source.txt"
printf '%s\n' "${expected_rollback}" >"${results_dir}/rollback-source.txt"

# Keep the source server running while each hot backup is taken. Hooks may use
# distinct connection settings for the two restored clusters.
run_hook start_restore source
for mode in "${modes[@]}"; do
  case "${mode}" in
    base_backup|direct_pfsd) run_mode "${mode}" ;;
    *) echo "unknown POLAR_E2E_MODES entry: ${mode}" >&2; exit 2 ;;
  esac
done
run_hook stop_restore source

echo "PolarDB E2E passed; metrics: ${metrics}"
