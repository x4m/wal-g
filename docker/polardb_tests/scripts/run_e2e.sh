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

for hook in preflight reset_source prepare_restore start_restore stop_restore digest; do
  if [[ ! -x "${POLAR_E2E_HOOKS}/${hook}" ]]; then
    echo "missing executable hook: ${POLAR_E2E_HOOKS}/${hook}" >&2
    exit 2
  fi
done

results_dir=${POLAR_E2E_RESULTS_DIR:-/tmp/wal-g-polardb-e2e-results}
work_dir=${POLAR_E2E_WORK_DIR:-/tmp/wal-g-polardb-e2e}
storage_root=${POLAR_E2E_STORAGE_ROOT:-${WALG_FILE_PREFIX:-}}
load_sql=${POLAR_E2E_LOAD_SQL:-"CREATE TABLE IF NOT EXISTS walg_e2e AS SELECT i, md5(i::text) AS payload FROM generate_series(1, 100000) i; CHECKPOINT;"}
digest_sql=${POLAR_E2E_DIGEST_SQL:-"SELECT count(*) || ':' || min(i) || ':' || max(i) || ':' || md5(string_agg(md5(payload), '' ORDER BY i)) FROM walg_e2e;"}
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
  record_timed "${mode}" fetch "${WALG_BIN}" backup-fetch "${restore_dir}" LATEST
  record_timed "${mode}" prepare_restore run_hook prepare_restore "${mode}" "${restore_dir}"
  record_timed "${mode}" start_restore run_hook start_restore "${mode}"
  local actual
  actual=$(run_hook digest "${mode}" "${digest_sql}")
  run_hook stop_restore "${mode}"
  printf '%s\n' "${actual}" >"${results_dir}/digest-${mode}.txt"
  if [[ "${actual}" != "${expected_digest}" ]]; then
    echo "${mode}: restored digest mismatch: expected '${expected_digest}', got '${actual}'" >&2
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
expected_digest=$(run_hook digest source "${digest_sql}")
run_hook stop_restore source
printf '%s\n' "${expected_digest}" >"${results_dir}/digest-source.txt"

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
