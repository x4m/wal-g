#!/usr/bin/env bash
set -Eeuo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "${tmp}"' EXIT
mkdir -p "${tmp}/hooks" "${tmp}/source"

for hook in preflight reset_source reset_restore restore_pfs_data_path \
  wait_backup_ready prepare_restore start_restore stop_restore digest; do
  ln -s "${root}/testdata/fake_hook" "${tmp}/hooks/${hook}"
done

E2E_STATE="${tmp}/state" \
WALG_BIN="${root}/testdata/fake_wal_g" \
POLAR_E2E_HOOKS="${tmp}/hooks" \
POLAR_SOURCE_PGDATA="${tmp}/source" \
POLAR_SOURCE_PFS_DATA_PATH=/fake-pbd/source \
POLAR_E2E_STORAGE_ROOT="${tmp}/repository" \
POLAR_E2E_WORK_DIR="${tmp}/work" \
POLAR_E2E_RESULTS_DIR="${tmp}/results" \
  "${root}/scripts/run_e2e.sh"

[[ $(wc -l <"${tmp}/results/metrics.jsonl") -eq 12 ]]
cmp "${tmp}/results/digest-source.txt" "${tmp}/results/digest-base_backup.txt"
cmp "${tmp}/results/digest-source.txt" "${tmp}/results/digest-direct_pfsd.txt"
cmp "${tmp}/results/rollback-source.txt" "${tmp}/results/rollback-base_backup.txt"
cmp "${tmp}/results/rollback-source.txt" "${tmp}/results/rollback-direct_pfsd.txt"
grep -qx t "${tmp}/results/recovery-base_backup.txt"
grep -qx t "${tmp}/results/recovery-direct_pfsd.txt"
grep -q 'base_backup' "${tmp}/results/metrics.jsonl"
grep -q 'direct_pfsd' "${tmp}/results/metrics.jsonl"
echo "PolarDB E2E runner self-test passed"
