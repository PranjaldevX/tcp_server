#!/usr/bin/env bash
# Prints the environment details a benchmark report has to contain (SRS PR-5).
# usage: scripts/collect_environment.sh

cpu_model=$(lscpu 2>/dev/null | sed -n 's/^Model name:[[:space:]]*//p' | head -1)
[ -z "$cpu_model" ] && cpu_model=$(sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -1)

# is this machine itself a virtual machine? (WSL2 is one)
virtualized="no"
if command -v systemd-detect-virt >/dev/null 2>&1; then
    detected=$(systemd-detect-virt 2>/dev/null)
    [ -n "$detected" ] && [ "$detected" != "none" ] && virtualized="yes ($detected)"
fi
if grep -qi microsoft /proc/version 2>/dev/null; then
    virtualized="yes (WSL: numbers from here are noisy, repeat final runs on native Linux)"
fi
hypervisor=$(lscpu 2>/dev/null | sed -n 's/^Hypervisor vendor:[[:space:]]*//p' | head -1)
[ "$virtualized" = "no" ] && [ -n "$hypervisor" ] && virtualized="yes (hypervisor: $hypervisor)"

echo "date:             $(date -Iseconds)"
echo "kernel:           $(uname -sr)"
echo "cpu model:        $cpu_model"
echo "cpu threads:      $(nproc)"
echo "memory:           $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo)"
echo "virtualized:      $virtualized"
echo "cpu governor:     $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
echo "compiler:         $(g++ --version | head -1)"
echo "open file limit:  $(ulimit -n)"
