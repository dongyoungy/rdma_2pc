#!/bin/bash
export TBBROOT="/gpfs/gpfs0/groups/mozafari/dyoon/install/tbb/tbb-2017_U5" #
tbb_bin="/gpfs/gpfs0/groups/mozafari/dyoon/install/tbb/tbb-2017_U5/build/linux_ppc64le_gcc_cc4.8.5_libc2.17_kernel3.10.0_release" #
if [ -z "$CPATH" ]; then #
    export CPATH="${TBBROOT}/include" #
else #
    export CPATH="${TBBROOT}/include:$CPATH" #
fi #
if [ -z "$LIBRARY_PATH" ]; then #
    export LIBRARY_PATH="${tbb_bin}" #
else #
    export LIBRARY_PATH="${tbb_bin}:$LIBRARY_PATH" #
fi #
if [ -z "$LD_LIBRARY_PATH" ]; then #
    export LD_LIBRARY_PATH="${tbb_bin}" #
else #
    export LD_LIBRARY_PATH="${tbb_bin}:$LD_LIBRARY_PATH" #
fi #
 #
