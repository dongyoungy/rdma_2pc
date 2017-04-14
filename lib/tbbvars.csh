#!/bin/csh
setenv TBBROOT "/gpfs/gpfs0/groups/mozafari/dyoon/install/tbb/tbb-2017_U5" #
setenv tbb_bin "/gpfs/gpfs0/groups/mozafari/dyoon/install/tbb/tbb-2017_U5/build/linux_ppc64le_gcc_cc4.8.5_libc2.17_kernel3.10.0_release" #
if (! $?CPATH) then #
    setenv CPATH "${TBBROOT}/include" #
else #
    setenv CPATH "${TBBROOT}/include:$CPATH" #
endif #
if (! $?LIBRARY_PATH) then #
    setenv LIBRARY_PATH "${tbb_bin}" #
else #
    setenv LIBRARY_PATH "${tbb_bin}:$LIBRARY_PATH" #
endif #
if (! $?LD_LIBRARY_PATH) then #
    setenv LD_LIBRARY_PATH "${tbb_bin}" #
else #
    setenv LD_LIBRARY_PATH "${tbb_bin}:$LD_LIBRARY_PATH" #
endif #
 #
