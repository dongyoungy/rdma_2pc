import subprocess
import os

env = Environment(ENV = os.environ, CC = 'mpicc', CXX = 'mpic++')
#env = Environment(ENV = os.environ, CC = 'mpicc', CXX = 'mpic++')
SetOption('num_jobs', 8)
debug = ARGUMENTS.get('debug', 0)
pedantic = ARGUMENTS.get('pedantic', 0)
root_dir = Dir('#').abspath
poco_dir = "{0}/lib/poco-1.7.8p3".format(root_dir)
lib_dir = '--prefix=' + root_dir + '/lib'

# build POCO library
pococonfig = env.Command("pococonfig", "", "cd lib/poco-1.7.8p3 && ./configure --prefix={0}/poco".format(root_dir))
poco = env.Command("pocolib", "", "cd lib/poco-1.7.8p3 && make install -j 8 1> /dev/null")
env.AlwaysBuild(pococonfig)
env.AlwaysBuild(poco)
env.Depends(poco, pococonfig)

env.Append(CCFLAGS='-std=c++11')
env.Append(CCFLAGS='-Wall -Wextra -Werror -Wno-unused-variable -Wno-unused-parameter -pedantic')
env.Append(CPPPATH='{0}/include/'.format(root_dir))
env.Append(CPPPATH='{0}/poco/include/'.format(root_dir))
env.Append(LIBS=['tbb', 'tbbmalloc', 'PocoFoundation', 'PocoNet', 'PocoUtil'])
env.Append(LIBPATH='{0}/lib/'.format(root_dir))
env.Append(LIBPATH='{0}/poco/lib/'.format(root_dir))

if int(debug):
    env.Append(CCFLAGS='-g -pg')
    env.Append(LINKFLAGS='-pg')
else:
    env.Append(CCFLAGS='-O2')

print "running with -j", GetOption('num_jobs')
binaries = []
# b = SConscript('src/test/SConscript', variant_dir='build/test', duplicate=0, exports='env')
# binaries.append(b)
# b = SConscript('src/test_race_condition/SConscript', variant_dir='build/test_race_condition', duplicate=0, exports='env')
# binaries.append(b)
# b = SConscript('src/test_read_write/SConscript', variant_dir='build/test_read_write', duplicate=0, exports='env')
# binaries.append(b)
# b = SConscript('src/test_sort_unsort/SConscript', variant_dir='build/test_sort_unsort', duplicate=0, exports='env')
# binaries.append(b)
# b = SConscript('src/N-CoSED/SConscript', variant_dir='build/N-CoSED', duplicate=0, exports='env')
# binaries.append(b)
# b = SConscript('src/algo1/SConscript', variant_dir='build/algo1', duplicate=0, exports='env')
# binaries.append(b)
# b = SConscript('src/algo2/SConscript', variant_dir='build/algo2', duplicate=0, exports='env')
# binaries.append(b)
#b = SConscript('src/new_proto3/SConscript', variant_dir='build/new_proto3', duplicate=0, exports='env')
#binaries.append(b)
#b = SConscript('src/test_zipf/SConscript', variant_dir='build/test_zipf', duplicate=0, exports='env')
#binaries.append(b)
b = SConscript('src/test_cs_fa/SConscript', variant_dir='build/test_cs_fa', duplicate=0, exports='env')
binaries.append(b)
env.Depends(binaries, poco)
env.Install('bin', binaries)
