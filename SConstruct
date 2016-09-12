import os
env = Environment(ENV = os.environ, CC = 'mpicc', CXX = 'mpic++')
SetOption('num_jobs', 8)
print "running with -j", 8
debug = ARGUMENTS.get('debug', 0)
if int(debug):
    env.Append(CCFLAGS='-g')
else:
    env.Append(CCFLAGS='-O2')
binaries = []
b = SConscript('src/test/SConscript', variant_dir='build/test', duplicate=0, exports='env')
binaries.append(b)
b = SConscript('src/test_race_condition/SConscript', variant_dir='build/test_race_condition', duplicate=0, exports='env')
binaries.append(b)
b = SConscript('src/new_proto/SConscript', variant_dir='build/new_proto', duplicate=0, exports='env')
binaries.append(b)
b = SConscript('src/N-CoSED/SConscript', variant_dir='build/N-CoSED', duplicate=0, exports='env')
binaries.append(b)
b = SConscript('src/algo1/SConscript', variant_dir='build/algo1', duplicate=0, exports='env')
binaries.append(b)
b = SConscript('src/algo2/SConscript', variant_dir='build/algo2', duplicate=0, exports='env')
binaries.append(b)
env.Install('bin', binaries)
