import os
env = Environment(ENV = os.environ, CC = 'mpicc', CXX = 'mpic++')
debug = ARGUMENTS.get('debug', 0)
if int(debug):
    env.Append(CCFLAGS='-g')
else:
    env.Append(CCFLAGS='-O2')
binaries = []
b = SConscript('src/test/SConscript', variant_dir='build/test', duplicate=0, exports='env')
binaries.append(b)
b = SConscript('src/proto/SConscript', variant_dir='build/proto', duplicate=0, exports='env')
binaries.append(b)
env.Install('bin', binaries)
