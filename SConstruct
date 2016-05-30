env = Environment(CC = 'mpicxx')
debug = ARGUMENTS.get('debug', 0)
if int(debug):
    env.Append(CCFLAGS='-g')
else:
    env.Append(CCFLAGS='-O2')
binaries = []
b = SConscript('src/test/SConscript', variant_dir='build', duplicate=0, exports='env')
binaries.append(b)
env.Install('bin', binaries)
