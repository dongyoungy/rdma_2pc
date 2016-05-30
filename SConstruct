env = Environment(CC = 'mpicxx', CCFLAGS='-g')
binaries = []
b = SConscript('src/test/SConscript', variant_dir='build', duplicate=0, exports='env')
binaries.append(b)
env.Install('bin', binaries)
