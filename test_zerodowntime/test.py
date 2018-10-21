import argparse
import contextlib
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time


# Change directory context manager from https://stackoverflow.com/a/24176022
@contextlib.contextmanager
def cd(newdir):
	prevdir = os.getcwd()
	os.chdir(os.path.expanduser(newdir))
	try:
		yield
	finally:
		os.chdir(prevdir)


# Parse arguments
parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('revA', help = 'path or git revision for the "old" version. When it is a path, it must be the directory containing the pysyncobj package. When it is a git revision, the parent directory of the directory containing this script must be the git repository, and this repository must contain the revision (i.e. run this script from within the repository).')
parser.add_argument('revB', help = 'path or git revision for the "new" version')
parser.add_argument('cycles', nargs = '?', type = int, default = 120, help = 'Number of cycles to run; must be at least ten times the number of processes')
parser.add_argument('processes', nargs = '?', type = int, default = 10, help = 'Number of parallel processes; must be at least 3')
parser.add_argument('seed', nargs = '?', type = int, default = None, help = 'Seed for PRNG. Using the same seed value produces the exact same order of operations *in this test script*, i.e. outside of PySyncObj. Everything inside the cluster, e.g. which node is elected leader and when, is essentially still completely random.')
args = parser.parse_args()

if args.processes < 3:
	print('Testing with less than 3 processes makes no sense', file = sys.stderr)
	sys.exit(1)

if args.cycles < args.processes * 10:
	print('Needs at least ten times as many cycles as there are processes to get useful results', file = sys.stderr)
	sys.exit(1)

workingDir = os.path.abspath(os.path.dirname(__file__))

# Seed
seed = args.seed
if seed is None:
	seed = random.randint(0, 2**32 - 1)
	print('Seed: {}'.format(seed))
random.seed(seed)

# Generate command to be executed at each cycle
commands = [] # list of tuples (proc index, command)
	# Commands:
	#  'increment' -- send an increment command to the process, wait until it returns 'incremented'
	#  'compare' -- compare the value across all processes, verify that the majority has the same, expected value; proc index is irrelevant in this case
	#  'upgrade' -- quit the process, upgrade the code, restart the process
for i in range(args.cycles):
	cmd = random.choice(('increment', 'increment', 'increment', 'increment', 'compare')) # 80 % increment, 20 % compare
	proc = random.randrange(args.processes)
	commands.append((proc, cmd))

upgrades = list(range(args.processes))
random.shuffle(upgrades)
# First upgrade at 20 % of the cycles, last at 80 %, equal cycle distance between
	# This, combined with the cycles >= 10 * processes requirement, also ensures that the upgrades don't overlap.
	# Each upgrade takes 3 cycles plus the startup time of the new process, which shouldn't be much worse than 1-2 cycles.
	# 60 % of the cycles must therefore be at least 5 times the number of processes, i.e. cycles >= 5/0.6 * processes = 8.33 * processes.
for i in range(args.processes):
	upgradeCycle = int((0.2 + 0.6 * i / (args.processes - 1)) * args.cycles)
	commands[upgradeCycle] = (upgrades[i], 'upgrade')
	# Ensure that this process doesn't receive any increment operations while it's upgrading
	for j in range(upgradeCycle, upgradeCycle + 3):
		if commands[j][1] == 'increment':
			while commands[j][0] == upgrades[i]:
				commands[j] = (random.randrange(args.processes), 'increment')

# Generate node addresses
addrs = ['127.0.0.1:{}'.format(42000 + i) for i in range(args.processes)]

status = 0

# Set up temporary directory
with tempfile.TemporaryDirectory() as tmpdirname:
	with cd(tmpdirname):
		os.mkdir('revA')
		os.mkdir('revB')

		# Check out revisions into the temporary directory
		for revArg, revTarget in ((args.revA, 'revA'), (args.revB, 'revB')):
			if os.path.isdir(os.path.join(workingDir, revArg)):
				# Copy directory contents to ./revTarget; I like rsync...
				if subprocess.call(['rsync', '-a', os.path.join(workingDir, revArg, ''), os.path.join(revTarget, '')]) != 0:
					print('rsync of {} failed'.format(revTarget), file = sys.stderr)
					sys.exit(1)
			else:
				with cd(os.path.join(workingDir, '..')): #TODO: Replace with GIT_DIR environment variable or something
					gitProc = subprocess.Popen(['git', 'archive', revArg], stdout = subprocess.PIPE)
					tarProc = subprocess.Popen(['tar', '-x', '-C', os.path.join(tmpdirname, revTarget), '--strip-components', '1', 'pysyncobj'], stdin = gitProc.stdout)
					gitProc.stdout.close()
					tarProc.communicate()
					if tarProc.returncode != 0:
						print('git or tar of {} failed'.format(revTarget), file = sys.stderr)
						sys.exit(1)

			with open(os.path.join(revTarget, 'testrevision.py'), 'w') as fp:
				fp.write('rev = {!r}'.format(revTarget))

		# Create each process's directory and initialise it with the revision A
		for i in range(args.processes):
			os.mkdir('proc{}'.format(i))
			os.mkdir(os.path.join('proc{}'.format(i), 'pysyncobj'))
			if subprocess.call(['rsync', '-a', os.path.join('revA', ''), os.path.join('proc{}'.format(i), 'pysyncobj', '')]) != 0:
				print('rsync of revA to proc{} failed'.format(i), file = sys.stderr)
				sys.exit(1)
			if subprocess.call(['rsync', '-a', os.path.join(workingDir, 'proc.py'), os.path.join('proc{}'.format(i), '')]) != 0:
				print('rsync of proc.py to proc{} failed'.format(i), file = sys.stderr)
				sys.exit(1)

		procs = []

		try:
			# Launch processes
			for i in range(args.processes):
				with cd('proc{}'.format(i)):
					procs.append(subprocess.Popen(['python3', 'proc.py', addrs[i]] + [addrs[j] for j in range(args.processes) if j != i], stdin = subprocess.PIPE, stdout = subprocess.PIPE, bufsize = 0))

			# Randomly run commands on the custer and upgrade the processes one-by-one, ensuring that everything's still fine after each step
			counter = 0 # The expected value of the counter
			restart = -1 # Variable for when to restart a process; set to 3 on the 'upgrade' command, counted down on each command, the upgraded process is restarted when it reaches zero
			upgradingProcId = None # The procId that is currently upgrading
			for procId, command in commands:
				if command == 'increment':
					assert procId != upgradingProcId, 'previous upgrade hasn''t finished'

					print('Sending increment to proc{}'.format(procId))

					# Send command
					procs[procId].stdin.write(b'increment\n')
					procs[procId].stdin.flush()

					# Wait until process is done with incrementing
					procs[procId].stdout.readline()

					counter += 1
				elif command == 'compare':
					print('Comparing')

					# Compare the *logs* of the processes
					# Comparing the values of the counter doesn't work because the commands might not have been applied yet.
					# So if the values don't match, that doesn't mean that replication is broken.
					# The log reflects what's actually replicated.

					for i in range(args.processes):
						if i == upgradingProcId:
							continue
						procs[i].stdin.write(b'printlog\n')
						procs[i].stdin.flush()
					logs = [procs[i].stdout.readline().strip() if i != upgradingProcId else None for i in range(args.processes)]

					# Ensure that a majority of the logs are equal; note that this doesn't verify that all increments were actually replicated.

					ok = False
					for i in range((args.processes + 1) // 2):
						count = 1
						for j in range(i, args.processes):
							if logs[i] == logs[j]:
								count += 1
						if count >= args.processes // 2 + 1:
							ok = True
							break
					if not ok:
						print('Didn''t find at least {} matching logs'.format(args.processes // 2 + 1), file = sys.stderr)
						for i in range(args.processes):
							print('proc{} log: {}'.format(i, logs[i].decode('utf-8')), file = sys.stderr)
						sys.exit(1)
				elif command == 'upgrade':
					assert upgradingProcId is None, 'previous upgrade hasn''t finished'

					print('Taking down proc{} for upgrade'.format(procId))

					# Let the process finish gracefully
					procs[procId].stdin.write(b'quit\n')
					procs[procId].stdin.flush()
					procs[procId].wait()

					# Delete revA code
					shutil.rmtree(os.path.join('proc{}'.format(procId), 'pysyncobj'))
					os.mkdir(os.path.join('proc{}'.format(procId), 'pysyncobj'))

					# Copy revB
					if subprocess.call(['rsync', '-a', os.path.join('revB', ''), os.path.join('proc{}'.format(procId), 'pysyncobj', '')]) != 0:
						print('rsync of revB to proc{} failed'.format(procId), file = sys.stderr)
						sys.exit(1)

					upgradingProcId = procId
					restart = 3

				restart -= 1
				if restart == 0:
					print('Restarting proc{}'.format(upgradingProcId))
					with cd('proc{}'.format(upgradingProcId)):
						procs[upgradingProcId] = subprocess.Popen(['python3', 'proc.py', addrs[upgradingProcId]] + [addrs[j] for j in range(args.processes) if j != upgradingProcId], stdin = subprocess.PIPE, stdout = subprocess.PIPE, bufsize = 0)
					upgradingProcId = None

			print('Final comparison...')

			# Give the processes some time to catch up
			time.sleep(5)

			# Check that all logs are the same, and that all counter values are equal to the expected value
			for i in range(args.processes):
				procs[i].stdin.write(b'printlog\n')
				procs[i].stdin.flush()
			logs = [procs[i].stdout.readline().strip() for i in range(args.processes)]

			for i in range(args.processes):
				procs[i].stdin.write(b'print\n')
				procs[i].stdin.flush()
			counters = [int(procs[i].stdout.readline().strip()) for i in range(args.processes)]

			if not all(x == logs[0] for x in logs):
				print('ERROR: not all logs are equal', file = sys.stderr)
				for i in range(args.processes):
					print('proc{} log: {}'.format(i, logs[i].decode('utf-8')), file = sys.stderr)
				status = 1
			elif not all(x == counter for x in counters):
				print('ERROR: not all counters are equal to the expected value {}: {}'.format(counter, counters), file = sys.stderr)
				status = 1
			else:
				print('OK', file = sys.stderr)

			print('Sending quit command', file = sys.stderr)
			for i in range(args.processes):
				procs[i].stdin.write(b'quit\n')
			for i in range(args.processes):
				procs[i].communicate()
		except:
			print('Killing processes', file = sys.stderr)
			for proc in procs:
				proc.kill()
			raise

sys.exit(status)
