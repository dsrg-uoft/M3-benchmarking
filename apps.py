from pprint import pprint
import signal
import sys
import os
import subprocess
from time import clock_gettime, CLOCK_REALTIME, sleep
import shutil
import abc
from typing import List, Dict, Union, NoReturn, TextIO, Callable, Set, Collection

K = 1024
M = 1024 * 1024
G = 1024 * 1024 * 1024

def memify(arg: str) -> int:
	if arg[-1] == "k" or arg[-1] == "K":
		return int(arg[:-1]) * K
	elif arg[-1] == "m" or arg[-1] == "M":
		return int(arg[:-1]) * M
	elif arg[-1] == "g" or arg[-1] == "G":
		return int(arg[:-1]) * G
	else:
		return int(arg)

def do_cmds(cmds: List[List[str]], quiet: bool = False) -> None:
	procs: List[subprocess.Popen] = []
	for cmd in cmds:
		procs.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
	for proc in procs:
		proc.wait()
		if not quiet and proc.returncode != 0:
			print("[warn] ret: {} for: {}".format(proc.returncode, proc.args))
			print(proc.stdout.read().decode("utf-8"))
			print(proc.stderr.read().decode("utf-8"))

# TODO: change this to take List[str] / Callable[[str], List[str]]?
def ssh_bakers(bakers: Collection[str], cmd: Union[str, Callable[[str], str]], quiet: bool = False) -> None:
	cmds: List[List[str]] = []
	for baker in bakers:
		cmds.append(["ssh", baker, cmd(baker) if callable(cmd) else cmd])
	do_cmds(cmds, quiet)

def rsync_bakers(bakers: Set[str], src_fn: Callable[[str], str], dst_fn: Callable[[str], str],
		exclude: str = None) -> None:
	cmds: List[List[str]] = []
	for baker in bakers:
		cmd = [ "rsync", "-a" ]
		if exclude:
			cmd.append("--exclude=" + exclude)
		cmd.append(src_fn(baker))
		cmd.append(dst_fn(baker))
		cmds.append(cmd)
	do_cmds(cmds)

def wait_and_report(proc: subprocess.Popen):
	proc.wait()
	if proc.returncode != 0:
		print("[warn] ret: {} for: {}".format(proc.returncode, proc.args))

class cgroup:
	def __init__(self, bakers: Set[str], group: str, mem: str) -> None:
		self.test_home: str
		self.bakers: Set[str] = bakers
		self.group = group
		self.mem = mem
		self.name = group.split(':')[-1]
		self.init_done: bool = False

	def write_conf(self) -> None:
		with open(self.test_home + "/conf/cgroup_" + self.name, "a") as conf_f:
			conf_f.write("type {}\n".format(type(self).__name__))
			conf_f.write("bakers {}\n".format(' '.join(self.bakers)))
			conf_f.write("group {}\n".format(self.group))
			conf_f.write("mem {}\n".format(self.mem))

	def prologue(self) -> None:
		print("running cgroup.prologue")
		ssh_bakers(self.bakers, "/homes/eurosys21/cg_helper " + self.group)
		ssh_bakers(self.bakers, "echo {} > /sys/fs/cgroup/memory/{}/memory.limit_in_bytes".format(self.mem, self.name))

class daemon:
	def __init__(self, bakers: Set[str], test_home: str, name: str, cmd: str) -> None:
		self.bakers: Set[str] = bakers
		self.name: str = name
		self.test_home: str = test_home
		self.test_log_dir: str = test_home + '/' + name
		self.cmd: str = cmd
		self.procs: List[subprocess.Popen] = []
		self.logs: List[TextIO] = []

	def write_conf(self) -> None:
		with open(self.test_home + "/conf/" + self.name, "a") as conf_f:
			conf_f.write("type {}\n".format(type(self).__name__))
			conf_f.write("test_log_dir {}\n".format(self.test_log_dir))
			conf_f.write("bakers {}\n".format(' '.join(self.bakers)))

	def prologue(self) -> None:
		print("running daemon.prologue")
		# both sigve and obs rely on this so always make it
		ssh_bakers(self.bakers, "mkdir /tmp/sigve", quiet=True)
		os.mkdir(self.test_log_dir)
		for baker in self.bakers:
			log = open(self.test_log_dir + '/' + baker + ".log", 'w')
			self.logs.append(log)
			self.procs.append(subprocess.Popen(["ssh", baker, self.cmd], stdout=log, stderr=log))

	def epilogue(self) -> None:
		print("running daemon.epilogue")
		for proc in self.procs:
			proc.terminate()
		for log in self.logs:
			log.close()

	def clean(self) -> None:
		pass

class sigve_conf:
	def __init__(self, home: str,
			top: str = "62g",
			low_wm_init: str = "50g",
			high_wm_init: str = "55g",
			low_wm_ratio: int = 32,
			high_wm_ratio: int = 32,
			low_wm_period: int = 32,
			high_wm_period: int = 32,
			wm_increment_percent: int = 2,
			high_wm_pool: int = 100,
			expected_shrink: int = 30,
			kill_time: int = 30000,
			poll_time: int = 1000) -> None:
		self.home: str = home
		self.top: int = memify(top)
		self.low_wm_init: int = memify(low_wm_init)
		self.high_wm_init: int = memify(high_wm_init)
		self.low_wm_ratio: int = low_wm_ratio
		self.high_wm_ratio: int = high_wm_ratio
		self.low_wm_period: int = low_wm_period
		self.high_wm_period: int = high_wm_period
		self.wm_increment_percent: int = wm_increment_percent
		self.high_wm_pool: int = high_wm_pool
		self.expected_shrink: int = expected_shrink
		self.kill_time: int = kill_time
		self.poll_time: int = poll_time

	def args(self) -> str:
		return ("{} " * 12).format(self.top,
				self.low_wm_init,
				self.high_wm_init,
				self.low_wm_ratio,
				self.high_wm_ratio,
				self.low_wm_period,
				self.high_wm_period,
				self.wm_increment_percent,
				self.high_wm_pool,
				self.expected_shrink,
				self.kill_time,
				self.poll_time)

	def write_conf(self, conf_f: TextIO) -> None:
		conf_f.write("home {}\n".format(self.home))
		conf_f.write("top {}\n".format(self.top))
		conf_f.write("low_wm_init {}\n".format(self.low_wm_init))
		conf_f.write("high_wm_init {}\n".format(self.high_wm_init))
		conf_f.write("low_wm_ratio {}\n".format(self.low_wm_ratio))
		conf_f.write("high_wm_ratio {}\n".format(self.high_wm_ratio))
		conf_f.write("low_wm_period {}\n".format(self.low_wm_period))
		conf_f.write("high_wm_period {}\n".format(self.high_wm_period))
		conf_f.write("wm_increment_percent {}\n".format(self.wm_increment_percent))
		conf_f.write("high_wm_pool {}\n".format(self.high_wm_pool))
		conf_f.write("expected_shrink {}\n".format(self.expected_shrink))
		conf_f.write("kill_time {}\n".format(self.kill_time))
		conf_f.write("poll_time {}\n".format(self.poll_time))

class sigve_daemon(daemon):
	def __init__(self, bakers: Set[str], test_home: str, conf: sigve_conf) -> None:
		self.conf = conf
		cmd = "{}/bin/sigve {}".format(self.conf.home, self.conf.args())
		super(sigve_daemon, self).__init__(bakers, test_home, "sigve", cmd)

	def write_conf(self) -> None:
		print("running sigve_daemon.write_conf")
		super(sigve_daemon, self).write_conf()
		with open(self.test_home + "/conf/" + self.name, "a") as conf_f:
			self.conf.write_conf(conf_f)

	def clean(self):
		print("running sigve_daemon.clean")
		ssh_bakers(self.bakers, "rm -r /tmp/sigve", quiet=True)

class obs_daemon(daemon):
	def __init__(self, bakers: Set[str], test_home) -> None:
		observer_cmd: str = "source /homes/adrian/cluster-misc/thermostat-scripts/env/bin/activate; "
		observer_cmd += "export PYTHONUNBUFFERED=1; "
		observer_cmd += "/homes/adrian/cluster-misc/krgc-scripts/obs.py"
		super(obs_daemon, self).__init__(bakers, test_home, "observer_wards", observer_cmd)

class jvm_conf:
	def __init__(self, home: str, args: List[str] = []) -> None:
		self.home = home
		self.args = args
		self.use_sigve: int = 0
		self.sigve_percent: int = -1

	def heap(self, xmx: str = "245g", xms: str = None) -> "jvm_conf":
		self.max = xmx
		if xms:
			self.args.append("-Xms" + xms)
		return self

	def sigve(self, cost: int = 50) -> "jvm_conf":
		self.use_sigve: int = 1
		self.sigve_percent: int = cost
		self.args += [ "-XX:+UseSIGVE", "-XX:SIGVECost=" + str(cost) ]
		return self

	def __str__(self) -> str:
		return "-Xmx" + self.max + ' ' + ' '.join(self.args)

	def write_conf(self, conf_f: TextIO) -> None:
		conf_f.write("xmx {}\n".format(self.max))
		conf_f.write("use_sigve {}\n".format(self.use_sigve))
		conf_f.write("sigve_percent {}\n".format(self.sigve_percent))
		conf_f.write("args {}\n".format(self))

class go_conf:
	def __init__(self, args: Dict[str, str] = None) -> None:
		self.args = args if args else {}

	def sigve(self, threshold: int = 10) -> "go_conf":
		self.args["GO_SIGVE"] = "1"
		self.args["GO_SIGVE_PERCENT"] = str(threshold)
		return self

	def write_conf(self, conf_f: TextIO) -> None:
		for k, v in self.args.items():
			conf_f.write("{} {}\n".format(k, v))

class application:
	def __init__(self, bakers: Set[str], cg: cgroup) -> None:
		self.test_home: str
		self.name: str
		self.test_log_dir: str
		self.bakers = bakers
		self.cg: cgroup = cg
		self.prepare_done: bool = False
		self.init_done: bool = False
		self.epilogue_done: bool = False
		self.clean_done: bool = False

	def prepare(self, test_home: str, order: int) -> None:
		self.test_home = test_home
		self.name = type(self).__name__ + str(order)
		self.test_log_dir = self.test_home + '/' + self.name
		if self.cg:
			self.cg.test_home = test_home

	def write_conf(self) -> None:
		raise NotImplementedError
	def prologue(self) -> None:
		raise NotImplementedError
	def epilogue(self) -> None:
		raise NotImplementedError
	def clean(self) -> None:
		raise NotImplementedError

class hibench_spark(application):
	def __init__(self, bakers: Set[str], hibench_home: str, spark_home: str,
			jvm: jvm_conf, scale: str = "bigdata0", workload: str = "ml/kmeans",
			cores: int = -1, max_cores: int = -1, mem_frac: float = -1,
			mem_storage_frac: float = -1, sigve: bool = False, sigve_n: int = -1,
			sigve_f: float = -1, cg: cgroup = None) -> None:
		super(hibench_spark, self).__init__(bakers, cg)
		self.hibench_home: str = hibench_home
		self.spark_home: str = spark_home
		self.jvm: jvm_conf = jvm
		self.scale: str = scale
		self.workload: str = workload
		self.cores: int = cores
		self.max_cores: int = max_cores
		self.mem_frac: float = mem_frac
		self.mem_storage_frac: float = mem_storage_frac
		self.sigve: bool = sigve
		self.sigve_n: int = sigve_n
		self.sigve_f: float = sigve_f

	def write_conf(self) -> None:
		with open(self.test_home + "/conf/" + self.name, "a") as conf_f:
			conf_f.write("type {}\n".format(type(self).__name__))
			conf_f.write("bakers {}\n".format(' '.join(self.bakers)))
			conf_f.write("test_home {}\n".format(self.test_home))
			conf_f.write("name {}\n".format(self.name))
			conf_f.write("hibench_home {}\n".format(self.hibench_home))
			conf_f.write("spark_home {}\n".format(self.spark_home))
			conf_f.write("scale {}\n".format(self.scale))
			conf_f.write("workload {}\n".format(self.workload))
			conf_f.write("cores {}\n".format(self.cores))
			conf_f.write("max_cores {}\n".format(self.max_cores))
			conf_f.write("mem_fraction {}\n".format(self.mem_frac))
			conf_f.write("mem_storage_fraction {}\n".format(self.mem_storage_frac))
			conf_f.write("test_log_dir {}\n".format(self.test_log_dir))
			conf_f.write("conf_dir {}\n".format(self.conf_dir))
			conf_f.write("report_dir {}\n".format(self.report_dir))
			conf_f.write("spark_log_dir {}\n".format(self.spark_log_dir))
			conf_f.write("sigve {}\n".format(self.sigve))
			conf_f.write("sigve_n {}\n".format(self.sigve_n))
			conf_f.write("sigve_f {}\n".format(self.sigve_f))
			conf_f.write("cgroup {}\n".format("None" if self.cg is None else self.cg.name))
			self.jvm.write_conf(conf_f)

	def setup_cgroup(self) -> None:
		java_bin = self.jvm.home + "/bin"
		ssh_bakers(self.bakers, "rm " + java_bin + "/java")
		if self.cg:
			ssh_bakers(self.bakers, "cp {} {}".format(java_bin + "/java_cgroup", java_bin + "/java"))
			sed_java = "sed -i "
			sed_java += "s/CHANGE_ME/{}/ ".format(self.cg.group)
			sed_java += java_bin + "/java"
			ssh_bakers(self.bakers, sed_java)
		else:
			ssh_bakers(self.bakers, "cp {} {}".format(java_bin + "/java_real", java_bin + "/java"))

	def prologue(self) -> None:
		print("running hibench_spark.prologue")
		self.conf_dir = self.test_log_dir + "/conf"
		self.report_dir = self.test_log_dir + "/report/" + self.workload.split('/')[-1] + "/spark"
		# note all spark jobs in this test will use this dir
		# only first epilogue will succeed to scp logs into this directory
		# use appid to distinguish jobs
		self.spark_log_dir: str = self.test_home + "/spark"

		os.mkdir(self.test_log_dir)
		os.mkdir(self.conf_dir)
		self.setup_cgroup()
		if not os.path.exists(self.spark_log_dir):
			os.mkdir(self.spark_log_dir)
		shutil.copy(self.hibench_home + "/conf/hibench.conf", self.conf_dir)
		shutil.copy(self.hibench_home + "/conf/hadoop.conf", self.conf_dir)
		shutil.copy(self.hibench_home + "/conf/spark.conf", self.conf_dir)
		os.makedirs(self.report_dir)

		hibench_conf = self.conf_dir + "/hibench.conf"
		spark_conf = self.conf_dir + "/spark.conf"

		# setup hibench.conf
		sed_report = "s|\${{hibench\.home}}/report|{}/report|".format(self.test_log_dir)
		subprocess.run(["sed", "-i", sed_report, hibench_conf])
		sed_scale = "s/\(hibench\.scale\.profile\).*/\\1 {}/".format(self.scale)
		subprocess.run(["sed", "-i", sed_scale, hibench_conf])

		# setup spark.conf
		sed_heap_size ="s/\(spark\.executor\.memory\).*/\\1 {}/".format(self.jvm.max)
		subprocess.run(["sed", "-i", sed_heap_size, spark_conf])
		if self.cores != -1:
			sed_cores = "s/^#\(spark\.executor\.cores\).*/\\1 {}/".format(self.cores)
			subprocess.run(["sed", "-i", sed_cores, spark_conf])
		if self.max_cores != -1:
			sed_max_cores = "s/^#\(spark\.cores\.max\).*/\\1 {}/".format(self.max_cores)
			subprocess.run(["sed", "-i", sed_max_cores, spark_conf])
		if self.mem_frac != -1:
			sed_mem_frac = "s/^#\(spark\.memory\.fraction\).*/\\1 {}/".format(self.mem_frac)
			subprocess.run(["sed", "-i", sed_mem_frac, spark_conf])
		if self.mem_storage_frac != -1:
			sed_mem_storage_frac = "s/^#\(spark\.memory\.storageFraction\).*/\\1 {}/".format(self.mem_storage_frac)
			subprocess.run(["sed", "-i", sed_mem_storage_frac, spark_conf])

		if self.sigve:
			sed_sigve = "s/^#\(spark\.sigve\) .*/\\1 true/"
			subprocess.run(["sed", "-i", sed_sigve, spark_conf])
		if self.sigve_n != -1:
			sed_sigve_n = "s/^#\(spark\.sigve_n\) .*/\\1 {}/".format(self.sigve_n)
			subprocess.run(["sed", "-i", sed_sigve_n, spark_conf])
		if self.sigve_f != -1:
			sed_sigve_f = "s/^#\(spark\.sigve_f\) .*/\\1 {}/".format(self.sigve_f)
			subprocess.run(["sed", "-i", sed_sigve_f, spark_conf])

		sed_jvm = "/^#.*PrintGCTimeStamps$/s/^#\(.*\)/\\1 {}/".format(' '.join(self.jvm.args))
		subprocess.run(["sed", "-i", sed_jvm, spark_conf])

	def epilogue(self) -> None:
		src_fn: Callable[[str], str] = lambda baker: baker + ':' + self.spark_home + "/work"
		dst_fn: Callable[[str], str] = lambda baker: self.spark_log_dir + '/' + baker
		rsync_bakers(self.bakers, src_fn, dst_fn, "*.jar")
		java_bin = self.jvm.home + "/bin"
		ssh_bakers(self.bakers, "cp {} {}".format(java_bin + "/java_real", java_bin + "/java"))

		subprocess.run(["pkill", "-9", "-f", "SparkSubmit"])
		ssh_bakers(self.bakers, r'pkill -9 -f "java_real .*CoarseGrainedExecutorBackend"', quiet = True)

	def clean(self) -> None:
		print("running hibench_spark.clean")
		ssh_bakers(self.bakers, "rm -rf " + self.spark_home + "/work/*", quiet=True)

class detc(application):
	def __init__(self, bakers: Set[str], detc_home: str, go: go_conf,
			size_gb: int = 8, wounds: int = 4, low_shrink: int = 0,
			high_shrink: int = 0, port: int = 32232, cg: cgroup = None) -> None:
		super(detc, self).__init__(bakers, cg)
		self.detc_home: str = detc_home
		self.go: go_conf = go
		self.size_gb: int = size_gb
		self.wounds: int = wounds
		self.port: int = port
		self.low_shrink: int = low_shrink
		self.high_shrink: int = high_shrink

		self.procs: List[subprocess.Popen]
		self.logs: List[TextIO]

	def write_conf(self) -> None:
		with open(self.test_home + "/conf/" + self.name, "a") as conf_f:
			conf_f.write("type {}\n".format(type(self).__name__))
			conf_f.write("bakers {}\n".format(' '.join(self.bakers)))
			conf_f.write("test_home {}\n".format(self.test_home))
			conf_f.write("name {}\n".format(self.name))
			conf_f.write("detc_home {}\n".format(self.detc_home))
			conf_f.write("size_gb {}\n".format(self.size_gb))
			conf_f.write("wounds {}\n".format(self.wounds))
			conf_f.write("port {}\n".format(self.port))
			conf_f.write("test_log_dir {}\n".format(self.test_log_dir))
			conf_f.write("cgroup {}\n".format("None" if self.cg is None else self.cg.name))
			conf_f.write("low_shrink {}\n".format(self.low_shrink))
			conf_f.write("high_shrink {}\n".format(self.high_shrink))
			self.go.write_conf(conf_f)

	def prologue(self) -> None:
		print("running detc.prologue")
		os.mkdir(self.test_log_dir)
		"""
		base_cmd = []
		for k, v in self.go.args.items():
			base_cmd.append("export")
			base_cmd.append(k + '=' + v + ';')
		base_cmd.extend([ "cgexec", "-g",  self.cg.group,
			self.detc_home + "/detc-go",
			"-size", str(self.size_gb),
			"-wounds", str(self.wounds),
			"-port", str(self.port) ])

		self.procs = []
		self.logs = []
		for baker in self.bakers:
			cmd = [ "ssh", baker ]
			cmd.extend(base_cmd)
			log = open("{}/{}.log".format(self.test_log_dir, baker), 'w')
			self.logs.append(log)
			proc = subprocess.Popen(cmd, stdout=log, stderr=log)
			self.procs.append(proc)

		sleep(8) # give detc time to start
		"""

	def epilogue(self) -> None:
		print("running detc.epilogue")
		ssh_bakers(self.bakers, r'pkill -9 -f "detcdetc/markbench"')

	def clean(self) -> None:
		print("running detc.clean (noop)")

class memcached(application):
	def __init__(self, bakers: Set[str], memcached_home: str,
			size_gb: int = 8, port: int = 32232, sigve: bool = False, cg: cgroup = None) -> None:
		super(memcached, self).__init__(bakers, cg)
		self.memcached_home: str = memcached_home
		self.size_gb: int = size_gb
		self.port: int = port
		self.sigve: bool = sigve

		self.procs: List[subprocess.Popen]
		self.logs: List[TextIO]

	def write_conf(self) -> None:
		with open(self.test_home + "/conf/" + self.name, "a") as conf_f:
			conf_f.write("type {}\n".format(type(self).__name__))
			conf_f.write("bakers {}\n".format(' '.join(self.bakers)))
			conf_f.write("test_home {}\n".format(self.test_home))
			conf_f.write("name {}\n".format(self.name))
			conf_f.write("memcached_home {}\n".format(self.memcached_home))
			conf_f.write("size_gb {}\n".format(self.size_gb))
			conf_f.write("port {}\n".format(self.port))
			conf_f.write("sigve {}\n".format(self.sigve))
			conf_f.write("test_log_dir {}\n".format(self.test_log_dir))
			conf_f.write("cgroup {}\n".format("None" if self.cg is None else self.cg.name))

	def prologue(self) -> None:
		print("running memcached.prologue")
		os.mkdir(self.test_log_dir)

		base_cmd = [ "cgexec", "-g",  self.cg.group,
			"export", "LD_PRELOAD=/home/eurosys21/applications/jemalloc-5.2.1/lib/libjemalloc.so;",
			"export", "MALLOC_CONF=background_thread:true,dirty_decay_ms:0;",
			"/home/eurosys21/applications/memcached-1.6.7/bin/memcached",
			"-p", str(self.port),
			"-t", "8", "-m", str(self.size_gb * 1024) ]

		if self.sigve:
			base_cmd.append("-z")

		self.procs = []
		self.logs = []
		for baker in self.bakers:
			cmd = [ "ssh", baker ]
			cmd.extend(base_cmd)
			cmd.extend([ "-l", baker ])

			log_name = "{}/{}".format(self.test_log_dir, baker)
			outlog = open(log_name + "_stdout.log", 'w')
			errlog = open(log_name + "_stderr.log", 'w')
			self.logs.append(outlog)
			self.logs.append(errlog)

			proc = subprocess.Popen(cmd, stdout=outlog, stderr=errlog)
			self.procs.append(proc)

		sleep(8) # give memcached time to start

	def epilogue(self) -> None:
		print("running memcached.epilogue")
		ssh_bakers(self.bakers, r'pkill -9 -f "memcached-1.6.7/bin/memcached"')
		for proc in self.procs:
			proc.terminate()
		for log in self.logs:
			log.close()

	def clean(self) -> None:
		print("running memcached.clean")
		ssh_bakers(self.bakers, r'pkill -9 -f "memcached-1.6.7/bin/memcached"')

