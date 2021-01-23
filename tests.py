from apps import *
import sys
from typing import List, NoReturn, Any, Tuple, Set
from pathlib import Path
import copy
from enum import Enum

class config(Enum):
	smol_brain = 0
	big_brain = 1
	sigve = 2
	pure_default = 3
	global_optimal = 4

class benchmark:
	def __init__(self, bakers: Set[str], delay: int = 0) -> None:
		self.order: int
		self.name: str
		self.test_home: str
		self.bakers: Set[str] = bakers
		self.delay: int = delay
		self.apps: List[application] = []

	def write_conf(self) -> None:
		print("running benchmark.write_conf")
		with open(self.test_home + "/conf/" + self.name, "a") as conf_f:
			conf_f.write("type {}\n".format(type(self).__name__))
			conf_f.write("order {}\n".format(self.order))
			conf_f.write("name {}\n".format(self.name))
			conf_f.write("test_home {}\n".format(self.test_home))
			conf_f.write("apps {}\n".format(' '.join([ app.name for app in self.apps ])))
			conf_f.write("delay {}\n".format(self.delay))
	
	def run(self) -> List[Tuple[subprocess.Popen, TextIO, TextIO]]:
		raise NotImplementedError

	def prepare(self, test_home: str, order: int) -> None:
		self.test_home =  test_home
		self.order = order
		self.name = type(self).__name__ + str(order)
		for app in self.apps:
			if not app.prepare_done:
				app.prepare(self.test_home, self.order)
			app.prepare_done = True

class test:
	_self: "test" = None
	def __init__(self, test_home: str, conf: config, benchmarks: List[benchmark],
			timeout: int = 0) -> None:
		self.test_home: str = test_home
		self.conf: config = conf
		self.benchmarks: List[benchmark] = benchmarks
		self.bakers: Set[str] = set()
		for i, bm in enumerate(self.benchmarks):
			self.bakers.update(bm.bakers)
			bm.prepare(self.test_home, i)
		self.timeout: int = timeout
		self.alarm: bool = False
		self.daemons: List[daemon] = []
		os.mkdir(test_home)
		os.mkdir(test_home + "/conf")
		test._self = self

	def prologue(self) -> None:
		print("running test.prologue")
		for d in self.daemons:
			d.prologue()
			d.write_conf()
		for bm in self.benchmarks:
			for app in bm.apps:
				if not app.init_done:
					if app.cg and not app.cg.init_done:
						app.cg.prologue()
						app.cg.write_conf()
						app.cg.init_done = True
					app.prologue()
					app.write_conf()
					app.init_done = True
			bm.write_conf()
		self.write_conf()

	def epilogue(self) -> None:
		print("running test.epilogue")
		for bm in self.benchmarks:
			for app in bm.apps:
				if not app.epilogue_done:
					app.epilogue()
					app.epilogue_done = True
		for daemon in self.daemons:
			daemon.epilogue()

	def clean(self) -> None:
		print("running test.clean")
		for daemon in self.daemons:
			daemon.clean()
		for bm in self.benchmarks:
			for app in bm.apps:
				if not app.clean_done:
					app.clean()
					app.clean_done = True
		ssh_bakers(self.bakers, "/homes/liondavi/cluster-misc/bin/toogle_swap")

	def add_obs_daemon(self) -> None:
		self.obs = obs_daemon(self.bakers, self.test_home)
		self.daemons.append(self.obs)

	def add_sigve_daemon(self, conf: sigve_conf) -> None:
		self.sigve = sigve_daemon(self.bakers, self.test_home, conf)
		self.daemons.append(self.sigve)

	def run(self) -> None:
		print("running test.run")
		start = clock_gettime(CLOCK_REALTIME)
		bm_info: List[Tuple[subprocess.Popen, TextIO, TextIO]] = []
		for bm in self.benchmarks:
			sleep(bm.delay)
			bm_info.extend(bm.run())

		for proc, outlog, errlog in bm_info:
			proc.wait()
			if proc.returncode != 0:
				print("[warn] ret: {} for: {}".format(proc.returncode, proc.args))
			outlog.close()
			errlog.close()

		end = clock_gettime(CLOCK_REALTIME)

		with open(self.test_home + "/info", "a") as info_f:
			info_f.write("start {}\n".format(int(start * 1e9)))
			info_f.write("end {}\n".format(int(end * 1e9)))

	def write_conf(self) -> None:
		print("running test.write_conf")
		with open(self.test_home + "/conf/test", "a") as conf_f:
			conf_f.write("type {}\n".format(type(self).__name__))
			conf_f.write("bakers {}\n".format(' '.join(self.bakers)))
			conf_f.write("test_home {}\n".format(self.test_home))
			conf_f.write("conf {}\n".format(self.conf))
			conf_f.write("benchmarks {}\n".format(' '.join([ bm.name for bm in self.benchmarks ])))

	@staticmethod
	def feelssignalman(signum, frame) -> NoReturn:
		print("running test.feelssignalman")
		test._self.epilogue()
		test._self.clean()
		if signum != signal.SIGALRM:
			sys.exit(1)

class test_runner:
	@staticmethod
	def run(t: test) -> int:
		# signal handlers for cleanup
		signal.signal(signal.SIGINT, t.feelssignalman)
		signal.signal(signal.SIGTERM, t.feelssignalman)
		def handle_alarm(signum, frame):
			t.alarm = True
			t.feelssignalman(signum, frame)
		signal.signal(signal.SIGALRM, handle_alarm)
		signal.alarm(t.timeout)

		t.clean()
		t.prologue()

		t.run()
		if t.alarm:
			return t.alarm

		t.epilogue()
		t.clean()

		return t.alarm

	@staticmethod
	def next_test_num(path) -> int:
		if os.path.exists(path):
			nums = [ int(d.split("test-")[1]) for d in os.listdir(path) if d.startswith("test-") ]
			if nums:
				return max(nums) + 1
			else:
				return 0
		else:
			return 0

	@staticmethod
	def run_1time(base_path: str, conf: config, benchmarks: List[benchmark],
			timeout: int = 45 * 60, _sigve_conf: sigve_conf = None) -> int:
			#timeout: int = 90 * 60, _sigve_conf: sigve_conf = None) -> int:
		i = test_runner.next_test_num(base_path)
		if i == 0 and not os.path.exists(base_path):
			os.mkdir(base_path)
		_test = test("{}/test-{}".format(base_path, i), conf, benchmarks, timeout)
		_test.add_obs_daemon()
		if _sigve_conf:
			_test.add_sigve_daemon(_sigve_conf)
		print("== running {}".format(_test.test_home))
		if test_runner.run(_test):
			print("[error] {} timeout".format(_test.test_home))
			Path(_test.test_home + "/timeout").touch()
			return 1
		return 0

class hibench_stress(benchmark):
	def __init__(self, bakers: Set[str], hibench: hibench_spark, delay: int = 0) -> None:
		super(hibench_stress, self).__init__(bakers, delay)
		self.hibench = hibench
		self.apps.append(self.hibench)

	def run(self) -> List[Tuple[subprocess.Popen, TextIO, TextIO]]:
		print("running {}.run".format(self.name))
		env = os.environ.copy()
		env["HIBENCH_CONF_FOLDER"] = self.hibench.conf_dir

		outlog = open(self.hibench.test_log_dir + "/stdout.log", 'w')
		errlog = open(self.hibench.test_log_dir + "/stderr.log", 'w')

		proc = subprocess.Popen([
			"{}/bin/workloads/{}/spark/run.sh".format(self.hibench.hibench_home, self.hibench.workload)
		], stdout=outlog, stderr=errlog, env=env)

		return [ (proc, outlog, errlog) ]

class detc_stress(benchmark):
	def __init__(self, bakers: Set[str], _detc: detc, delay: int = 0,
			clients: int = 16, requests: int = 4 * 1000 * 1000,
			keys: int = 10 * 1000 * 1000, cores: int = 5 , port: int = 32232) -> None:
		super(detc_stress, self).__init__(bakers, delay)
		self._detc = _detc
		self.apps.append(self._detc)
		self.clients: int = clients
		self.requests: int = requests
		self.keys: int = keys
		self.cores: int = cores
		self.port: int = port
	
	def write_conf(self) -> None:
		print("running detc_stress.write_conf")
		super(detc_stress, self).write_conf()
		with open(self.test_home + "/conf/" + self.name, "a") as conf_f:
			conf_f.write("clients {}\n".format(self.clients))
			conf_f.write("requests {}\n".format(self.requests))
			conf_f.write("keys {}\n".format(self.keys))
			conf_f.write("cores {}\n".format(self.cores))
			conf_f.write("server port {}\n".format(self.port))

	def run(self) -> List[Tuple[subprocess.Popen, TextIO, TextIO]]:
		print("running {}.run".format(self.name))

		info: List[Tuple[subprocess.Popen, TextIO, TextIO]] = []

		"""
		base_cmd: List[str] = [
			"export", "GOMAXPROCS" + '=' + str(self.cores) + ';',
			"{}/benchmark".format(self._detc.detc_home),
			"-clients", str(self.clients),
			"-requests", str(self.requests),
			"-keys", str(self.keys),
			"-size", str(self._detc.size_gb),
		]
		"""

		base_cmd = []
		for k, v in self._detc.go.args.items():
			base_cmd.append("export")
			base_cmd.append(k + '=' + v + ';')
		base_cmd.extend([ "cgexec", "-g",  self._detc.cg.group,
			"{}/markbench".format(self._detc.detc_home),
			"-clients", str(self.clients),
			"-requests", str(self.requests),
			"-keys", str(self.keys),
			"-size", str(self._detc.size_gb),
			"-lowpercent", str(self._detc.low_shrink),
			"-highpercent", str(self._detc.high_shrink),
			"-wounds", str(self._detc.wounds),
		])

		for baker in self.bakers:
			log_name = "{}/{}_{}".format(self._detc.test_log_dir, self.name, baker)

			outlog = open(log_name + "_stdout.log", 'w')
			errlog = open(log_name + "_stderr.log", 'w')

			cmd = [ "ssh", baker ]
			cmd.extend(base_cmd)
			#cmd.extend([ "-hosts", baker + ":" + str(self.port) ])
			proc = subprocess.Popen(cmd, stdout=outlog, stderr=errlog)

			info.append((proc, outlog, errlog))

		return info

class memcached_stress(benchmark):
	def __init__(self, bakers: Set[str], _memcached: memcached, delay: int = 0,
			requests: int = 1 * 1000 * 1000,
			keys: int = 64 * 1000 * 1000, port: int = 32232) -> None:
		super(memcached_stress, self).__init__(bakers, delay)
		self._memcached = _memcached
		self.apps.append(self._memcached)
		self.requests: int = requests
		self.keys: int = keys
		self.port: int = port
	
	def write_conf(self) -> None:
		print("running memcached_stress.write_conf")
		super(memcached_stress, self).write_conf()
		with open(self.test_home + "/conf/" + self.name, "a") as conf_f:
			conf_f.write("requests {}\n".format(self.requests))
			conf_f.write("keys {}\n".format(self.keys))
			conf_f.write("server port {}\n".format(self.port))

	def run(self) -> List[Tuple[subprocess.Popen, TextIO, TextIO]]:
		print("running {}.run".format(self.name))

		info: List[Tuple[subprocess.Popen, TextIO, TextIO]] = []

		base_cmd: List[str] = [
			"/home/eurosys21/applications/memtier_benchmark-1.3.0/memtier_benchmark",
			"-s", "baker10", "-p", str(self.port), "-P", "memcache_binary",
			"-d", "2048", "-t", "12", "-c", "8"
		]

		cmd = base_cmd + [ "--key-maximum=" + str(self.keys), "-n", str(self.requests),
			"--ratio=1:0" ]
		log_name = "{}/{}0_{}".format(self._memcached.test_log_dir, self.name, "baker10")
		outlog = open(log_name + "_stdout.log", 'w')
		errlog = open(log_name + "_stderr.log", 'w')
		proc0 = subprocess.run(cmd, stdout=outlog, stderr=errlog)
		if proc0.returncode != 0:
			print("[warn] ret: {} for: {}".format(proc0.returncode, proc0.args))
		outlog.close()
		errlog.close()

		cmd = base_cmd + [ "--key-maximum=" + str(self.keys), "-n", str(self.requests),
			"--ratio=1:10" ]
		log_name = "{}/{}1_{}".format(self._memcached.test_log_dir, self.name, "baker10")
		outlog = open(log_name + "_stdout.log", 'w')
		errlog = open(log_name + "_stderr.log", 'w')
		proc1 = subprocess.Popen(cmd, stdout=outlog, stderr=errlog)

		info.append((proc1, outlog, errlog))

		return info
