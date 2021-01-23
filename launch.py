#!/usr/bin/env python3

from apps import *
from tests import *
import sys
from typing import Dict, Tuple, Union, cast
import copy

java_home = "/home/eurosys21/jvms/java_home"
hibench_home = "/home/eurosys21/applications/HiBench"
spark_home = "/home/eurosys21/applications/spark-2.3.2-bin-hadoop2.7"
detc_home = "/home/eurosys21/applications/detc"
memcached_home = "/home/eurosys21/applications/memcached-1.6.7"
bakers = set([ "baker" + str(i) for i in range(10, 17 + 1) ])

def minutes(m: int) -> int:
	return m * 60

def _jvm_conf(heap_size: str, sigve: bool = False, jvm_args: List[str] = None) -> jvm_conf:
	args = [ "-XX:+UseG1GC", "-XX:+UseSIGVEPidFile" ]
	args.append("-XX:+PrintGCApplicationStoppedTime")
	if sigve:
		args.append("-XX:+UseSIGVE")
		args.append("-XX:+ExplicitGCInvokesConcurrent")
		args.append("-XX:SIGVECost=25")
	if jvm_args:
		args.extend(jvm_args)
	return jvm_conf(java_home, args = args).heap(heap_size)

class spark_params:
	def __init__(self, heap_size: int, workload: str = "ml/kmeans", scale: str = None,
			mem_frac: float = -1, mem_storage_frac: float = -1,
			sigve: bool = False, sigve_n: int = -1, sigve_f: float = -1) -> None:
		self.heap_size: int = heap_size
		self.mem_frac: float = mem_frac
		self.mem_storage_frac: float = mem_storage_frac
		self.workload: str = workload
		self.scale: str = scale
		if scale == None:
			if self.workload == "ml/kmeans":
				self.scale = "gigantic"
			elif self.workload == "websearch/pagerank":
				#self.scale = "hugeplus"
				self.scale = "hugeplusplus"
			elif self.workload == "graph/nweight":
				#self.scale = "largeplus"
				self.scale = "largeplus_p120_"
			else:
				print("[error] bogus workload: " + self.workload)
				assert(False)
		self.sigve: bool = sigve
		self.sigve_n: int = sigve_n
		self.sigve_f: float = sigve_f

class detc_params:
	def __init__(self, size: int, wounds: int = 5, clients: int = 5,
			requests: int = 13 * 100 * 1000, keys: int = 12 * 1000 * 1000,
			cores: int = 5, gc: int = 100, port: int = 32232,
			low_shrink: int = 0, high_shrink: int = 6) -> None:
		self.size: int = size
		self.wounds: int = wounds
		self.clients: int = clients
		self.requests: int = requests
		self.keys: int = keys
		self.cores: int = cores
		self.gc: int = gc
		self.port: int = port
		self.low_shrink: int = low_shrink
		self.high_shrink: int = high_shrink

class memcached_params:
	def __init__(self, size: int, requests: int = 13 * 100 * 1000, keys: int = 12 * 1000 * 1000,
			port: int = 32232) -> None:
		self.size: int = size
		self.requests: int = requests
		self.keys: int = keys
		self.port: int = port
		self.sigve: bool = False

def init_spark_old(conf: config, sps: List[spark_params], jvm_args: List[str] = None) -> List[jvm_conf]:
	count: Dict[str, int] = {}
	jvms: List[jvm_conf] = []
	for sp in sps:
		if sp.workload not in count:
			count[sp.workload] = 0
		sp.scale += str(count[sp.workload])
		count[sp.workload] += 1

		if conf == config.sigve:
			jvm = _jvm_conf("64G", True, jvm_args)
		else:
			jvm = _jvm_conf(str(sp.heap_size) + "G", False)
		jvms.append(jvm)
	return jvms

def init_spark(conf: config, sp: spark_params, jvm_args: List[str] = None) -> jvm_conf:
	if conf == config.sigve:
		jvm = _jvm_conf(str(sp.heap_size) + "G", True, jvm_args)
	else:
		jvm = _jvm_conf(str(sp.heap_size) + "G", False)
	return jvm

def init_detc(conf: config, dp: detc_params) -> go_conf:
	_go_conf: Dict[str, str] = {
		"GOMAXPROCS": str(5),
	}
	if conf == config.sigve:
		go = go_conf(_go_conf)
		go.sigve()
	else:
		if conf != config.pure_default:
			_go_conf["GOGC"] = str(dp.gc)
		go = go_conf(_go_conf)
	return go

def init_global(conf: config, cgroup_mem: str = "64g") -> Tuple[cgroup, sigve_conf]:
	cg = cgroup(bakers, "memory:thermostat", cgroup_mem)
	sc: sigve_conf = None
	if conf == config.sigve:
		if cgroup_mem == "64g":
			sc = sigve_conf(java_home)
		else:
			sc = sigve_conf(java_home,
				top = str(7 * 1024 + 512) + "m",
				low_wm_init = str(6 * 1024 + 512) + "m",
				high_wm_init = "7g")
	return cg, sc

def init_params(conf: config, params: List[Union[spark_params, detc_params, memcached_params]]) -> List[Union[jvm_conf, go_conf]]:
	runtimes: List[Union[jvm_conf, go_conf]] = []
	spark_count: Dict[str, int] = {}
	port_count: int = 0
	for param in params:
		if isinstance(param, spark_params):
			if param.workload not in spark_count:
				spark_count[param.workload] = 0
			param.scale += str(spark_count[param.workload])
			spark_count[param.workload] += 1
			runtimes.append(init_spark(conf, param))
		elif isinstance(param, detc_params):
			param.port += port_count
			port_count += 1
			runtimes.append(init_detc(conf, param))
		elif isinstance(param, memcached_params):
			param.port += port_count
			port_count += 1
			if conf == config.sigve:
				param.sigve = True
			runtimes.append(None)
		else:
			print("[error] init_param invalid param type... {}".format(param))
			sys.exit(1)
	return runtimes

def workload_n(conf: config, params: List[Union[spark_params, detc_params, memcached_params]], delay: int = 0, path: str = None, cgroup_mem: str = "64g") -> None:
	cg, sc = init_global(conf, cgroup_mem)
	if sc != None and path != None and "hightop" in path:
		sc.top = 64 * 1024 * 1024 * 1024
	if sc != None and path != None and "nokill" in path:
		sc.kill_time = 3 * 3600 * 1000
	if sc != None and path != None and "static" in path:
		sc.wm_increment_percent = 0
		sc.low_wm_init = 40 * 1024 * 1024 * 1024
		sc.high_wm_init = 45 * 1024 * 1024 * 1024
	if sc != None and path != None and "dynamic" in path:
		sc.low_wm_init = 40 * 1024 * 1024 * 1024
		sc.high_wm_init = 45 * 1024 * 1024 * 1024
	runtimes = init_params(conf, params)

	apps: List[application] = []
	for param, runtime in zip(params, runtimes):
		if isinstance(param, spark_params):
			if path != None and "util-smolbrain" in path and len(apps) == 2:
				apps.append(hibench_spark(bakers, hibench_home, spark_home, cast(jvm_conf, runtime),
					scale = param.scale, workload = param.workload,
					max_cores = 5 * 8, cores = 5,
					mem_frac = param.mem_frac, mem_storage_frac = param.mem_storage_frac,
					cg = None))
			else:
				if cgroup_mem == "64g":
					max_cores = 5 * 8
					cores = 5
				else:
					max_cores = 8
					cores = 8
				apps.append(hibench_spark(bakers, hibench_home, spark_home, cast(jvm_conf, runtime),
					scale = param.scale, workload = param.workload,
					max_cores = max_cores, cores = cores,
					mem_frac = param.mem_frac, mem_storage_frac = param.mem_storage_frac,
					sigve = param.sigve, sigve_n = param.sigve_n, sigve_f = param.sigve_f,
					cg = cg))
		elif isinstance(param, detc_params):
			apps.append(detc(bakers, detc_home, cast(go_conf, runtime), param.size, param.wounds, param.low_shrink, param.high_shrink, param.port, cg))
		elif isinstance(param, memcached_params):
			apps.append(memcached(bakers, memcached_home, param.size, param.port, param.sigve, cg))
		else:
			print("[error] workload_n apps invalid param type... {}".format(param))
			sys.exit(1)

	stresses: List[benchmark] = []
	for i, app_param in enumerate(zip(apps, params)):
		app, param = app_param
		if isinstance(app, hibench_spark):
			stresses.append(hibench_stress(bakers, cast(hibench_spark, app), 0 if i == 0 else delay))
		elif isinstance(app, detc):
			dp = cast(detc_params, param)
			stresses.append(detc_stress(bakers, cast(detc, app), 0 if i == 0 else delay, dp.clients, dp.requests, dp.keys, dp.cores, dp.port))
		elif isinstance(app, memcached):
			mcp = cast(memcached_params, param)
			stresses.append(memcached_stress(bakers, cast(memcached, app), 0 if i == 0 else delay, mcp.requests, mcp.keys, mcp.port))
		else:
			print("[error] workload_n stresses invalid param type... {}".format(param))
			sys.exit(1)

	test_runner.run_1time(path if path else sys.argv[1], conf, stresses, _sigve_conf = sc)

def run_global_optimal(prefix: str, count: int = 1) -> None:
	nw = lambda: spark_params(24, "graph/nweight", mem_frac = 0.5, mem_storage_frac = 0.9)
	detc = lambda: detc_params(10, gc = 5)
	km = lambda: spark_params(14, "ml/kmeans", mem_frac = 0.7, mem_storage_frac = 0.9)
	pr = lambda: spark_params(14, "websearch/pagerank", mem_frac = 0.7, mem_storage_frac = 0.9)

	for i in range(count):
		##WW0
		workload_n(config.global_optimal,
			[ nw(), nw() ],
			0, prefix + "-globaloptimal-WW0")
		##CCC0
		workload_n(config.global_optimal,
			[ detc(), detc(), detc() ],
			0, prefix + "-globaloptimal-CCC0")
		##PPP0
		workload_n(config.global_optimal,
			[ pr(), pr(), pr() ],
			0, prefix + "-globaloptimal-PPP0")
		##MMM0
		workload_n(config.global_optimal,
			[ km(), km(), km() ],
			0, prefix + "-globaloptimal-MMM0")

		##MMM180
		workload_n(config.global_optimal,
			[ km(), km(), km() ],
			180, prefix + "-globaloptimal-MMM180")
		##MMW180
		workload_n(config.global_optimal,
			[ km(), km(), nw() ],
			180, prefix + "-globaloptimal-MMW180")
		##WMM300
		workload_n(config.global_optimal,
			[ nw(), km(), km() ],
			300, prefix + "-globaloptimal-WMM300")
		##MCM180
		workload_n(config.global_optimal,
			[ km(), detc(), km() ],
			180, prefix + "-globaloptimal-MCM180")
		##CPW180
		workload_n(config.global_optimal,
			[ detc(), pr(), nw() ],
			180, prefix + "-globaloptimal-CPW180")
		##WPM180
		workload_n(config.global_optimal,
			[ nw(), pr(), km() ],
			180, prefix + "-globaloptimal-WPM180")
		##CWM180
		workload_n(config.global_optimal,
			[ detc(), nw(), km() ],
			180, prefix + "-globaloptimal-CWM180")
		##CMW180
		workload_n(config.global_optimal,
			[ detc(), km(), nw() ],
			180, prefix + "-globaloptimal-CMW180")
		##WMP240
		workload_n(config.global_optimal,
			[ nw(), km(), pr() ],
			240, prefix + "-globaloptimal-WMP240")
		##CCC480
		workload_n(config.global_optimal,
			[ detc(), detc(), detc() ],
			480, prefix + "-globaloptimal-CCC480")
		##CCW300
		workload_n(config.global_optimal,
			[ detc(), detc(), nw() ],
			300, prefix + "-globaloptimal-CCW300")
		##MWP180
		workload_n(config.global_optimal,
			[ km(), nw(), pr() ],
			180, prefix + "-globaloptimal-MWP180")

def run_default(prefix: str, count: int = 1) -> None:
	nw = lambda: spark_params(16, "graph/nweight")
	detc = lambda: detc_params(10)
	km = lambda: spark_params(16, "ml/kmeans")
	pr = lambda: spark_params(16, "websearch/pagerank")

	for i in range(count):
		##PPP0
		workload_n(config.pure_default,
				[ pr(), pr(), pr() ],
				0, prefix + "-default-PPP0")
		##MMM0
		workload_n(config.pure_default,
				[ km(), km(), km() ],
				0, prefix + "-default-MMM0")
		##CCC0
		workload_n(config.pure_default,
				[ detc(), detc(), detc() ],
				0, prefix + "-default-CCC0")

		##MMM180
		workload_n(config.pure_default,
				[ km(), km(), km() ],
				180, prefix + "-default-MMM180")
		##MCM180
		workload_n(config.pure_default,
				[ km(), detc(), km() ],
				180, prefix + "-default-MCM180")
		##CCC480
		workload_n(config.pure_default,
				[ detc(), detc(), detc() ],
				480, prefix + "-default-CCC480")

def run_oracle(conf: config, prefix: str, count: int = 1) -> None:
	detc = lambda x: detc_params(x, gc = 5)
	if conf == config.big_brain:
		km = lambda x: spark_params(x, "ml/kmeans", mem_frac = 0.7, mem_storage_frac = 0.9)
		nw = lambda x: spark_params(x, "graph/nweight", mem_frac = 0.5, mem_storage_frac = 0.9)
		pr = lambda x: spark_params(x, "websearch/pagerank", mem_frac = 0.7, mem_storage_frac = 0.9)
		path = "oracle-spark"
	else:
		km = lambda x: spark_params(x, "ml/kmeans")
		nw = lambda x: spark_params(x, "graph/nweight")
		pr = lambda x: spark_params(x, "websearch/pagerank")
		path = "oracle"

	for i in range(count):
		##WW0
		workload_n(conf,
			[ nw(27), nw(27) ],
			0, prefix + "-" + path + "-WW0")
		##CCC0
		workload_n(conf,
			[ detc(16), detc(16), detc(16) ],
			0, prefix + "-" + path + "-CCC0")
		##PPP0
		workload_n(conf,
			[ pr(18), pr(18), pr(18) ],
			0, prefix + "-" + path + "-PPP0")
		##MMM0
		workload_n(conf,
			[ km(18), km(18), km(18) ],
			0, prefix + "-" + path + "-MMM0")

		##MMM180
		workload_n(conf,
			[ km(18), km(18), km(18) ],
			180, prefix + "-" + path + "-MMM180")
		##MMW180
		workload_n(conf,
			[ km(16), km(16), nw(24) ],
			180, prefix + "-" + path + "-MMW180")
		##WMM300
		workload_n(conf,
			[ nw(24), km(16), km(16) ],
			300, prefix + "-" + path + "-WMM300")
		##MCM180
		workload_n(conf,
			[ km(20), detc(13), km(20) ],
			180, prefix + "-" + path + "-MCM180")
		##CPW180
		workload_n(conf,
			[ detc(13), pr(16), nw(24) ],
			180, prefix + "-" + path + "-CPW180")
		##WPM180
		workload_n(conf,
			[ nw(24), pr(16), km(14) ],
			180, prefix + "-" + path + "-WPM180")
		##CWM180
		workload_n(conf,
			[ detc(11), nw(24), km(17) ],
			180, prefix + "-" + path + "-CWM180")
		##CMW180
		workload_n(conf,
			[ detc(11), km(18), nw(24) ],
			180, prefix + "-" + path + "-CMW180")
		##WMMP240
		workload_n(conf,
			[ nw(24), km(14), pr(16) ],
			240, prefix + "-" + path + "-WMP240")
		##CCC480
		workload_n(conf,
			[ detc(16), detc(16), detc(16) ],
			480, prefix + "-" + path + "-CCC480")
		##CCW300
		workload_n(conf,
			[ detc(14), detc(14), nw(24) ],
			300, prefix + "-" + path + "-CCW300")
		##MWP180
		workload_n(conf,
			[ km(14), nw(24), pr(16) ],
			180, prefix + "-" + path + "-MWP180")

def run_m3(prefix: str, count: int = 1) -> None:
	nw = lambda: spark_params(64, "graph/nweight")
	detc = lambda: detc_params(64)
	km = lambda: spark_params(64, "ml/kmeans")
	pr = lambda: spark_params(64, "websearch/pagerank")

	for i in range(count):
		##WW0
		workload_n(config.sigve,
			[ nw(), nw() ],
			0, prefix + "-m3-WW0")
		##CCC0
		workload_n(config.sigve,
			[ detc(), detc(), detc() ],
			0, prefix + "-m3-CCC0")
		##PPP0
		workload_n(config.sigve,
			[ pr(), pr(), pr() ],
			0, prefix + "-m3-PPP0")
		##MMM0
		workload_n(config.sigve,
			[ km(), km(), km() ],
			0, prefix + "-m3-MMM0")

		##MMM180
		workload_n(config.sigve,
			[ km(), km(), km() ],
			180, prefix + "-m3-MMM180")
		##MMW180
		workload_n(config.sigve,
			[ km(), km(), nw() ],
			180, prefix + "-m3-MMW180")
		##WMM300
		workload_n(config.sigve,
			[ nw(), km(), km() ],
			300, prefix + "-m3-WMM300")
		##MCM180
		workload_n(config.sigve,
			[ km(), detc(), km() ],
			180, prefix + "-m3-MCM180")
		##CPW180
		workload_n(config.sigve,
			[ detc(), pr(), nw() ],
			180, prefix + "-m3-CPW180")
		##WPM180
		workload_n(config.sigve,
			[ nw(), pr(), km() ],
			180, prefix + "-m3-WPM180")
		##CWM180
		workload_n(config.sigve,
			[ detc(), nw(), km() ],
			180, prefix + "-m3-CWM180")
		##CMW180
		workload_n(config.sigve,
			[ detc(), km(), nw() ],
			180, prefix + "-m3-CMW180")
		##WMP240
		workload_n(config.sigve,
			[ nw(), km(), pr() ],
			240, prefix + "-m3-WMP240")
		##CCC480
		workload_n(config.sigve,
			[ detc(), detc(), detc() ],
			480, prefix + "-m3-CCC480")
		##CCW300
		workload_n(config.sigve,
			[ detc(), detc(), nw() ],
			300, prefix + "-m3-CCW300")
		##MWP180
		workload_n(config.sigve,
			[ km(), nw(), pr() ],
			180, prefix + "-m3-MWP180")

def memcached_workload(prefix: str, count: int = 1) -> None:
	global bakers
	bakers = set([ "baker10" ])

	km = lambda x, y: spark_params(x, "ml/kmeans", "large", sigve = y)

	for i in range(count):
		workload_n(config.smol_brain,
			[ km(4, False), memcached_params(4) ],
			240, prefix + "-memecached-vanilla", "8g")

		workload_n(config.sigve,
			[ km(8, True), memcached_params(8) ],
			240, prefix + "-memecached-m3", "8g")

def main() -> None:
	# Each of these functions will run a set of workloads with a certain configuration.
	# The string argument ("artifact") is a prefix for the directory the tests will be saved in.
	# The integer argument (1) is the number of runs to perform.
	# As is, this will run all benchmarks for the main results (Figure 5 and 8) once.
	run_default("artifact", 1)

	run_m3("artifact", 1)
	run_oracle(config.big_brain, "artifact", 1) # oracle with spark conf
	run_oracle(config.smol_brain, "artifact", 1) # oracle without spark conf
	run_global_optimal("artifact", 1)
	run_default("artifact", 1)

	# In order to run this the Spark cluster must be restarted with only one worker.
	# Comment all workers except "baker10" in "~/applications/spark-2.3.2-bin-hadoop2.7/conf/slaves"
	#memcached_workload("artifact", 1)

if __name__ == "__main__":
	main()

