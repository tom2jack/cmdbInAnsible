# -*- coding:utf-8 -*-
import json
from collections import namedtuple
from ansible.parsing.dataloader import DataLoader
from ansible.vars import VariableManager
from ansible.inventory import Inventory
from ansible.playbook.play import Play
from ansible.executor.task_queue_manager import TaskQueueManager
from ansible.executor.task_result import TaskResult
from ansible.plugins.callback import CallbackBase
import MySQLdb

def insert_DB(sql, args):
    db = MySQLdb.connect("mysql_ip","user","password","db")
    cursor = db.cursor()
    try:
    	cursor.execute(sql % args)
        db.commit()
    except:
        db.rollback()
    db.close()


# 自定义 callback，即在运行 api 后调用本类中的 v2_runner_on_ok()，在这里会输出 host 和 result 格式
class ResultCallback(CallbackBase):
    def v2_runner_on_ok(self, result, **kwargs):
        # result 包含'_check_key', '_host', '_result', '_task', 'is_changed', 'is_failed', 'is_skipped', 'is_unreachable'
        host = result._host
        #print json.dumps({host.name: result._result}, indent=4)
	#print type(result._result['ansible_facts'])
	dict_facts = result._result['ansible_facts']


	sql_insert_host = 'insert into t_hostinfo (nodename, form_factor, virtualization_role, virtualization_type, system_vendor, product_name, product_serial, machine, bios_version, system, os_family, distribution, distribution_major_version, distribution_release, distribution_version, architecture, kernel, userspace_architecture, userspace_bits, pkg_mgr, selinux_status, processor, processor_count, processor_cores, processor_threads_per_core, processor_vcpus, memtotal, swaptotal, fqdn, default_ipv4_interface, default_ipv4_address, default_ipv4_gateway) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%d", "%s", "%s", "%s", "%s", "%s", "%d", "%s", "%s", "%s", "%d", "%d", "%d", "%d", "%d", "%d", "%s", "%s", "%s", "%s")'

	nodename = dict_facts['ansible_nodename']
	print '---- General ----'
	form_factor = dict_facts['ansible_form_factor']
	
	print dict_facts['ansible_nodename']
	virtualization_role =  dict_facts['ansible_virtualization_role']
	virtualization_type =  dict_facts['ansible_virtualization_type']
	print '---- Hardware ----'
	system_vendor =  dict_facts['ansible_system_vendor']
	product_name =  dict_facts['ansible_product_name']
	product_serial =  dict_facts['ansible_product_serial']
	machine =  dict_facts['ansible_machine']
	bios_version = dict_facts['ansible_bios_version']
	print '---- OS ----'
	system = dict_facts['ansible_system']
	os_family = dict_facts['ansible_os_family']
	distribution = dict_facts['ansible_distribution']
	try:
		distribution_major_version = int(dict_facts['ansible_distribution_major_version'])
	except Exception as e:
		distribution_major_version = 0
	distribution_release = dict_facts['ansible_distribution_release']
	distribution_version = dict_facts['ansible_distribution_version']
	architecture = dict_facts['ansible_architecture']
	kernel = dict_facts['ansible_kernel']
	userspace_architecture = dict_facts['ansible_userspace_architecture']
	try:
		userspace_bits = int(dict_facts['ansible_userspace_bits'])
	except Exception as e:
		userspace_bits = 0
	
	pkg_mgr = dict_facts['ansible_pkg_mgr']
	selinux_status = dict_facts['ansible_selinux']['status']
	print '---- CPU ----'
	processor = dict_facts['ansible_processor'][1]
	try:
		processor_count = int(dict_facts['ansible_processor_count'])
		processor_cores = int(dict_facts['ansible_processor_cores'])
		processor_threads_per_core = int(dict_facts['ansible_processor_threads_per_core'])
		processor_vcpus = int(dict_facts['ansible_processor_vcpus'])
	except Exception as e:
		processor_count = 0
		processor_cores = 0
		processor_threads_per_core = 0
		processor_vcpus = 0


	print '---- MEM (MB) ----'
	try:
		memtotal = int(dict_facts['ansible_memtotal_mb'])/1024
	except Exception as e:
		memtotal = 0
	print '---- swap (MB) ----'
	try:
		swaptotal = int(dict_facts['ansible_swaptotal_mb'])/1024
	except Exception as e:
		swaptotal = 0
	print '---- network interface ----'
	fqdn = dict_facts['ansible_fqdn']
	default_ipv4_interface = dict_facts['ansible_default_ipv4']['interface']
	default_ipv4_address = dict_facts['ansible_default_ipv4']['address']
	default_ipv4_gateway = dict_facts['ansible_default_ipv4']['gateway']
	args_host = (nodename, form_factor, virtualization_role, virtualization_type, system_vendor, product_name, product_serial, machine, bios_version, system, os_family, distribution, distribution_major_version, distribution_release, distribution_version, architecture, kernel, userspace_architecture, userspace_bits, pkg_mgr, selinux_status, processor, processor_count, processor_cores, processor_threads_per_core, processor_vcpus, memtotal, swaptotal, fqdn, default_ipv4_interface, default_ipv4_address, default_ipv4_gateway)
	print args_host
	insert_DB(sql_insert_host, args_host)
	print sql_insert_host % args_host
		
	print '---- DISK ----'
	sql_insert_disk = 'insert into t_disk(devices, vendor, model, host, size, nodename) VALUES ("%s", "%s", "%s", "%s", "%d", "%s")'
	for k,v in dict_facts['ansible_devices'].items():
		print k
		print v['vendor']
		print v['model']
		print v['host']
		print v['size']
		devices = k
		vendor = v['vendor']
		model = v['model']
		host = v['host']
		size_tmp = v['size']
		s = size_tmp.split(' ')
		print s
		if s[1] == 'TB':
			size = float(s[0]) * 1024
		elif s[1] == 'GB':
			size = float(s[0])
		elif s[1] == 'MB':
			size = float(float(s[0])/1024)
		else:
			size = 0
		
		args = (devices, vendor, model, host, size, nodename)
		insert_DB(sql_insert_disk, args)
		#print size
	

	
	sql_insert_interface = 'insert into t_interface(nodename, interface, ipv4, ipv6, macAddress) VALUES ("%s", "%s", "%s", "%s", "%s")'
	network_cards = dict_facts['ansible_interfaces']
	try:
		network_cards.remove('lo')
	except Exception as e:
		print e
	for i in network_cards:
		print i
		interface = i
		try:
			ipv4 = dict_facts["ansible_"+str(i)]['ipv4']['address']
			print ipv4
		except Exception as e:
			print e
			ipv4 = ''
		try:
			ipv6 = dict_facts["ansible_"+str(i)]['ipv6'][0]['address']
			print ipv6
		except Exception as e:
			print e
			ipv6 = ''
		mac = dict_facts["ansible_"+str(i)]['macaddress']
		print mac
		args = (nodename, interface, ipv4, ipv6, mac)
		insert_DB(sql_insert_interface, args)

		
        print '******************************************************************'
	#sql_insert = 'insert into t_hostinfo '
            
Options = namedtuple('Options', ['connection', 'module_path', 'forks', 'become', 'become_method', 'become_user', 'check'])
# initialize needed objects
variable_manager = VariableManager()
loader = DataLoader()
options = Options(connection='smart', module_path=None, forks=100, become=None, become_method=None, become_user=None, check=False)
passwords = dict(conn_pass='')  # 目前只发现有两个key，conn_pass, become_pass

# Instantiate our ResultCallback for handling results as they come in
results_callback = ResultCallback()

# create inventory and pass to var manager
inventory = Inventory(loader=loader, variable_manager=variable_manager, host_list='/etc/ansible/hosts')  
# hosts文件，也可以是 ip列表 '10.1.162.18:322, 10.1.167.36' 或者 ['10.1.162.18:322', '10.1.167.36']
variable_manager.set_inventory(inventory)

# create play with tasks
play_source =  dict(
        name = "Ansible Play",
        hosts = 'all',   # 对应 playbook 入口yaml文件的 hosts变量，也可以是 ip 10.1.162.18
        gather_facts = 'no',
        tasks = [
            #dict(action=dict(module='setup', args='filter=ansible_all_ipv4_addresses'), register='shell_out'),
            dict(action=dict(module='setup', args=''), register='shell_out'),
            #dict(action=dict(module='debug', args=dict(msg='{{shell_out.stdout}}')))
         ]
    )
play = Play().load(play_source, variable_manager=variable_manager, loader=loader)

# actually run it
# TaskQueueManager 是创建进程池，负责输出结果和多进程间数据结构或者队列的共享协作
tqm = None
try:
    tqm = TaskQueueManager(
              inventory=inventory,
              variable_manager=variable_manager,
              loader=loader,
              options=options,
              passwords=passwords,
              stdout_callback=results_callback,  # Use our custom callback instead of the ``default`` callback plugin
              # 如果注释掉 callback 则会调用原生的 DEFAULT_STDOUT_CALLBACK，输出 task result的output，同 ansible-playbook debug
          )
    result = tqm.run(play)
    print '-------------------------------------------------------------------------------'  # 返回码，只要有一个 host 出错就会返回 非0 数字
    print result  # 返回码，只要有一个 host 出错就会返回 非0 数字
finally:
    if tqm is not None:
        tqm.cleanup()
