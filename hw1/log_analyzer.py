# -*- coding: utf-8 -*-

from os import listdir, makedirs
from os.path import isfile, join, exists, getmtime, split
import gzip
from getopt import getopt
import shlex
import sys
import errno
import logging
from collections import namedtuple
import datetime

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
	"LOG_FILE" : None,
	"LOG_LEVEL": logging.ERROR,
	"TS_FILE" : './log_analyzer.ts'
}

LOG_NAME_PREFIX = 'nginx-access-ui.log-'

LogInfo = namedtuple('LogInfo', ['path', 'date'])

class ConfigInitException(Exception):
	def __init__(self,*args,**kwargs):
		Exception.__init__(self, *args, **kwargs)
		
def is_log_file(path):
	file_name = split(path)[1]
	if (not file_name.startswith(LOG_NAME_PREFIX)):
		return False
	date = file_name[len(LOG_NAME_PREFIX):]
	if (date.endswith('.gz')):
		date = date[:len('.gz')]
	try:
		datetime.datetime.strptime(date, '%Y%m%d')
	except:
		return False
	return True
		
def get_log_info(path):
	file_name = split(path)[1]
	date = file_name[len(LOG_NAME_PREFIX):]
	if (date.endswith('.gz')):
		date = date[:len('.gz')]
	date_formatted = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y.%m.%d')
	return LogInfo(path, date_formatted)
		
def get_latest_log_info(logdir):
	logfiles = [get_log_info(join(logdir, path)) for path in listdir(logdir) if is_log_file(join(logdir, path))]
	if (not logfiles):
		logging.error("Didn't found any log in %s", logdir)
		raise Exception("Didn't found any log in %s" % logdir)
	logfiles = sorted(logfiles, reverse = True, key = lambda log_info : log_info.date)
	logging.info('Analyzing %s log file', logfiles[0].path)
	return logfiles[0]

def get_url(query):
    return query.split(' ')[1]

def parse_log_line(line):
    tokens =  shlex.split(line.replace('[', '"').replace(']', '"'))
    return get_url(tokens[4]), float(tokens[12])

def get_log_records(log_file):
	if (log_file.endswith('.gz')):
		opened_log = gzip.open(log_file, 'r')
	else:
		opened_log = open(log_file, 'r')
	for line in opened_log:
		try:
			if (not isinstance(line, str)):
				line = line.decode('utf-8')
			yield parse_log_line(line)
		except Exception as e:
			logging.info('Unknown format in log file: %s', line)
			
def median(sorted_values):
    arr_len = len(sorted_values)
    if (arr_len % 2 == 0):
        return (sorted_values[arr_len // 2 - 1] + sorted_values[arr_len // 2]) / 2.0
    return sorted_values[arr_len // 2]

def calc_stats(url, times, queries_count, total_time):
	sorted_times = sorted(times)
	stats = {}
	stats['url'] = url
	stats['count'] = len(sorted_times)
	stats['count_perc'] = stats['count'] * 100.0 / queries_count
	stats['time_sum'] = sum(sorted_times)
	stats['time_perc'] = stats['time_sum'] * 100.0 / total_time
	stats['time_avg'] = stats['time_sum'] / stats['count']
	stats['time_max'] = sorted_times[len(sorted_times) - 1]
	stats['time_med'] = median(sorted_times)
	
	return stats

def calc_table(lines_generator, report_size):
	url_times = {}
	queries_count = 0
	total_time = 0.0
	lines_processed = 0
	for url, time in lines_generator:
		queries_count += 1
		if (not (url in url_times)):
			url_times[url] = []
		total_time += time
		url_times[url].append(time)
		lines_processed += 1
		if (lines_processed % 5000 == 0):
			logging.info('Lines processed: %d', lines_processed)
	
	logging.info('Line processing is done')
	
	table = []
	for url, times in url_times.items():
		table.append(calc_stats(url, times, queries_count, total_time))
		
	sorted_table = sorted(table, key = lambda rec : rec['time_sum'], reverse = True)
	
	if (len(sorted_table) <= report_size):
		return sorted_table
	return sorted_table[:report_size]

def render_template(template, table):
	if (template is None):
		return None
	return template.replace('$table_json', str(table))
	
def get_template():
	try:
		with open('report.html', 'r') as template:
			return template.read()
	except:
		logging.exception('Error while reading template')
		raise
		
def get_report_path(report_dir, log_date):
	return report_dir + '/' + 'report-' + log_date + '.html'
	
def job_is_done(report_dir, log_date):
	return isfile(get_report_path(report_dir, log_date))
		
def write_report(report_dir, report, log_date):
	try:
		makedirs(report_dir)
	except OSError as e:
		if (not exists(report_dir)):
			logging.exception('Error while creating directory for report: %s', report_dir)
	try:
		report_path = get_report_path(report_dir, log_date)
		with open(report_path, 'w') as report_file:
			report_file.write(report)
	except:
		logging.exception('Error while writing report')
		
def write_ts(report_dir, log_date):
	report_path = get_report_path(report_dir, log_date)
	ts = getmtime(report_path)
	with open(config['TS_FILE'], 'w') as f:
		f.write(str(ts))
		
def set_config_value(config_line):
	try:
		tokens = shlex.split(config_line)
	except:
		raise ConfigInitException('Bad format of config line: %s' % config_line)
	if (len(tokens) != 2):
		raise ConfigInitException('Bad format of config line: %s' % config_line)
	if (tokens[0] == 'REPORT_SIZE:'):
		try:
			config['REPORT_SIZE'] = int(tokens[1])
		except:
			raise ConfigInitException('Bad value of REPORT_SIZE: %s' % tokens[1])
		return
	if (tokens[0] == 'REPORT_DIR:'):
		config['REPORT_DIR'] = tokens[1]
		return
	if (tokens[0] == 'LOG_DIR:'):
		config['LOG_DIR'] = tokens[1]
		return
	if (tokens[0] == 'LOG_FILE:'):
		config['LOG_FILE'] = tokens[1]
		return
	if (tokens[0] == 'TS_FILE:'):
		config['TS_FILE'] = tokens[1]
		return
	raise ConfigInitException('Unknown option in config: %s' % tokens[0])

def init_config_from_file(config_file_path):
	with open(config_file_path, 'r') as config_file:
		for line in config_file.readlines():
			set_config_value(line)
		
def init_config(cmd_args):
	config_file = './log_analyzer.conf'
	try:
		options, arguments = getopt(cmd_args, '', ['config=', 'log-level='])
	except:
		raise ConfigInitException('Only --config and --log-level options supported')
	for key, value in options:
		if (key == '--config'):
			config_file = value
		if (key == '--log-level'):
			config['LOG_LEVEL'] = getattr(logging, value, logging.ERROR)
	if (not isfile(config_file)):
		raise ConfigInitException('File not found: %s' % config_file)
	return init_config_from_file(config_file)

def set_up_logger():
	logging.basicConfig(
		format = '[%(asctime)s] %(levelname).1s %(message)s',
		filename = config['LOG_FILE'],
		level = config['LOG_LEVEL'],
		datefmt='%Y.%m.%d %H:%M:%S'
	)

def main():
	init_config(sys.argv[1:])
	set_up_logger()
	latest_log_info = get_latest_log_info(config['LOG_DIR'])
	if (job_is_done(config['REPORT_DIR'], latest_log_info.date)):
		logging.info('Report have been already created')
		return
	table = calc_table(get_log_records(latest_log_info.path), config['REPORT_SIZE'])
	rep = render_template(get_template(), table)
	write_report(config['REPORT_DIR'], rep, latest_log_info.date)
	write_ts(config['REPORT_DIR'], latest_log_info.date)

if __name__ == "__main__":
	main()