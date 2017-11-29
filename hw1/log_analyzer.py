# -*- coding: utf-8 -*-

from os import listdir, makedirs
from os.path import isfile, join, exists, getmtime
import gzip
from getopt import getopt
import shlex
import sys
import errno
import logging

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
	"LOG_FILE" : None,
	"LOG_LEVEL": logging.ERROR,
	"TS_FILE" : './log_analyzer.ts'
}

LOG_NAME_PREFIX = 'nginx-access-ui.log-'

class ConfigInitException(Exception):
	def __init__(self,*args,**kwargs):
		Exception.__init__(self, *args, **kwargs)
		
def is_log_file(rel_path, abs_path):
    return str(rel_path).startswith(LOG_NAME_PREFIX) and isfile(abs_path)

def get_latest_log(logdir):
	logfiles = [(path, join(logdir, path)) for path in listdir(logdir) if is_log_file(path, join(logdir, path))]
	if (not logfiles):
		logging.error("Didn't found any log in %s", logdir)
		raise Exception("Didn't found any log in %s" % logdir)
	logfiles = sorted(logfiles, reverse = True)
	if (len(logfiles[0][0]) != len(LOG_NAME_PREFIX) + 8):
		logging.error('Incorrect date format: %s', logfiles[0][0])
		raise Exception('Incorrect date format: %s' % logfiles[0][0])
	log_date = logfiles[0][0][len(LOG_NAME_PREFIX):len(LOG_NAME_PREFIX) + 8]
	log_date = log_date[:4] + '.' + log_date[4:6] + '.' + log_date[6:]
	logging.info('Analyzing %s log file', logfiles[0][1])
	return logfiles[0][1], log_date

def open_log_file(log_file):
    if (log_file.endswith('.gz')):
        return gzip.open(log_file, 'r')
    return open(log_file, 'r')
	
def get_url(query):
    return query.split(' ')[1]

def parse_log_line(line):
    tokens =  shlex.split(line.replace('[', '"').replace(']', '"'))
    return get_url(tokens[4]), float(tokens[12])

def get_log_records(log_file):
	for line in open_log_file(log_file):
		try:
			if (not isinstance(line, str)):
				print('NOT')
				line = line.decode('utf-8')
			yield parse_log_line(line)
		except Exception as e:
			logging.info('Unknown format in log file: %s', line)
			
def median(sorted_values):
    arr_len = len(sorted_values)
    if (arr_len % 2 == 0):
        return (sorted_values[arr_len // 2 - 1] + sorted_values[arr_len // 2]) / 2.0
    return sorted_values[arr_len // 2]

def calc_stats(times, queries_count, total_time):
    sorted_times = sorted(times)
    stats = {}
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
	
	stat = {}
	for url, times in url_times.items():
		stat[url] = calc_stats(times, queries_count, total_time)
		
	sorted_stat = sorted(stat.items(), key = lambda rec : rec[1]['time_sum'], reverse = True)
	
	if (len(sorted_stat) <= report_size):
		return sorted_stat
	return sorted_stat[:report_size]
		
def json_repr_one_rec(stat_rec):
    json = {}
    json['url'] = stat_rec[0]
    json['count'] = stat_rec[1]['count']
    json['count_perc'] = stat_rec[1]['count_perc']
    json['time_sum'] = stat_rec[1]['time_sum']
    json['time_perc'] = stat_rec[1]['time_perc']
    json['time_avg'] = stat_rec[1]['time_avg']
    json['time_max'] = stat_rec[1]['time_max']
    json['time_med'] = stat_rec[1]['time_med']
    
    return json
	
def json_repr(stat_dict):
    return [json_repr_one_rec(rec) for rec in stat_dict]

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
		if e.errno != errno.EEXIST:
			logging.exception('Error while creating directory for report: %s', report_dir)
	try:
		report_path = get_report_path(report_dir, log_date)
		with open(report_path, 'w') as report_file:
			report_file.write(report)
		return getmtime(report_path)
	except:
		logging.exception('Error while writing report')
		
def write_ts(ts):
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
	logging.basicConfig(format = '[%(asctime)s] %(levelname).1s %(message)s', filename = config['LOG_FILE'], level = config['LOG_LEVEL'], datefmt='%Y.%m.%d %H:%M:%S')

def main():
	init_config(sys.argv[1:])
	set_up_logger()
	log_file, log_date = get_latest_log(config['LOG_DIR'])
	if (job_is_done(config['REPORT_DIR'], log_date)):
		logging.info('Report have been already created')
		return
	table = calc_table(get_log_records(log_file), config['REPORT_SIZE'])
	rep = render_template(get_template(), json_repr(table))
	write_ts(write_report(config['REPORT_DIR'], rep, log_date))

if __name__ == "__main__":
	main()