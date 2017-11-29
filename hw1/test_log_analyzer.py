import unittest
import log_analyzer
import os
import shutil

class TestAnazyler(unittest.TestCase):

	def setUp(self):
		try:
			os.makedirs('test-dir/logdir')
			os.makedirs('test-dir/reportdir')
		except:
			pass
		test_files = ['test-dir/logdir/not-log-file.txt', 'test-dir/logdir/nginx-access-ui.log-20170502', 'test-dir/logdir/nginx-access-ui.log-20170625', 'test-dir/logdir/nginx-access-ui.log-20170301.gz', 'test-dir/logdir/nginx-access-ui.log-20170510.gz', 'test-dir/reportdir/report-2010.07.07.html']
		for test_file in test_files:
			with open(test_file, 'w') as f:
				f.write('')

	def test_is_log_file(self):
		self.assertFalse(log_analyzer.is_log_file('file-doesnt-exist.txt', 'test-dir/logdir/file-doesnt-exist.txt'))
		self.assertFalse(log_analyzer.is_log_file('not-log-file.txt', 'test-dir/logdir/not-log-file.txt'))
		self.assertTrue(log_analyzer.is_log_file('nginx-access-ui.log-20170301.gz', 'test-dir/logdir/nginx-access-ui.log-20170301.gz'))
		self.assertTrue(log_analyzer.is_log_file('nginx-access-ui.log-20170502', 'test-dir/logdir/nginx-access-ui.log-20170502'))
		
	def test_latest_log(self):
		log_file, log_date = log_analyzer.get_latest_log('test-dir/logdir')
		self.assertEqual(log_file, 'test-dir/logdir\\nginx-access-ui.log-20170625')
		self.assertEqual(log_date, '2017.06.25')
		
	def test_parse_log_file(self):
		self.assertEqual(log_analyzer.parse_log_line('1.196.116.32 -  - [29/Jun/2017:03:50:24 +0300] "GET /api/v2/banner/26613316 HTTP/1.1" 200 1283 "-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5" "-" "1498697424-2190034393-4708-9752818" "dc7161be3" 0.189'), ('/api/v2/banner/26613316', 0.189))
		
	def create_lines_generator(self):
		lines = ['1.168.65.96 -  - [29/Jun/2017:03:50:49 +0300] "GET /api/v2/internal/banner/24278838/info HTTP/1.1" 200 340 "-" "-" "-" "1498697449-2539198130-4708-9753107" "89f7f1be37d" 0.000', 
				'1.170.209.160 -  - [29/Jun/2017:03:50:49 +0300] "GET /export/appinstall_raw/2017-06-29/ HTTP/1.0" 200 28481 "-" "Mozilla/5.0 (Windows; U; Windows NT 6.0; ru; rv:1.9.0.12) Gecko/2009070611 Firefox/3.0.12 (.NET CLR 3.5.30729)" "-" "-" "-" 10.000',
				'1.196.116.32 -  - [29/Jun/2017:03:50:49 +0300] "GET /api/v2/banner/26596561 HTTP/1.1" 200 1485 "-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5" "-" "1498697448-2190034393-4708-9753097" "dc7161be3" 90',
				'1.170.209.160 -  - [29/Jun/2017:03:50:49 +0300] "GET /export/appinstall_raw/2017-06-30/ HTTP/1.0" 404 162 "-" "Mozilla/5.0 (Windows; U; Windows NT 6.0; ru; rv:1.9.0.12) Gecko/2009070611 Firefox/3.0.12 (.NET CLR 3.5.30729)" "-" "-" "-" 0.000',
				'1.169.137.128 -  - [29/Jun/2017:03:50:49 +0300] "GET /api/v2/banner/15521472 HTTP/1.1" 200 48421 "-" "Slotovod" "-" "1498697448-2118016444-4708-9753105" "712e90144abee9" 0.000']
		for line in lines:
			yield log_analyzer.parse_log_line(line)
		
	def test_calc_table(self):
		table = log_analyzer.calc_table(self.create_lines_generator(), 1)
		self.assertEqual(table, [('/api/v2/banner/26596561', {'count' : 1, 'count_perc' : 20.0, 'time_avg' : 90.0, 'time_max' : 90.0, 'time_med' : 90.0, 'time_perc' : 90.0, 'time_sum' : 90.0})])
		
	def test_get_report_path(self):
		self.assertEqual(log_analyzer.get_report_path('mydir', '1992.01.07'), 'mydir/report-1992.01.07.html')
		
	def test_job_is_done(self):
		self.assertTrue(log_analyzer.job_is_done('test-dir/reportdir', '2010.07.07'))
		self.assertFalse(log_analyzer.job_is_done('test-dir/reportdir', '2010.07.08'))
           
	def test_median(self):
		self.assertEqual(log_analyzer.median([1, 2, 3]), 2)
		self.assertEqual(log_analyzer.median([1]), 1)
		self.assertEqual(log_analyzer.median([1, 3, 5, 10]), 4)

	def test_url_parsing(self):
		self.assertEqual(log_analyzer.get_url('GET /export/appinstall_raw/2017-06-29/ HTTP/1.0'), '/export/appinstall_raw/2017-06-29/')
		
	def tearDown(self):
		try:
			shutil.rmtree('test-dir', ignore_errors=True)
		except:
			pass
			
if __name__ == '__main__':
	unittest.main()