from urllib2 import Request, urlopen, URLError
from urlparse import urlparse
import re, json, multiprocessing, time

NUM_PROCESSES = 6
REDIRECT = True

BASE_URL = 'https://api.similarweb.com/v1/website/'
SW_API_KEY = ''
START_DATE = '2017-01'
END_DATE = '2017-06'
MAIN_DOMAIN_ONLY = 'false'
GRANULARITY = 'monthly'

INPUT_FILE_NAME = ''
OUTPUT_FILE_NAME = '_'.join(['avg-hits', GRANULARITY, START_DATE, END_DATE]) + '.csv'


def get_domain(url):
	"""
	Parses url for domain:
		- uses urlparse to get domain
		- lower case
		- strips trailing whitespace
		- removes any instance of www., www4., m., etc. at begining of string
	Args:
		url (str)
	Returns:
		str: the domain of url
	"""
	domain = urlparse(url).netloc.lower().strip()
	match = re.match(r'(www([0-9])*|m)\.', domain)
	if match != None:
		domain = domain[match.span()[1]:]
	return domain


def build_api_request(domain):
	"""
	Constructs Request object for similar-web api call
	Args:
		domain (str)
	Returns:
		Request object
	"""
	api_request = ''.join([BASE_URL, domain, '/total-traffic-and-engagement/visits?api_key=', SW_API_KEY, '&start_date=', START_DATE, '&end_date=', END_DATE, '&main_domain_only=', MAIN_DOMAIN_ONLY, '&granularity=', GRANULARITY])
	return Request(api_request) 


def attempt_redirect(url, emptyRes):
	"""
	Attempts to resolve url and get final destination
	Args:
		url (str)
		emptyRes (dict): response to return if error occurs
	Returns:
		dict: api response
	"""
	finalurl = None
	try:
		res = urlopen(Request(url))
		finalurl = res.geturl()
	except URLError, e:
		return emptyRes
	
	if finalurl != None:
		req = build_api_request(get_domain(finalurl))
		try:
			return json.loads(urlopen(req).read())
		except URLError, e:
			return emptyRes


def request_monthly_hits(source_detail):
	"""
	Constructs and sends api request
	Args:
		source_detail (str): contains the media_id and url, comma-separated
	Returns:
		dict: similar-web api response
	"""

	source_detail = source_detail.strip().split(',')
	media_id = source_detail[0].strip()
	url = source_detail[1].strip()
	domain = get_domain(url)
	req = build_api_request(domain)

	try:
		res = json.loads(urlopen(req).read())
	except URLError, e:
		emptyRes = {'meta': {'request': {'domain': domain}}, 'visits': 'Data Not Found'}
		if REDIRECT:
			res = attempt_redirect(url, emptyRes)
		else:
			res = emptyRes

	res['url'] = url
	res['media_id'] = media_id
	return res


if __name__ == '__main__':
	start = time.time()
	
	# expects media_id and url (comma-separated) for each source (line-separated)
	with open(INPUT_FILE_NAME, 'r') as input:
		sources = input.readlines()

	p = multiprocessing.Pool(NUM_PROCESSES)
	results = p.map(request_monthly_hits, sources)

	# write results to file
	with open(OUTPUT_FILE_NAME, 'w') as output:
		output.write('media_id,url,domain,' + GRANULARITY + ' average\n')

		for result in results:
			media_id = result['media_id']
			url = result['url']
			domain = result['meta']['request']['domain']

			# calculate average visits
			if (type(result['visits']) == list):
				visits = [v['visits'] for v in result['visits']]
				avg_visits = str(sum(visits) / len(visits))
			else:
				avg_visits = result['visits']

			output.write(','.join([media_id, url, domain, avg_visits + '\n']))

	end = time.time()
	print 'took', str((end-start)), 'seconds'
