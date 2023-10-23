# Desc: Utility functions for the project
# Auth: Samuel Santos
# Date: 20-10-15



# importing the requests module
#import requests
#import wget
#import pandas as pd

#from os import rename
from os.path import exists

import time, datetime


verbose = True

def vprint(*args, **kwargs):
	if verbose:
		print(*args, **kwargs)

# print current time and elapsed time since last execution
def vprint_time(start_time = 0, prefix = ''):
	current_time = time.time()
	if start_time == 0:
		vprint(prefix + 'Current time: ' + str(datetime.datetime.now()))
	else:
		elapsed_time = current_time - start_time
		vprint(prefix + 'Current time: ' + str(datetime.datetime.now()) + '; Elapsed time: ' + str(datetime.timedelta(seconds=elapsed_time)))
	return current_time
