# Desc: Utility functions for the project
# Auth: Samuel Santos
# Date: 20-10-15



# importing the requests module
#import requests
#import wget
#import pandas as pd

#from os import rename
from os.path import exists



def vprint(*args, **kwargs):
	if verbose:
		print(*args, **kwargs)

