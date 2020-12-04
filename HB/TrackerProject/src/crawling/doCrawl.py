

from __future__ import absolute_import
import sys
import os
import time
import json
import copy
import random
import argparse
from multiprocessing import Process
from publicsuffix import fetch
from publicsuffix import PublicSuffixList
from getSitesToVisit import GetSitesToVisit as gs

psl_file = fetch()
psl = PublicSuffixList(psl_file)
baseSrcPath = os.path.abspath('../../')
baseHBPath = os.path.abspath('../../../../')
sys.path.append(baseSrcPath)
sys.path.append(baseHBPath)
from lcdk import lcdk as LeslieChow


from six.moves import range
from ScriptUtils.scriptUtils import ScriptUtils
from automation import CommandSequence, TaskManager

class DoCrawl:
    def __init__(self, **kwargs):
        self.crawl_type = kwargs.get("crawl_type", "AB_TRAINING")
        
        
    def ml_training(self):
        generated_profiles = []
        with open('generated_profiles.json') as f: 
            generated_profiles = json.load(f)
        

if __name__ == "__main__":
    ml_crawl = DoCrawl(crawl_type="ML_TRAINING")