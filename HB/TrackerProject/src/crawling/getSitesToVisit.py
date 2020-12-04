import os
import sys
import json
import random
from lcdk import lcdk as LeslieChow
from argparse import ArgumentParser


class GetSitesToVisit:
    def __init__(self):    
        self.DBG = LeslieChow.lcdk()
        self.categories = [ "Adult",
                            "Arts",
                            "Business",
                            "Computers",
                            "Games",
                            "Health",
                            "Home",
                            "intent",
                            "KidsAndTeens",
                            "News",
                            "Recreation",
                            "Reference",
                            "Regional",
                            "Science",
                            "Shopping",
                            "Society",
                            "Sports"
                        ]


    def getTestingSites(self, sitesToVisit_path, crawl_type):
        """ 
        getTestingSites: 
        Args: 
            sitesToVisit_path (:obj:`str`): Path to the alexa top 50 sits from each category.
            crawl_type (:obj:`str`): The type training crawl [AB_TRAIN|ML_TRAIN]
        Returns:
            A list of prebid.js urls based upon crawl_type 
        """
        prebidJsSites = []
        with open(sitesToVisit_path) as f:
            prebidJsSites = json.load(f)
            if "ML_TEST" in crawl_type:
                ml_test_site = []
                sn = random.randint(0, prebidJsSites.__len__())
                ml_test_site.append(prebidJsSites[sn])
                prebidJsSites = ml_test_site
        return prebidJsSites

                     
    def getTrainingSites(self, sitesToVisit_path, volume, intent, category, crawl_type):
        """
        getTrainingSites: 
        Args: 
            sitesToVisit_path (:obj:`str`): Path to the alexa top 50 sits from each category.
            volume (`int`): The number of sites to vist [1..10].
            intent (:obj:`str`): intent type of training crawl [NO_INTENT|INTENT]
            category (:obj:`str`): Alex category type [Adult|Arts|Business|Computers|Games|Health|Home|intent|KidsAndTeens|News|Recreation|Reference|Regional|Science|Shopping|Society|Sports]
            crawl_type (:obj:`str`): The type training crawl [AB_TRAIN|ML_TRAIN]
                        
        Returns: 
            A list of urls for training
        """  
        tmp = os.path.join('../config/', 'training', 'sites', sitesToVisit_path )
        sitesToVisit_path = tmp

        intent_sites = []
        no_intent_sites = []
        categories= os.listdir(tmp)
        category_path = ""
        sitesToVisit = []
        for f in categories:
            if category in f: 
                category_path = os.path.join(sitesToVisit_path, f)
        intent_site_path = os.path.join(sitesToVisit_path, 'intent.json')

        with open(intent_site_path) as f:
            intent_sites = json.load(f)
            intent_sites = intent_sites['Intent']

        with open(category_path) as f: 
            data = json.load(f)
            no_intent_sites = data[category]
            if crawl_type == "ML_TRAIN":
                ml_train_sites = []
                site_nums = []
                for i in list(range(0, volume)):
                    site_nums.append(random.randint(0,49))
                for sn in site_nums: 
                    ml_train_sites.append(no_intent_sites[sn])
                no_intent_sites = ml_train_sites
        for site in no_intent_sites: 
            intent_sites.append(site)
        if intent == 'NO_INTENT':
           sitesToVisit = no_intent_sites
        else:
            sitesToVisit = intent_sites
        return sitesToVisit
