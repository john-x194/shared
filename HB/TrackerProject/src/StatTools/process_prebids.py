import os
import sys
import time
import math
import json
import scipy
import logging
import tempfile
import operator
import argparse
import numpy as np
import pandas as pd

from lcdk import lcdk as LeslieChow
import statsmodels.stats.weightstats as statW 
from scipy.stats import chi2_contingency

class process_prebids: 
    def __init__(self,**kwargs): 
        self.DBG = LeslieChow.lcdk()
        archive_path = kwargs.get('archive_path', '/mnt/hgfs/archive/AB_TESTING_BIDS')
        tar_files = os.listdir(archive_path)
        self.logger = logging.getLogger('TRACKER PROJECT')
        os.system('echo \{\}> intent_stats.json')
        os.system('echo \{\}> no_intent_stats.json')
        os.system('echo \{\}> winning_no_intent_stats.json')
        os.system('echo \{\}> bids_intent.json')
        os.system('echo \{\}> bids_no_intent.json')
        os.system('echo \{\}> winning_bids_no_intent.json')

        for f in tar_files: 
            if 'tar.gz' in f: 
                continue
            else: 
                tar_files.remove(f)
        self.top_5_bidders = ['appnexus','rubicon', 'ix','pubmatic', 'openx']
        self.all_bidders = []

        self.bidders_count = {}
        self.winning_no_intent_bidders_count = {}
        self.bidders_zero_count = {}                
        self.all_bidders_count = {}
        self.bidder_sites = {}
        self.no_intent_winning_bids = {}

        self.zero_bids_count_intent = 0
        self.zero_bids_count_no_intent = 0
        self.no_intent_bid_count = 0
        self.winning_no_intent_bid_count = 0
        self.intent_bid_count = 0

        self.no_intent_median_cpms_df = None
        self.winning_no_intent_cpms_df = None
        self.intent_median_cpms_df = None
        self.cpm_ratios_df = None

        self.no_intent_bid_counts_df = None
        self.winning_no_intent_bid_count_df = None
        self.intent_bid_counts_df = None
        self.bid_count_ratios_df = None

        rendered = {}
        tarfile_count = 1
        rows = []

        for f in tar_files:

            bids_dump = {}


            self.DBG.warning("processing file - {} - {}/{}".format(f, tarfile_count, tar_files.__len__()))

            results_path = ""
            tmpdir = tempfile.mkdtemp()
            tar_path = os.path.join(archive_path, f)
            os.system('tar -xf {} -C {}'.format(tar_path, tmpdir))
            untarred = os.listdir(tmpdir)[0]
            if 'mnt' in os.listdir(tmpdir):
                untarred = os.listdir(os.path.join(tmpdir, 'mnt/hgfs/archive'))[0]
                results_path = os.path.join(tmpdir, 'mnt/hgfs/archive', untarred)
            else: 
                results_path = os.path.join(tmpdir, untarred )
            bids_path = ""
            output = ""
   
            if "NO_INTENT" in untarred: 
                bids_path = os.path.join(results_path, 'bids_no_intent')
                output = 'bids_no_intent.json'
            else: 
                bids_path = os.path.join(results_path, 'bids_intent')
                output = 'bids_intent.json'


            self.bids_files = os.listdir(bids_path)

        
            for bid_file in self.bids_files: 
                bids = []
                path = os.path.join(bids_path, bid_file)
                msg = "[PROCESS BIDS] - {} retreiving bids {}".format(time.asctime(), bid_file)
                # self.log_msg(msg)
                path = os.path.join(bids_path,bid_file)
                with open(path) as f: 
                    bids = json.load(f)
                for cat in bids: 
                    for site in bids[cat]:
                        for visit in bids[cat][site]:
                            for bid in bids[cat][site][visit]:
                                if isinstance(bid, type(u'')): 
                                    continue
                        
                                cpm = bid['cpm']
                                adSlot = bid['adunit']
                                bidder = bid['bidder']
                                if bidder == "indexExchange":
                                    bidder = 'ix'
                                if bidder == "appnexusAst":
                                    bidder = 'appnexus'
                                
                                if bidder not in self.all_bidders: 
                                    self.all_bidders.append(bidder)
                                rendered = bid['rendered']
                                
                                bdr_intent  =""
                                cat_profile = ""
                                if bidder in self.bidder_sites: 
                                    if site in self.bidder_sites[bidder]: 
                                        self.bidder_sites[bidder][site]+=1
                                    else: 
                                        self.bidder_sites[bidder][site]=1
                                else: 
                                    self.bidder_sites[bidder]={site: 1}
                                if "NO_INTENT" in untarred: 
                                    bdr_intent = 'no_intent'
                                else: 
                                    bdr_intent = 'intent'
                                
                                if cpm == 0 or cpm == 0.0:
                                    cat_profile = 'zero_bid'
                                else: 
                                    cat_profile = 'pos_bid'
                                if bidder in self.all_bidders_count: 
                                    if cat in self.all_bidders_count[bidder]:
                                        if bdr_intent in self.all_bidders_count[bidder][cat]:
                                            if cat_profile in self.all_bidders_count[bidder][cat][bdr_intent]:
                                                self.all_bidders_count[bidder][cat][bdr_intent][cat_profile] +=1    
                                            else: 
                                                self.all_bidders_count[bidder][cat][bdr_intent].update({cat_profile:1})
                                        else: 
                                            self.all_bidders_count[bidder][cat][bdr_intent]= {cat_profile:1}
                                            
                                    else: 
                                        self.all_bidders_count[bidder][cat] = {bdr_intent: {cat_profile:1}}
                                else: 
                                    self.all_bidders_count.update({bidder: {cat: {bdr_intent: {cat_profile:1}}}})
 
                                if 'NO_INTENT' in untarred: 
                                    rows.append([bidder, adSlot, rendered, 'INTENT', cat, cpm])
                                    # self.DBG.warning(df)
                                        

                                else: 
                                    rows.append([bidder, adSlot, rendered, 'INTENT', cat, cpm])
                                    # self.DBG.warning(df)

                                if cpm == 0 or cpm == 0.0: 
                                    if 'NO_INTENT' in untarred: 
                                        self.zero_bids_count_no_intent+=1
                                        if bidder in self.bidders_zero_count:
                                            if 'no_intent' in self.bidders_zero_count[bidder]: 
                                                self.bidders_zero_count[bidder]['no_intent']+=1
                                            else:
                                                self.bidders_zero_count[bidder]['no_intent'] = 1
                                        else:
                                            self.bidders_zero_count[bidder]={'no_intent':1}
                                    else:
                                        self.zero_bids_count_intent+=1
                                        if bidder in self.bidders_zero_count: 
                                            if 'intent' in self.bidders_zero_count[bidder]: 

                                                self.bidders_zero_count[bidder]['intent']+=1
                                            else:
                                                self.bidders_zero_count[bidder]['intent']=1
                                        else:
                                            self.bidders_zero_count[bidder]={'intent':1}  
                                    
                                else: 
                                    if 'NO_INTENT' in untarred: 
                                        self.no_intent_bid_count+=1
                                        if rendered: 
                                            self.winning_no_intent_bid_count+=1
                                            if bidder in self.winning_no_intent_bidders_count: 
                                                if cat in self.winning_no_intent_bidders_count[bidder]:
                                                    if bdr_intent in self.winning_no_intent_bidders_count[bidder][cat]:
                                                        if cat_profile in self.winning_no_intent_bidders_count[bidder][cat][bdr_intent]:

                                                            self.winning_no_intent_bidders_count[bidder][cat][bdr_intent][cat_profile] +=1    
                                                        else: 
                                                            self.winning_no_intent_bidders_count[bidder][cat][bdr_intent].update({cat_profile:1})
                                                    else: 
                                                        self.winning_no_intent_bidders_count[bidder][cat][bdr_intent]= {cat_profile:1}
                                                        
                                                else: 
                                                    self.winning_no_intent_bidders_count[bidder][cat] = {bdr_intent: {cat_profile:1}}
                                            else: 
                                                self.winning_no_intent_bidders_count.update({bidder: {cat: {bdr_intent: {cat_profile:1}}}})
                                        if bidder in self.bidders_count:
                                            if 'no_intent' in self.bidders_count[bidder]: 
                                                self.bidders_count[bidder]['no_intent']+=1
                                                
                                            else:
                                                self.bidders_count[bidder]['no_intent'] = 1
                                        else:
                                            self.bidders_count[bidder]={'no_intent':1}
                                    else:
                                        self.intent_bid_count+=1
                                        if bidder in self.bidders_count: 
                                            if 'intent' in self.bidders_count[bidder]: 
                                                self.bidders_count[bidder]['intent']+=1
                                            else:
                                                self.bidders_count[bidder]['intent']=1
                                        else:
                                            self.bidders_count[bidder]={'intent':1}
                                    
                                    if bidder in bids_dump: 
                                        if cat in bids_dump[bidder]:
                                            bids_dump[bidder][cat].append(cpm)
                                        else: 
                                            bids_dump[bidder][cat] = [cpm]
                                    else: 
                                        bids_dump[bidder] = {cat:[cpm]}
                                    if rendered: 
                                        if 'NO_INTENT' in untarred: 
                                            if bidder in self.no_intent_winning_bids: 
                                                if cat in self.no_intent_winning_bids[bidder]:
                                                    self.no_intent_winning_bids[bidder][cat].append(cpm)
                                                else: 
                                                    self.no_intent_winning_bids[bidder][cat] = [cpm]
                                            else: 
                                                self.no_intent_winning_bids[bidder] = {cat:[cpm]}
            tarfile_count+=1
            # if tarfile_count > 2: 
            #     break
            data = []

            with open(output) as f: 
                data = json.load(f)
                for bidder in  self.no_intent_winning_bids:
                    if bidder in data: 
                        for category in  self.no_intent_winning_bids[bidder]:
                            if category in data[bidder]:
                                for bid in  self.no_intent_winning_bids[bidder][category]:
                                    data[bidder][category].append(bid)
                            else: 
                                data[bidder][category] =  self.no_intent_winning_bids[bidder][category]
                    else: 
                        data[bidder] =  self.no_intent_winning_bids[bidder]
            with open('winning_bids_no_intent.json', 'w') as f:
                json.dump(self.no_intent_winning_bids, f)
            
            data = []
            #read in previous bids and update with new bids
            with open(output) as f: 
                data = json.load(f)
                for bidder in bids_dump:
                    if bidder in data: 
                        for category in bids_dump[bidder]:
                            if category in data[bidder]:
                                for bid in bids_dump[bidder][category]:
                                    data[bidder][category].append(bid)
                            else: 
                                data[bidder][category] = bids_dump[bidder][category]
                    else: 
                        data[bidder] = bids_dump[bidder]
            
            with open(output, 'w') as f:
                json.dump(data, f)
  

        self.winning_bid_stats_df = pd.DataFrame(rows, columns=['Bidders', 'AdSlot', 'Rendered', 'Intent/No-Intent','category', 'CPM'])
        self.winning_bid_stats_df.to_csv('results/winning_bids.csv', index=False)


    def remove_low_bids(self, bids, advertiser, category, intent, median, threshold=10**2): 
        N =len(bids)
        sorted_bids = sorted(bids)
        if len(bids) < 10: 
            return bids
        remove = 0
        for bid in sorted_bids:
            if bid < median*threshold:
                bids.remove(bid)
                remove+=1

        #update counts for any removed bids
        if intent == "intent":
            self.intent_bid_count-=2
            if advertiser in self.bidders_count: 
                if intent in self.bidders_count[advertiser]:
                    self.bidders_count[advertiser][intent]-=remove
        else: 
            self.no_intent_bid_count-=2
            if advertiser in self.bidders_count: 
                if intent in self.bidders_count[advertiser]:
                    self.bidders_count[advertiser][intent]-=remove
        if advertiser in self.all_bidders_count:                    
            if category in self.all_bidders_count[advertiser]:
                if intent in self.all_bidders_count[advertiser][category]:
                    if 'pos_bid' in self.all_bidders_count[advertiser][category][intent]:
                        self.all_bidders_count[advertiser][category][intent]['pos_bid'] -=remove
        
        return bids
         


    def weighted_avg_and_std(self, values, weights):
        """
        Return the weighted average and standard deviation.

        values, weights -- Numpy ndarrays with the same shape.
        """
        average = np.average(values, weights=weights)

        # Fast and numerically precise:
        variance = np.average((values-average)**2, weights=weights)
        return (average, math.sqrt(variance))

    def remove_outliers(self, bids, advertiser, category, intent, threshold=0.01):
        return bids
        N =len(bids)
        sorted_bids = sorted(bids)
        if len(bids) < 10: 
            return bids
        remove = int(N*threshold)

        if remove <1: 
            remove = 1
        while remove > 0: 


            bids.remove(sorted_bids[remove])
            bids.remove(sorted_bids[-remove])
            remove-=1
            if intent == "intent":
                self.intent_bid_count-=2
                if advertiser in self.bidders_count: 
                    if intent in self.bidders_count[advertiser]:
                        self.bidders_count[advertiser][intent]-=2
            else: 
                self.no_intent_bid_count-=2
                if advertiser in self.bidders_count: 
                    if intent in self.bidders_count[advertiser]:
                        self.bidders_count[advertiser][intent]-=2
            if advertiser in self.all_bidders_count:                    
                if category in self.all_bidders_count[advertiser]:
                    if intent in self.all_bidders_count[advertiser][category]:
                        if 'pos_bid' in self.all_bidders_count[advertiser][category][intent]:
                            self.all_bidders_count[advertiser][category][intent]['pos_bid'] -=2

        return bids
    def parse_winning_bids(self): 
        print(self.all_bidders)
        bidder_rendered = {}
        bidder_not_rendered = {}
        adSlot_rendered = {}
        adSlot_not_rendered= {}
        bidder_adslot_rendered = {}
        bidder_adslot_not_rendered = {}
        category_rendered = {}
        category_not_rendered = {}
        category_adslot_rendered = {}
        category_adslot_rendered = {}

        cpm=0.0
        rendered_bids = {'rendered_count':1, 'rendered_cpm':cpm }
        not_rendered_bids = {'not_rendered_count':1, 'not_rendered_cpm':cpm }

        for index, row in self.winning_bid_stats_df.iterrows():
            bidder  = row['Bidders']
            adslot  = row['AdSlot']
            rendered  = row['Rendered']
            intent  = row['Intent/No-Intent']
            category  = row['category']
            cpm  = row['CPM']
            if rendered: 
                rendered_bids['rendered_count']+=1
                rendered_bids['rendered_cpm']+=cpm

                if bidder in bidder_rendered:
                    if 'count' in  bidder_rendered[bidder]:
                        bidder_rendered[bidder]['count']+=1
                    else: 
                        bidder_rendered[bidder]['count']=1
                    if 'cpm' in  bidder_rendered[bidder]:
                        bidder_rendered[bidder]['cpm']+=cpm
                    else: 
                        bidder_rendered[bidder]['cpm']+=cpm
                else:
                    bidder_rendered[bidder] = {'count': 1, 'cpm':cpm}
                if adslot in adSlot_rendered: 
                    if 'count' in  adSlot_rendered[adslot]:
                        adSlot_rendered[adslot]['count']+=1
                    else: 
                        adSlot_rendered[adslot]['count']=1
                    if 'cpm' in  adSlot_rendered[adslot]:
                        adSlot_rendered[adslot]['cpm']+=cpm
                    else: 
                        adSlot_rendered[adslot]['cpm']=cpm
                else:
                    adSlot_rendered[adslot]= {'count': 1, 'cpm': cpm}
                if category in category_rendered: 
                    if 'count' in  category_rendered[category]:
                        category_rendered[category]['count']+=1
                    else: 
                        category_rendered[category]['count']=1
                    if 'cpm' in  category_rendered[category]:
                        category_rendered[category]['cpm']+=cpm
                    else: 
                        category_rendered[category]['cpm']=cpm
                else:
                    category_rendered[category]= {'count': 1, 'cpm': cpm}

                if bidder in bidder_adslot_rendered: 
                    if adslot in bidder_adslot_rendered[bidder]: 
                        if 'count' in bidder_adslot_rendered[bidder][adslot]:
                            bidder_adslot_rendered[bidder][adslot]['count']+=1
                        else: 
                            bidder_adslot_rendered[bidder][adslot]['count']=1 

                        if 'cpm' in bidder_adslot_rendered[bidder][adslot]:
    
                            bidder_adslot_rendered[bidder][adslot]['cpm']+=cpm
                        else:
                            bidder_adslot_rendered[bidder][adslot]['cpm']=cpm
                        
                    else:
                        bidder_adslot_rendered[bidder][adslot]= {'count': 1, 'cpm': cpm}
            
                else:
                    bidder_adslot_rendered[bidder] = { adslot : {'count':1, 'cpm': cpm}}
                

                if bidder in rendered_bids: 
                    
                    if category in rendered_bids: 
                        if adslot in rendered_bids[category]: 
                            rendered_bids[bidder][category][adslot]['adslot_count'] +=1
                            rendered_bids[bidder][category][adslot]['adslot_cpm'] += cpm
                            rendered_bids['category_cpm']+=cpm
                            rendered_bids['category_count']+=1
                        else: 
                            rendered_bids[bidder][category][adslot] = {'adslot_count': 1, 'adslot_cpm':cpm}
                    
        
                    else: 
                        rendered_bids[bidder][category] = {'adslot_count': 1, 'adslot_cpm':cpm, 'category_count':1, 'category_cpm':cpm}
                        rendered_bids[bidder]['bidder_count']+=1
                        rendered_bids[bidder]['bidder_cpm']+=cpm
                        


                else: 
                    not_rendered_bids[bidder] = {   category: { 'adslot_count': 1, 
                                                                'adslot_cpm':cpm
                                                                },

                                                    'bidder_count' :1, 
                                                    'bidder_cpm':cpm
                                                }

                    
                

                    

            else: 
                not_rendered_bids['not_rendered_count']+=1
                not_rendered_bids['not_rendered_cpm']+=cpm

                if bidder in bidder_not_rendered:
                    if 'count' in  bidder_not_rendered[bidder]:
                        bidder_not_rendered[bidder]['count']+=1
                    else: 
                        bidder_not_rendered[bidder]['count']=1
                    if 'cpm' in  bidder_not_rendered[bidder]:
                        bidder_not_rendered[bidder]['cpm']+=cpm
                    else: 
                        bidder_not_rendered[bidder]['cpm']+=cpm
                else:
                    bidder_not_rendered[bidder] = {'count': 1, 'cpm':cpm}
                if adslot in adSlot_not_rendered: 
                    if 'count' in  adSlot_not_rendered[adslot]:
                        adSlot_not_rendered[adslot]['count']+=1
                    else: 
                        adSlot_not_rendered[adslot]['count']=1
                    if 'cpm' in  adSlot_not_rendered[adslot]:
                        adSlot_not_rendered[adslot]['cpm']+=cpm
                    else: 
                        adSlot_not_rendered[adslot]['cpm']=cpm
                else:
                    adSlot_not_rendered[adslot]= {'count': 1, 'cpm': cpm}
                if category in category_not_rendered: 
                    if 'count' in  category_not_rendered[category]:
                        category_not_rendered[category]['count']+=1
                    else: 
                        category_not_rendered[category]['count']=1
                    if 'cpm' in  category_not_rendered[category]:
                        category_not_rendered[category]['cpm']+=cpm
                    else: 
                        category_not_rendered[category]['cpm']=cpm
                else:
                    category_not_rendered[category]= {'count': 1, 'cpm': cpm}    

                if bidder in bidder_adslot_not_rendered: 
                    if adslot in bidder_adslot_not_rendered[bidder]: 
                        if 'count' in bidder_adslot_not_rendered[bidder][adslot]:
                            bidder_adslot_not_rendered[bidder][adslot]['count']+=1
                        else: 
                            bidder_adslot_not_rendered[bidder][adslot]['count']=1

                        if 'cpm' in bidder_adslot_not_rendered[bidder][adslot]:
    
                            bidder_adslot_not_rendered[bidder][adslot]['cpm']+=cpm
                        else:
                            bidder_adslot_not_rendered[bidder][adslot]['cpm']=cpm
                        
                    else:
                        bidder_adslot_not_rendered[bidder][adslot]= {'count': 1, 'cpm':cpm}
            
                else:
                    bidder_adslot_not_rendered[bidder] = { adslot : {'count':1, 'cpm':cpm}}
                

                if bidder in not_rendered_bids: 
                    
                    if category in not_rendered_bids: 
                        if adslot in not_rendered_bids[category]: 
                            not_rendered_bids[bidder][category][adslot]['adslot_count'] +=1
                            not_rendered_bids[bidder][category][adslot]['adslot_cpm'] += cpm
                            not_rendered_bids['category_cpm']+=cpm
                            not_rendered_bids['category_count']+=1
                        else: 
                            not_rendered_bids[bidder][category][adslot] = {'adslot_count': 1, 'adslot_cpm':cpm}
                    
        
                    else: 
                        not_rendered_bids[bidder][category] = {'adslot_count': 1, 'adslot_cpm':cpm, 'category_count':1, 'category_cpm':cpm}
                        not_rendered_bids[bidder]['bidder_count']+=1
                        not_rendered_bids[bidder]['bidder_cpm']+=cpm
                        


                else: 
                    not_rendered_bids[bidder] = {   category: { 'adslot_count': 1, 
                                                                'adslot_cpm':cpm
                                                                },

                                                    'bidder_count' :1, 
                                                    'bidder_cpm':cpm
               
                                                }
        bidders_winning = {}
        winning_bidders = []
        all_wining_bidders = []
        # df = pd.DataFrame('')
        adslot_bidders = {}
        all_adslots = [] 
        for bidder in bidder_adslot_rendered:
            if bidder not in all_wining_bidders:
                all_wining_bidders.append(bidder)
            for adslot in bidder_adslot_rendered[bidder]:
                if adslot not in all_adslots: 
                    all_adslots.append(adslot)
                cpm = bidder_adslot_rendered[bidder][adslot]['cpm']
                count = bidder_adslot_rendered[bidder][adslot]['count']
                if adslot in adslot_bidders: 
                    if bidder in adslot_bidders[adslot]: 
                        adslot_bidders[adslot][bidder] += count
                    else: 
                        adslot_bidders[adslot][bidder] = count
                else: 
                    adslot_bidders[adslot]= {bidder:count}

        rows = []
        total_bids_in_slot = 0
        total_cpm = 0
        columns = pd.MultiIndex.from_product([all_wining_bidders, ['win%_in_slot', 'cpm_spent_in_slot', 'total_bids_in_slot']])
        df = pd.DataFrame(rows, index=all_adslots, columns=columns)
        for bidder in bidder_adslot_rendered: 
            total_bids_in_slot = 0
            bidder_winning_pct = {}
            total_cpm_in_slot = 0
            
            for adslot in bidder_adslot_rendered[bidder]:
                total_bids_in_slot += bidder_adslot_rendered[bidder][adslot]['count']
                total_cpm_in_slot += bidder_adslot_rendered[bidder][adslot]['cpm']
            for bidder in adslot_bidders[adslot]: 
                cpm = bidder_adslot_rendered[bidder][adslot]['cpm']
                bidder_count = bidder_adslot_rendered[bidder][adslot]['count']
                bidder_winning_pct[bidder] = float(bidder_count) / float(total_bids_in_slot)
                # self.DBG.warning('adslot: {}\nbidder {}\nbidder count {}\ntotal bids in slot {}\nBidder PCT {}'.format(adslot, bidder, bidder_count, total_bids_in_slot, bidder_winning_pct[bidder]))
                df.at[adslot, (bidder, 'win%_in_slot')] = bidder_winning_pct[bidder]
                df.at[adslot, (bidder, 'cpm_spent_in_slot')] = cpm
                df.at[adslot, (bidder, 'total_bids_in_slot')] = bidder_count


            rows.append([adslot, bidder_winning_pct])
        df.fillna(0.0, inplace=True)
        df.to_csv('results/winning_bidders.csv')
        print(df) 

        

    def dict_to_csv(self, data, columns="", index="", output=''): 
       
        df = pd.DataFrame(index=index)
        df = df.from_dict(data)

        df= df.transpose() 
        df1=pd.DataFrame()
        if index: 
            df1 = pd.DataFrame(df, index=index)
            df1.fillna(0.0, inplace=True)  
            df1.to_csv(output, index=index)
        else: 
            df1 = pd.DataFrame(df)
            df1.fillna(0.0, inplace=True)  

            df1.to_csv(output, index=False)

        self.DBG.warning("{}".format(df1))

        self.DBG.warning("saving {}".format(output))
        

    def get_bid_stats(self):
        no_intent_path = 'bids_no_intent.json'
        intent_path = 'bids_intent.json'
        winning_no_intent_path = 'winning_bids_no_intent.json'
        no_intent = []
        intent = []
        winning_no_intent = []
        bid_stats_ni = {}
        bid_stats_i = {}
        bids_stats_winning_ni = {}
        with open(no_intent_path) as f: 
            no_intent = json.load(f)
        with open(winning_no_intent_path) as f: 
            winning_no_intent = json.load(f)

        for advertiser in winning_no_intent:     
            for category in winning_no_intent[advertiser]:
                less_than_10 = False
                # cleaned_bids = self.remove_low_bids(no_intent[advertiser][category], advertiser, category, 'no_intent', scipy.median(no_intent[advertiser][category]))
                cleaned_bids = self.remove_outliers(winning_no_intent[advertiser][category], advertiser, category, 'no_intent')

                mean_bids = scipy.mean(cleaned_bids)
                std_bids = scipy.std(cleaned_bids)
                median_bids = scipy.median(cleaned_bids)
                
                if len(winning_no_intent[advertiser][category])  <= 10: 
                    less_than_10 = True
                if advertiser in bids_stats_winning_ni:
                    bids_stats_winning_ni[advertiser].update({category: {'mean':mean_bids, "std_dev":std_bids, 'median':median_bids,'less_than_10':less_than_10}})
                else:
                    bids_stats_winning_ni[advertiser] = {category: {'mean':mean_bids, "std_dev":std_bids, 'median':median_bids,'less_than_10':less_than_10}}

        for advertiser in no_intent:     
            for category in no_intent[advertiser]:
                less_than_10 = False
                # cleaned_bids = self.remove_low_bids(no_intent[advertiser][category], advertiser, category, 'no_intent', scipy.median(no_intent[advertiser][category]))
                cleaned_bids = self.remove_outliers(no_intent[advertiser][category], advertiser, category, 'no_intent')

                mean_bids = scipy.mean(cleaned_bids)
                std_bids = scipy.std(cleaned_bids)
                median_bids = scipy.median(cleaned_bids)
                
                if len(no_intent[advertiser][category])  <= 10: 
                    less_than_10 = True
                if advertiser in bid_stats_ni:
                    bid_stats_ni[advertiser].update({category: {'mean':mean_bids, "std_dev":std_bids, 'median':median_bids,'less_than_10':less_than_10}})
                else:
                    bid_stats_ni[advertiser] = {category: {'mean':mean_bids, "std_dev":std_bids, 'median':median_bids,'less_than_10':less_than_10}}
                # print("adv: {}\ncategory: {}\nintent: {}\nmean: {}\nstd: {}\ncleaned_bids: {}\n".format(advertiser, category, 'no_intent', mean_bids, std_bids, sorted(cleaned_bids)))

        with open(intent_path) as f: 
            intent = json.load(f)
        for advertiser in intent:     
            for category in intent[advertiser]:
                less_than_10 = False

                # cleaned_bids = self.remove_low_bids(intent[advertiser][category], advertiser, category, 'intent', scipy.median(intent[advertiser][category]))
                cleaned_bids = self.remove_outliers(intent[advertiser][category], advertiser, category, 'intent')
                
                mean_bids = scipy.mean(cleaned_bids)
                std_bids = scipy.std(cleaned_bids)
                median_bids = scipy.median(cleaned_bids)

                if len(intent[advertiser][category])  <= 10: 
                    less_than_10 = True                
                if advertiser in bid_stats_i:
                    bid_stats_i[advertiser].update({category: {'mean':mean_bids, "std_dev":std_bids, 'median':median_bids,'less_than_10':less_than_10}})
                else:
                    bid_stats_i[advertiser] = {category: {'mean':mean_bids, "std_dev":std_bids, 'median':median_bids,'less_than_10':less_than_10}}
                # print("adv: {}\ncategory: {}\nintent: {}\nmean: {}\nstd: {}\ncleaned_bids: {}\n".format(advertiser, category, 'intent', mean_bids, std_bids, sorted(cleaned_bids)))
        with open('no_intent_stats.json', 'w') as f:
            json.dump(bid_stats_ni, f, indent=4, separators=(',',':')) 
        with open('winning_no_intent_stats.json', 'w') as f:
            json.dump(bids_stats_winning_ni, f, indent=4, separators=(',',':')) 


        with open('intent_stats.json', 'w') as f:
            json.dump(bid_stats_i, f, indent=4, separators=(',',':')) 
   
  
            
    def table_bids_mean_std_dev(self):
        intent_bids = []
        no_intent_bids = []
        with open('intent_stats.json') as f: 
            intent_bids = json.load(f)
        with open('no_intent_stats.json') as f: 
            no_intent_bids = json.load(f)
        rows = []
        for bidder in self.all_bidders:
            line = ""
            if bidder in no_intent_bids: 
                line += "{}& ".format(bidder)
                for category in self.cols: 
                    if category in no_intent_bids[bidder]:
                        line += "{}& {}& ".format(round(no_intent_bids[bidder][category]['mean'], 4),
                                                  round(no_intent_bids[bidder][category]['std_dev'], 4))
                    else:
                        line += "{}& {}& ".format(0.0, 0.0)
                line = line[:-2]+"\\hline"
                rows.append(line)
                
            if bidder in intent_bids: 
                line = ""
                line += "{}& ".format(bidder)
                for category in self.cols: 
                    if category in intent_bids[bidder]:
                        line += "{}& {}& ".format(round(intent_bids[bidder][category]['mean'], 4),
                                                round(intent_bids[bidder][category]['std_dev'], 4))
                    else:
                        line += "{}& {}& ".format(0.0, 0.0)
                line = line[:-2]+"\\hline"
                line = line.replace("&", "&\\cellcolor{light-gray}", 32)
                rows.append(line)
        # print('\nBids CPM Mean and Std. Dev. Summary Table\n--------------------------------------')                                              
        # for row in rows: 
        #     print(row)
    def table_summary_count(self):
        rows = []
        table = ""
        
        for bidder in self.all_bidders: 
            line=""
            line+='{}& {}& '.format(bidder, len(self.bidder_sites[bidder]))
            #No Intent Column, positive bid
            if bidder in self.bidders_count: 
                if 'intent' in self.bidders_count[bidder]: 
                    line+='{}& '.format(self.bidders_count[bidder]['no_intent'])
                else: 
                    line+='{}& '.format(0)
            else: 
                line+='{}& '.format(0)
            
            #No Intent Column, zero bid
            if bidder in self.bidders_zero_count:
                if 'intent' in self.bidders_zero_count[bidder]: 
                    line+='{}& '.format(self.bidders_zero_count[bidder]['no_intent'])
                else: 
                    line+='{}& '.format(0)
            else: 
                line+='{}& '.format(0)
            #Intent Column, postive bid
            if bidder in self.bidders_count: 
                if 'intent' in self.bidders_count[bidder]: 
                    line+='{}& '.format(self.bidders_count[bidder]['intent'])
                else: 
                    line+='{}& '.format(0)
            else: 
                    line+='{}& '.format(0)
            #Intent Column, zero bid
            if bidder in self.bidders_zero_count: 
                if 'intent' in self.bidders_zero_count[bidder]: 
                    line+='{} \\hline'.format(self.bidders_zero_count[bidder]['intent'])
                else: 
                    line+='{} \\hline'.format(0)
            else: 
                line+='{} \\hline'.format(0)



            
            rows.append(line)
        rows.append('Total & - &{}& {}& {}& {} \\hline'.format(self.no_intent_bid_count, 
                                                            self.zero_bids_count_no_intent,
                                                            self.intent_bid_count,
                                                            self.zero_bids_count_intent))
        print('\nBid Count Summary Table\n--------------------------------------')                                              
        for row in rows: 
            print(row)
            
    def table_with_arrows(self, df, table_name='table.txt'): 
        category = {}
        rows = []
        black_arrow = {}
        red_arrow = {}
        black_arrow["up"] = "$^\\uparrow$"
        black_arrow["down"] = "$^\\downarrow$"
        red_arrow["up"] = "\\boldred{$^\\uparrow$}"
        red_arrow["down"] = "\\boldred{$^\\downarrow$}"
        line = "{:>2}& ".format("Categories")

        for bidder in df.columns:
            line+="{:>2}& ".format(bidder)
        line = line[:-2]
        line+= "\\\\\n"
        
        rows.append(line)

        for category in df.index:
            line = "{}& ".format(category)
            """
            Significance of category row values
            mu: (row: category, col: Avg.)
            sigma: (row: category: col: Std.)
            """
            cat_mu = float(df.at[category, 'Avg.'])
            cat_sigma = float(df.at[category, 'Std.'])
            cat_sigma_plus = cat_mu + cat_sigma
            cat_sigma_minus = cat_mu - cat_sigma

            """
            Significance of category Avg. column values
            mu: (row: Avg, col: Avg.)
            sigma: (row: Std: col: Avg.)
            """
            cat_mu_avg = float(df.at['Avg.', 'Avg.'])
            cat_sigma_avg = float(df.at['Std.', 'Avg.'])
            cat_mu_avg_sigma_plus = cat_mu_avg+cat_sigma_avg
            cat_mu_avg_sigma_minus = cat_mu_avg-cat_sigma_avg

            """
            Significance of category Std. column values
            mu: (row: Avg, col: Avg.)
            sigma: (row: Std: col: Avg.)
            """
            cat_mu_std = float(scipy.mean(df['Std.']))
            cat_sigma_std = float(scipy.std(df['Std.']))
            cat_mu_std_sigma_plus = cat_mu_std+cat_sigma_std
            cat_mu_std_sigma_minus = cat_mu_std-cat_sigma_std      

            """
            Significance of Bidder row. column values
            mu: (row: Avg, col: Std.)
            sigma: (row: Std: col: Std.)
            """
            bidder_mu_avg = float(df.at['Avg.', 'Std.'])
            bidder_sigma_avg = float(df.at['Std.', 'Std.'])
            bidder_mu_avg_sigma_plus = bidder_mu_avg+bidder_sigma_avg
            bidder_mu_avg_sigma_minus = bidder_mu_avg-bidder_sigma_avg

            for bidder in df.columns:
                """
                Significance of Bidder col. values
                mu: (row: Avg, col: bidder)
                sigma: (row: Std: col: bidder)
                """
                bidder_mu = float(df.at['Avg.', bidder])
                bidder_sigma = float(df.at['Std.', bidder])
                bidder_sigma_plus = bidder_mu + bidder_sigma
                bidder_sigma_minus = bidder_mu - bidder_sigma

                """
                Significance of Bidder Avg. col values
                mu: (row: Avg., col: bidder)
                sigma: (row: Std.: col: bidder)
                """
                bidder_mu_avg = float(df.at['Avg.', 'Std.'])
                bidder_sigma_avg = float(df.at['Std.', 'Std.'])
                bidder_mu_avg_sigma_plus = bidder_mu_avg + bidder_sigma_avg
                bidder_mu_avg_sigma_minus = bidder_mu_avg - bidder_sigma_avg
                
                """
                Significance of Bidder Avg. col values
                mu: (row: Avg., col: bidder)
                sigma: (row: Std.: col: bidder)
                """
                bidder_mu_std = scipy.mean(float(df.at['Std.', 'Std.']))
                bidder_sigma_std = scipy.std(float(df.at['Std.', 'Std.']))
                bidder_mu_std_sigma_plus = bidder_mu_std + bidder_sigma_std
                bidder_mu_std_sigma_minus = bidder_mu_std - bidder_sigma_std

                cpm = float(df.at[category, bidder])
                
                """
                Bottom right 4 cells
                """
                if category in ['Avg.'] and bidder in ['Avg.']: 
                    line+="\\boldred{"
                    cpm = round(cpm, 2)
                    cpm_str = str(cpm)
                    if len(cpm_str) < 4: 
                        cpm_str+='0'
                    line+= "{}".format(cpm_str)
                    
                    line+="}"
                elif category in ['Avg.'] and bidder in ['Std.']: 
                    line+="\\textbf{"
                    cpm = round(cpm, 2)
                    cpm_str = str(cpm)
                    if len(cpm_str) < 4: 
                        cpm_str+='0'
                    line+= "{}".format(cpm_str)
                    line+="} "
                elif category in ['Std.'] and bidder in ['Avg.']: 
                    line+="\\boldred{"
                    cpm = str(round(cpm, 2))
                    if len(cpm) < 4: 
                        cpm+='0'
                    line+= "{}".format(cpm)
                    line+="}"
                elif category in ['Std.'] and bidder in ['Std.']: 
                    line+="\\textbf{"
                    cpm = round(cpm, 2)
                    cpm_str = str(cpm)
                    if len(cpm_str) < 4: 
                        cpm_str+='0'
                    line+= "{}".format(cpm_str)
                    line+="} "

                

                elif category in ['Avg.']: 
                    cpm = round(cpm, 2)
                    cpm_str = str(cpm)
                    if len(cpm_str) < 4: 
                        cpm_str+='0'
                    line+= "{}".format(cpm_str)
                    """
                    Avg. row
                    
                    Avg. - category -  red up arrow
                    """
                    if cpm > bidder_mu_avg_sigma_plus: 
                        line+=black_arrow['up']
                    """                    
                    Avg. - category -  red down arrow
                    """
                    if cpm < bidder_mu_avg_sigma_minus: 
                        line+=black_arrow['down']

                 
                elif category in ['Std.']: 
                    cpm = round(cpm, 2)
                    cpm_str = str(cpm)
                    if len(cpm_str) < 4: 
                        cpm_str+='0'
                    line+= "{}".format(cpm_str)
                    """
                    Std. Row
                    Std. - category -  red up arrow
                    """
                    if cpm > bidder_mu_std_sigma_plus: 
                        line+=black_arrow['up']
                    """
                    Std. - category -  red down arrow
                    """
                    if cpm < bidder_mu_std_sigma_minus: 
                        line+=black_arrow['down']

                elif bidder in ['Avg.']: 
                    cpm = round(cpm, 2)
                    cpm_str = str(cpm)
                    if len(cpm_str) < 4: 
                        cpm_str+='0'
                    line+= "{}".format(cpm_str)
                    """
                    Avg. Column

                    Avg. - bidder -  red up arrow
                    """
                    if cpm > cat_mu_avg_sigma_plus: 
                        line+=red_arrow['up']
                    """
                    Avg. - bidder -  red down arrow
                    """                            
                    if cpm < cat_mu_avg_sigma_minus: 
                        line+=red_arrow['down']
               
                elif bidder in ['Std.']: 
                    cpm = round(cpm, 2)
                    cpm_str = str(cpm)
                    if len(cpm_str) < 4: 
                        cpm_str+='0'
                    line+= "{}".format(cpm_str)
                    """
                    Std. Column

                    Std. - bidder -  red up arrow
                    """
                    if cpm > cat_mu_std_sigma_plus: 
                        line+=red_arrow['up']
                    """
                    Std. - bidder -  red down arrow
                    """                            
                    if cpm < cat_mu_std_sigma_minus: 
                        line+=red_arrow['down']
                
                else: 
                    cpm = round(cpm, 2)
                    cpm_str = str(cpm)
                    if len(cpm_str) < 4: 
                        cpm_str+='0'
                    line+= "{}".format(cpm_str)
                    """
                    CPM. Interior Table
                    CPM. - category - red uparrow
                    """ 
                    if cpm > cat_sigma_plus: 
                        line+=red_arrow['up']
                    if cpm < cat_sigma_minus: 
                        line+=red_arrow['down']
                    if cpm > bidder_sigma_plus: 
                        line+=black_arrow['up']
                    if cpm < bidder_sigma_minus: 
                        line+=black_arrow['down']
                line += "{:>2} ".format('&')
            line = line[:-2]
            line+= "\\\\\n"
            rows.append(line)
        with open('results/{}'.format(table_name), 'w') as f: 

            for row in rows: 
                f.write(row)

    def table_weighted_means(self, values_df, weights_df, columns, rows):
        row_weighted_means = []
        row_weighted_stds = []
        col_weighted_means = []
        col_weighted_stds = []

        for row in rows:
            if row in ['Avg.', 'Std.']:
                continue
            row_values = values_df.loc[[row]].to_numpy()
            row_values = np.squeeze(np.asarray(row_values))
            row_weights = weights_df.loc[[row]].to_numpy()
            row_weights = np.squeeze(np.asarray(row_weights))

            mean, std = self.weighted_avg_and_std(row_values, row_weights)


            row_weighted_means.append(mean)
            row_weighted_stds.append(std)
        
        column_mean_weighted_means = scipy.mean(row_weighted_means)
        column_std_weighted_means = scipy.std(row_weighted_means)
        values_df['Avg.'] = row_weighted_means
        values_df['Std.'] = row_weighted_stds

        for col in columns:
            if col in ['Avg.', 'Std.']:
                continue 
            col_values = values_df[col].to_numpy()
            col_values = np.squeeze(np.asarray(col_values))
            col_weights = weights_df[col].to_numpy()
            col_weights = np.squeeze(np.asarray(col_weights))
            mean, std = self.weighted_avg_and_std(col_values, col_weights)
            col_weighted_means.append(mean)
            col_weighted_stds.append(std)
        row_mean_weighted_means = scipy.mean(col_weighted_means)
        row_std_weighted_stds = scipy.mean(col_weighted_stds)
        

        mean_row = col_weighted_means
        mean_row.append(column_mean_weighted_means)
        mean_row.append(row_mean_weighted_means)

        # self.DBG.warning("{}, {}".format(mean_row, mean_row.__len__()))
        mean_row = np.array(mean_row)
        mean_row = np.reshape(mean_row, (1,len(columns)+2))


        std_row = col_weighted_stds
        std_row.append(column_std_weighted_means)
        std_row.append(row_std_weighted_stds)

        std_row = np.array(std_row)
        std_row = np.reshape(std_row, (1,len(columns)+2))
        # self.DBG.warning(self.no_intent_median_cpms_df)

        row_mean_df = pd.DataFrame(mean_row,columns=values_df.columns)
        row_std_df = pd.DataFrame(std_row,columns=values_df.columns)
        self.DBG.warning(values_df)

        values_df = pd.concat([row_mean_df, values_df])
        values_df.rename({0:"Avg."}, inplace=True)

        values_df = pd.concat([row_std_df, values_df])
        values_df.rename({0:"Std."}, inplace=True)
        self.DBG.warning(values_df)
        index = self.cols
        if 'Avg.' not in index: 
            index.append('Avg.')
        if 'Std.' not in index: 
            index.append('Std.')

        self.DBG.warning(index)     
          
        values_df = values_df.reindex(index)
        values_df.fillna(0.0, inplace=True)
        return values_df.copy()

            
    def table_median_bids(self): 
        intent_bids = []
        no_intent_bids = []
        winning_no_intent_bids = []
    
        with open('no_intent_stats.json') as f: 
            no_intent_bids = json.load(f)
        with open('winning_no_intent_stats.json') as f: 
            winning_no_intent_bids = json.load(f)
        with open('intent_stats.json') as f: 
            intent_bids = json.load(f)

        self.no_intent_median_cpms_df = pd.DataFrame(index=self.cols, columns=self.top_5_bidders)
        self.no_intent_bid_counts_df = pd.DataFrame(index=self.cols, columns=self.top_5_bidders)
        self.winning_no_intent_cpms_df = pd.DataFrame(index=self.cols, columns=self.top_5_bidders)
        self.winning_no_intent_bid_count_df = pd.DataFrame(index=self.cols, columns=self.top_5_bidders)
        self.intent_median_cpms_df = pd.DataFrame(index=self.cols, columns=self.top_5_bidders)
        self.intent_bid_counts_df = pd.DataFrame(index=self.cols, columns=self.top_5_bidders)
        self.bid_count_ratios_df = pd.DataFrame(index=self.cols, columns=self.top_5_bidders)
        self.cpm_ratios_df = pd.DataFrame(index=self.cols, columns=self.top_5_bidders)

        self.no_intent_median_cpms_df.fillna(0.0, inplace=True)
        self.no_intent_bid_counts_df.fillna(0.0, inplace=True)
        self.winning_no_intent_cpms_df.fillna(0.0, inplace=True)
        self.winning_no_intent_bid_count_df.fillna(0.0, inplace=True)
        self.intent_median_cpms_df.fillna(0.0, inplace=True)
        self.intent_bid_counts_df.fillna(0.0, inplace=True)
        self.bid_count_ratios_df.fillna(0.0, inplace=True)  
        self.cpm_ratios_df.fillna(0.0, inplace=True) 

        for bidder in winning_no_intent_bids:
            if bidder not in self.top_5_bidders: 
                    continue
            for cat in winning_no_intent_bids[bidder]:
                self.winning_no_intent_cpms_df.at[cat, bidder] = winning_no_intent_bids[bidder][cat]['median']

        for bidder in no_intent_bids:
            if bidder not in self.top_5_bidders: 
                    continue
            for cat in no_intent_bids[bidder]:
                self.no_intent_median_cpms_df.at[cat, bidder] = no_intent_bids[bidder][cat]['median']

        for bidder in intent_bids:
            if bidder not in self.top_5_bidders: 
                    continue
            for cat in intent_bids[bidder]:
                self.intent_median_cpms_df.at[cat, bidder] = intent_bids[bidder][cat]['median']
       
        for bidder in self.top_5_bidders: 
            for category in self.cols: 
               
                """
                Intent/No intent Counts
                """
                if category in self.all_bidders_count[bidder]:
                    if 'no_intent' in self.all_bidders_count[bidder][category]:
                        if 'pos_bid' in self.all_bidders_count[bidder][category]['no_intent']:
                            self.no_intent_bid_counts_df.at[category, bidder] = self.all_bidders_count[bidder][category]['no_intent']['pos_bid']
                        else: 
                            self.no_intent_bid_counts_df.at[category, bidder] = self.all_bidders_count[bidder][category]['no_intent']['zero_bid']
                    if 'intent' in self.all_bidders_count[bidder][category]:
                        if 'pos_bid' in self.all_bidders_count[bidder][category]['intent']:
                            self.intent_bid_counts_df.at[category, bidder] = self.all_bidders_count[bidder][category]['intent']['pos_bid']
                        else: 
                            self.intent_bid_counts_df.at[category, bidder] = self.all_bidders_count[bidder][category]['intent']['zero_bid']
                if category in self.winning_no_intent_bidders_count[bidder]:
                    if 'no_intent' in self.winning_no_intent_bidders_count[bidder][category]:
                        if 'pos_bid' in self.all_bidders_count[bidder][category]['no_intent']:
                            self.winning_no_intent_bid_count_df.at[category, bidder] = self.winning_no_intent_bidders_count[bidder][category]['no_intent']['pos_bid']
                        else: 
                            self.winning_no_intent_bid_count_df.at[category, bidder] = self.winning_no_intent_bidders_count[bidder][category]['no_intent']['pos_bid']
                                            
      

        for bidder in self.top_5_bidders: 
            # if bidder in ['Avg.', 'Std.']:
            #         continue
            for category in self.cols: 
                if category in ['Avg.', 'Std.']:
                    continue
                ratio = 0.0
                no_intent_cpm = 0.0
                intent_cpm = 0.0
                """
                CPM Ratios
                """
                if bidder in self.no_intent_median_cpms_df: 
                    no_intent_cpm = self.no_intent_median_cpms_df.at[category, bidder]
                if bidder in self.intent_median_cpms_df: 
                    intent_cpm = self.intent_median_cpms_df.at[category, bidder]
                if no_intent_cpm and intent_cpm:
                    ratio = float(intent_cpm) / float(no_intent_cpm)

                # self.DBG.warning(self.cpm_ratios_df)
                # self.DBG.warning('{},{},{},{},{}'.format(category, bidder, ratio, intent_cpm, no_intent_cpm ))

                self.cpm_ratios_df.at[category, bidder] = ratio
            


        for bidder in self.top_5_bidders: 
            if bidder in ['Avg.', 'Std.']:
                    continue
            for category in self.cols:  
                if category in ['Avg.', 'Std.']:
                    continue
                # self.DBG.warning("{}, {}".format(category, bidder))
                ratio = 0.0
                intent_count= 0.0
                no_intent_count = 0.0
                """
                Bid Count Ratios
                """
                if bidder in self.no_intent_bid_counts_df: 
                    no_intent_count = self.no_intent_bid_counts_df.at[category, bidder]
                if bidder in self.intent_bid_counts_df: 
                    intent_count = self.intent_bid_counts_df.at[category, bidder]
                if intent_count and no_intent_count:
                    ratio = float(intent_count) / float(no_intent_count)
                self.bid_count_ratios_df.at[category, bidder] = ratio



        
        self.DBG.warning('\nTop 5 No-Intent Bid CPM \n--------------------------------------')         
        self.DBG.warning(self.no_intent_median_cpms_df)

        self.DBG.warning('\nTop 5 No-Intent Bid Counts \n--------------------------------------')         
        self.DBG.warning(self.no_intent_bid_counts_df)
        
        self.DBG.warning('\nTop 5 Intent Bid CPM \n--------------------------------------')         
        self.DBG.warning(self.intent_median_cpms_df)

        self.DBG.warning('\nTop 5 Intent Bid Counts \n--------------------------------------')         
        self.DBG.warning(self.intent_bid_counts_df)

        self.DBG.warning('\nTop 5 Winning No-Intent Bid CPM \n--------------------------------------')         
        self.DBG.warning(self.winning_no_intent_cpms_df)

        self.DBG.warning('\nTop 5 Winning No-Intent Bid Counts \n--------------------------------------')         
        self.DBG.warning(self.winning_no_intent_bid_count_df)  
        
        self.DBG.warning('\nTop 5 CPM Ratios\n--------------------------------------')         
        self.DBG.warning(self.cpm_ratios_df)


        self.DBG.warning('\nTop 5 Bid Count Ratios\n--------------------------------------')         
        self.DBG.warning(self.bid_count_ratios_df) 


 
        self.table3 = self.table_weighted_means(self.no_intent_median_cpms_df, self.no_intent_bid_counts_df, self.top_5_bidders, self.cols )    
        self.table4 = self.table_weighted_means(self.cpm_ratios_df, self.bid_count_ratios_df, self.top_5_bidders, self.cols)
        self.table_winning_bids = self.table_weighted_means(self.winning_no_intent_cpms_df, self.winning_no_intent_bid_count_df, self.top_5_bidders, self.cols)

        self.DBG.warning('\nTable 3 - median CPM bids \n--------------------------------------')    

        self.table3.rename(index={'Kids_and_Teens':"Kids&Teens"}, inplace=True)
        self.table3.rename(index={'BLOCK':"Control"}, inplace=True)
        self.table3.rename(columns={'appnexus':"App."}, inplace=True)
        self.table3.rename(columns={'rubicon':"Rub."}, inplace=True)
        self.table3.rename(columns={'ix':"IX"}, inplace=True)
        self.table3.rename(columns={'openx':"OpX."}, inplace=True)
        self.table3.rename(columns={'pubmatic':"Pub."}, inplace=True)
        self.DBG.warning(self.table3)
        self.table_with_arrows(self.table3, table_name='table3.txt')
        
        self.DBG.warning('\nTable 4 - CPM Ratios \n--------------------------------------')    
   
        self.table4.rename(index={'Kids_and_Teens':"Kids&Teens"}, inplace=True)
        self.table4.rename(index={'BLOCK':"Control"}, inplace=True)
        self.table4.rename(columns={'appnexus':"App."}, inplace=True)
        self.table4.rename(columns={'rubicon':"Rub."}, inplace=True)
        self.table4.rename(columns={'ix':"IX"}, inplace=True)
        self.table4.rename(columns={'openx':"OpX."}, inplace=True)
        self.table4.rename(columns={'pubmatic':"Pub."}, inplace=True)
        self.DBG.warning(self.table4)
        self.table_with_arrows(self.table4, table_name='table4.txt')

        self.DBG.warning('\nTable Winning Bids median CPMS \n--------------------------------------')    
   
        self.table_winning_bids.rename(index={'Kids_and_Teens':"Kids&Teens"}, inplace=True)
        self.table_winning_bids.rename(index={'BLOCK':"Control"}, inplace=True)
        self.table_winning_bids.rename(columns={'appnexus':"App."}, inplace=True)
        self.table_winning_bids.rename(columns={'rubicon':"Rub."}, inplace=True)
        self.table_winning_bids.rename(columns={'ix':"IX"}, inplace=True)
        self.table_winning_bids.rename(columns={'openx':"OpX."}, inplace=True)
        self.table_winning_bids.rename(columns={'pubmatic':"Pub."}, inplace=True)
        self.DBG.warning(self.table_winning_bids)
        self.table_with_arrows(self.table_winning_bids, table_name='table_winning_bids.txt')


    def table_full_count(self):
        rows = []
        for bidder in self.all_bidders: 
            for intent in ['no_intent', 'intent']:
                line=""
                line+="{}& {} &".format(bidder, self.bidder_sites[bidder])
                for category in self.cols:   
                    if category in self.all_bidders_count[bidder]: 
                        
                            if 'no_intent' == intent:      
                                if intent in self.all_bidders_count[bidder][category]:                      
                                    if 'pos_bid' in self.all_bidders_count[bidder][category]['no_intent']:
                                        line+="{}& ".format(self.all_bidders_count[bidder][category]['no_intent']['pos_bid'])
                                    else: 
                                        line+="{}& ".format(0)
                                    if 'zero_bid' in self.all_bidders_count[bidder][category]['no_intent']: 
                                        line+="{}& ".format(self.all_bidders_count[bidder][category]['no_intent']['zero_bid'])
                                    else: 
                                        line+="{}& ".format(0)
                                else: 
                                    line+="{}& {}& ".format(0,0)
                            
                            if 'intent' == intent:
                                if intent in self.all_bidders_count[bidder][category]:
                                    if 'pos_bid' in self.all_bidders_count[bidder][category]['intent']:
                                        line+="{}& ".format(self.all_bidders_count[bidder][category]['intent']['pos_bid'])
                                    else: 
                                        line+="{}& ".format(0)
                                    if 'zero_bid' in self.all_bidders_count[bidder][category]['intent']: 
                                        line+="{}& ".format(self.all_bidders_count[bidder][category]['intent']['zero_bid'])
                                    else: 
                                        line+="{}& ".format(0)      
                                else: 
                                    line+="{}& {}& ".format(0,0)
                    else: 
                        line+="{}& {}& ".format(0, 0)
            
                
                line = line[:-2]+"\\hline"
                rows.append(line)
        
        print('\nBids Full Count Table\n--------------------------------------')                                              
        for row in rows: 
            print(row)
    def chi_sq_test_population(self):
        pass

    def table_zero_bids(self): 
        self.zero_bids_df = pd.DataFrame(index=self.bidders_zero_count.keys(), columns=["No-Intent","Intent", "Total"])
        self.zero_bids_df.fillna(0.0, inplace=True)
        bidder_zero_bid_count_no_intent = {}
        bidder_zero_bid_count_intent = {}
        total_no_intent_zero_bid_count = 0.0
        total_intent_zero_bid_count = 0.0

  

        significant_zero_bids = {}
        for bidder in self.all_bidders: 
            significant_zero_bids[bidder] = ""
            no_intent_zero_bid_count = 0
            intent_zero_bid_count = 0

            if bidder in self.bidders_zero_count:
                if 'intent' in self.bidders_zero_count[bidder]: 
                    intent_zero_bid_count = self.bidders_zero_count[bidder]['intent']
                    total_intent_zero_bid_count+=self.bidders_zero_count[bidder]['no_intent']
                if 'intent' in self.bidders_zero_count[bidder]: 
                    no_intent_zero_bid_count = self.bidders_zero_count[bidder]['no_intent']
                    total_no_intent_zero_bid_count+=self.bidders_zero_count[bidder]['no_intent']

            # if bidder in self.all_bidders_count: 
            #     for category in self.cols:   
            #         if category in self.all_bidders_count[bidder]:
            #             for intent in ['no_intent', 'intent']:
            #                 if 'no_intent' == intent: 
                            
            #                     if intent in self.all_bidders_count[bidder][category]: 
            #                         if 'zero_bid' in self.all_bidders_count[bidder][category]['no_intent']:
            #                             no_intent_zero_bid_count += self.all_bidders_count[bidder][category]['no_intent']['zero_bid']
            #                             total_no_intent_zero_bid_count+=self.all_bidders_count[bidder][category]['no_intent']['zero_bid']
            #                         if 'pos_bid' in self.all_bidders_count[bidder][category]['no_intent']:
            #                             no_intent_pos_bid_count += self.all_bidders_count[bidder][category]['no_intent']['pos_bid']
            #                             total_no_intent_pos_bid_count+=self.all_bidders_count[bidder][category]['no_intent']['pos_bid']
            #                 else: 
            #                     if intent in self.all_bidders_count[bidder][category]: 
            #                         if 'zero_bid' in self.all_bidders_count[bidder][category]['intent']:
            #                             intent_zero_bid_count+= self.all_bidders_count[bidder][category]['intent']['zero_bid']
            #                             total_intent_zero_bid_count+=self.all_bidders_count[bidder][category]['intent']['zero_bid']
            #                         if 'pos_bid' in self.all_bidders_count[bidder][category]['intent']:
            #                             intent_pos_bid_count += self.all_bidders_count[bidder][category]['intent']['pos_bid']
            #                             total_intent_pos_bid_count+=self.all_bidders_count[bidder][category]['intent']['pos_bid']


                bidder_zero_bid_count_no_intent[bidder] = no_intent_zero_bid_count
                bidder_zero_bid_count_intent[bidder] = intent_zero_bid_count


        self.DBG.lt_green("bidder_zero_bid_count_no_intent['pubmatic']: {}".format(bidder_zero_bid_count_no_intent['pubmatic']))
        self.DBG.lt_green("total_no_intent_zero_bid_count: {}".format(total_no_intent_zero_bid_count))
        self.DBG.lt_green("bidder_zero_bid_count_intent['pubmatic']: {}".format(bidder_zero_bid_count_intent['pubmatic']))
        self.DBG.lt_green("total_intent_zero_bid_count: {}".format(total_intent_zero_bid_count))




        zero_bidders =  bidder_zero_bid_count_no_intent.keys() 
        for bidder in zero_bidders: 
            if bidder not in bidder_zero_bid_count_intent.keys(): 
                zero_bidders.pop(bidder)

        for bidder in zero_bidders: 
            no_intent_zero_count = bidder_zero_bid_count_no_intent[bidder]
            intent_zero_count = bidder_zero_bid_count_intent[bidder]
            total_zero_pct = 0.0
            if 0!= total_no_intent_zero_bid_count:
                self.zero_bids_df.at[bidder, 'No-Intent'] = float(bidder_zero_bid_count_no_intent[bidder]) / float(total_no_intent_zero_bid_count)
            if 0!= total_intent_zero_bid_count: 
                self.zero_bids_df.at[bidder, 'Intent'] = float(bidder_zero_bid_count_intent[bidder]) / float(total_intent_zero_bid_count)
            if 0!= total_no_intent_zero_bid_count+total_intent_zero_bid_count:
                total_zero_pct = float(bidder_zero_bid_count_no_intent[bidder] + bidder_zero_bid_count_intent[bidder]) / float(total_no_intent_zero_bid_count+total_intent_zero_bid_count)
            self.zero_bids_df.at[bidder, 'Total'] = total_zero_pct

            
            obs = np.array([[no_intent_zero_count, intent_zero_count], [total_no_intent_zero_bid_count, total_intent_zero_bid_count]])
            try:
                chi2, p, dof, ex = chi2_contingency(obs, correction=False)
                # self.DBG.lt_green("bidder: {} no_intent_zero: {} intent_zero: {} no_intent_zero_total: {} intent_zero_total: {}".format(bidder, 
                #                                                                                                                        no_intent_zero_count, 
                #                                                                                                                        intent_zero_count, 
                #                                                                                                                        total_no_intent_zero_bid_count,
                #                                                                                                                        total_intent_zero_bid_count))
                # # self.DBG.lt_green("bidder: {} chi2: {} p: {} dof: {} ex: {}".format(bidder, chi2, p, dof, ex))

                if p > 0.05: 
                    self.DBG.lt_green('SIGNIFCANT: bidder: {} p: {}'.format(bidder, p))
                    significant_zero_bids[bidder]="*"
                else:
                    self.DBG.lt_cyan('Not Significant: bidder: {} p: {}'.format(bidder, p))
                    
            except Exception as e: 
                self.DBG.red("Exception:{} -  bidder: {} no_intent_zero: {} intent_zero: {} no_intent_zero_total: {} intent_zero_total: {}".format(e, 
                                                                                                                                                   bidder, 
                                                                                                                                                   no_intent_zero_count, 
                                                                                                                                                   intent_zero_count, 
                                                                                                                                                   total_no_intent_zero_bid_count,
                                                                                                                                                   total_intent_zero_bid_count))
        no_intent_pct = total_no_intent_zero_bid_count / (total_no_intent_zero_bid_count+total_intent_zero_bid_count)
        intent_pct = total_intent_zero_bid_count / (total_no_intent_zero_bid_count+total_intent_zero_bid_count)
        index = self.zero_bids_df.index.to_list()
        index.append('Total')

        self.DBG.warning(index)     
        table6_zerobids_df = self.zero_bids_df.reindex(index)
        table6_zerobids_df.at['Total', 'No-Intent'] = no_intent_pct
        table6_zerobids_df.at['Total', 'Intent'] = intent_pct
        table6_zerobids_df.fillna(0.0, inplace=True)

        table6_zerobids_df.to_csv('results/table_zero_bids.csv')

        self.DBG.lt_cyan(table6_zerobids_df)
        with open('results/table_zero_bids.txt', 'w') as f: 
            for bidder in table6_zerobids_df.index: 
                bidder_str = str(bidder[0]).upper()+bidder[1:]
                no_intent = round(table6_zerobids_df.at[bidder, 'No-Intent'], 2)
                no_intent = self.str_fill_0(str(no_intent), 3)
                intent = round(table6_zerobids_df.at[bidder, 'Intent'], 2)
                intent = self.str_fill_0(str(intent), 3)

                total = round(table6_zerobids_df.at[bidder, 'Total'], 2)
                total = self.str_fill_0(str(total), 3)

                sig = ""
                if bidder == 'Total':
                    sig = ""
                else: 
                    sig = significant_zero_bids[bidder]
                f.write("{}{}& {:>8}& {:>8}& {:>8}\\\\\n".format(bidder_str, sig, no_intent, intent, total))



    def str_fill_0(self, num_str, dec):
        if len(num_str) < dec:
            num_str+="0"
        return num_str



    def bidders_site_table(self):
        line = ""
        rows = []
        bidders = []
        for bidder in self.bidder_sites: 
            bidders.append(bidder)
            line +="{} &".format(bidder)
        rows.append(line)
        self.site_bidders = {}
        for bidder in bidders: 
            for site in self.bidder_sites[bidder]: 
                if site in self.site_bidders: 
                    if bidder in self.site_bidders[site]: 
                        pass
                    else: 
                        self.site_bidders[site].update({bidder: self.bidder_sites[bidder][site]})
                else: 
                    self.site_bidders[site]= {bidder: self.bidder_sites[bidder][site]}
        
        
        
        
        for site in self.site_bidders:
            line=""
            line+="{}& ".format(site)
            for bidder in self.all_bidders:
                if bidder in self.site_bidders[site]:   
                    line+="{}& ".format(self.site_bidders[site][bidder])
                else: 
                    line+="{}& ".format(0)
            line = line[:-2]+'\\hline'
            rows.append(line)
        print('\nBidders site Table\n--------------------------------------')          
        for row in rows:                                    
            print(row)
                
    def create_tables(self):
        #gather all bidders from both intent and no intent bids
        #These bidders are sorted by table 1, no intent column postive bids in descending order
        self.all_bidders = []
        self.cols = ['Adult',
                     'Arts',
                     'Business',
                     'Computers',
                     'Games',
                     'Health',
                     'Home',
                     'Kids_and_Teens',
                     'News',
                     'Recreation',
                     'Reference',
                     'Regional',
                     'Science',
                     'Shopping',
                     'Society',
                     'Sports',
                     'BLOCK']

        # self.sorted_bidder_count = sorted(self.bidders_count.items(), key=operator.itemgetter(1), reverse=True)
        for bidder in self.bidders_count:
            self.all_bidders.append(bidder)

        for bidder in self.bidders_zero_count: 
            if bidder not in self.all_bidders: 
                self.all_bidders.append(bidder)

        # self.parse_winning_bids()
        self.table_bids_mean_std_dev()
        self.table_median_bids()
        self.table_zero_bids()
        self.table_summary_count()
        # self.table_full_count()
        # self.bidders_site_table()



     
                    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive_path')
    args=parser.parse_args()
    a = process_prebids(archive_path=args.archive_path)
    a.get_bid_stats()
    a.create_tables()
        