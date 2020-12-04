import os
import sys
import time
import json
import glob
import scipy
import tempfile
import operator
import itertools
import numpy as np
import pandas as pd
from argparse import ArgumentParser
from lcdk import lcdk as LeslieChow



#cite: https://www.tutorialspoint.com/python/os_walk.htm
class FileWalker: 
    def __init__(self, **kwargs):
        self.DBG = LeslieChow.lcdk(print_output=False)
        self.base_dir = kwargs.get('base_dir', '.')

    def get_paths(self):
        paths = []
        for root, dirs, files in os.walk(self.base_dir, topdown=False):
            for name in files:
                path = os.path.join(root, name)
                paths.append(path)
                # self.DBG.log(os.path.join(root, name))
        return paths  
    
    

class process_HB_bids: 
    def __init__(self, **kwargs):
        self.DBG = LeslieChow.lcdk()
        bids_directory = kwargs.get('bids_dir', None)
        # self.DBG.log(bids_directory)

        self.filewalker = FileWalker(base_dir=bids_directory).get_paths()
        # self.DBG.log(self.filewalker)
        

        self.bidders_count = {}
        self.bidders_zero_count = {}
        self.all_bidders_count = {}
        self.bidder_sites = {}
        
        self.zero_bids_count_intent = 0
        self.zero_bids_count_no_intent = 0
        
        self.no_intent_bid_count = 0
        self.intent_bid_count = 0
        i = 1
        file_count = self.filewalker.__len__()
        for f in self.filewalker:
            bids_dump = {}
            
            msg = "processing file {}".format(f)
            # self.DBG.log(msg)

            results_path = ""
            tmpdir = tempfile.mkdtemp()
            msg = "{} untarring".format(time.asctime())
            tar_path = os.path.join(bids_directory, f)
            self.DBG.log("processing tar file {}/{}".format(i, file_count))
            os.system('tar -xf {} -C {}'.format(tar_path, tmpdir))
            i+=1
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
                output = os.path.abspath(os.path.join('results', 'bids_no_intent.json'))
            else: 
                bids_path = os.path.join(results_path, 'bids_intent')
                output =  os.path.abspath(os.path.join('results', 'bids_intent.json'))
            if not os.path.exists(output):
                init = {}
                with open(output, 'w+') as f: 
                    json.dump(init, f)


            self.bids_files = os.listdir(bids_path)
            bidderSeenOnSite = False

            for bid_file in self.bids_files: 
                bids = []
                path = os.path.join(bids_path, bid_file)
                msg = "Retreiving bids {}".format(bid_file)
                # self.DBG.log(msg)
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
                                bidder = bid['bidder']
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
            if i >2: 
                break
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
        # self.DBG.log("removing {} bids from len: {} \n\nbids:{}\n\n".format(remove, len(bids), sorted(bids)))

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
         


    
    def remove_outliers(self, bids, advertiser, category, intent, threshold=0.01):
        return bids
        N =len(bids)
        sorted_bids = sorted(bids)
        if len(bids) < 10: 
            return bids
        remove = int(N*threshold)
        # self.DBG.log("removing {} bids from len: {} \n\nbids:{}\n\n".format(remove, len(bids), sorted(bids)))

        if remove <1: 
            remove = 1
        while remove > 0: 
            # self.DBG.log("removing bottom bid {}".format(sorted_bids[remove]))
            # self.DBG.log("removing top bid {}".format( sorted_bids[-remove]))

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

    def get_bid_stats(self):
        if not os.path.exists('results'): 
            os.mkdir('results')


        bids_no_intent_path = os.path.abspath(os.path.join('results', 'bids_no_intent.json'))
        bids_intent_path = os.path.abspath(os.path.join('results', 'bids_intent.json'))

        
        no_intent = []
        intent = []
        bid_stats_ni = {}
        bid_stats_i = {}
        with open(bids_no_intent_path) as f: 
            no_intent = json.load(f)
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
                # self.DBG.log("adv: {}\ncategory: {}\nintent: {}\nmean: {}\nstd: {}\ncleaned_bids: {}\n".format(advertiser, category, 'no_intent', mean_bids, std_bids, sorted(cleaned_bids)))
                    
        with open(bids_intent_path) as f: 
            intent = json.load(f)
        for advertiser in intent:     
            for category in intent[advertiser]:
                less_than_10 = False
                bids_before = intent[advertiser][category]

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
                # self.DBG.log("adv: {}\ncategory: {}\nintent: {}\nmean: {}\nstd: {}\ncleaned_bids: {}\n".format(advertiser, category, 'intent', mean_bids, std_bids, sorted(cleaned_bids)))

        intent_stats_path = os.path.abspath(os.path.join('results', 'intent_stats.json'))
        no_intent_stats_path = os.path.abspath(os.path.join('results', 'no_intent_stats.json'))
        init = {}
        with open(no_intent_stats_path, 'w+') as f: 
            json.dump(init, f)

        with open(no_intent_stats_path, 'w+') as f:
            json.dump(bid_stats_ni, f, indent=4, separators=(',',':')) 
            
        with open(intent_stats_path, 'w+') as f: 
            json.dump(init, f)
        with open(intent_stats_path, 'w+') as f:
            json.dump(bid_stats_i, f, indent=4, separators=(',',':')) 
     
    def table_summary_count(self):
        csv_dir = 'csv'
        rows = []
        table = ""
        cols = [['Intent', 'Intent'], ['No Intent', 'No Intent']]
        
        header = pd.MultiIndex.from_product([['Positive Bids', 'Zero Bids'],
                                ['Intent','No Intent']],
                                )
        summary = pd.DataFrame( 
                            columns=header)
        

        bidders = []
        

        pos = pd.DataFrame.from_dict(self.bidders_count).transpose()
        # pos.rename(index=str, columns={'intent':'Positive Intent', 'no_intent':'Positive No Intent'}, inplace=True)
        zero =  pd.DataFrame.from_dict(self.bidders_zero_count).transpose()
        # zero.rename(index=str, columns={'intent':'Zero Intent', 'no_intent':'Zero No Intent'}, inplace=True)
        for bidder in pos['intent'].index: 
            if bidder not in bidders: 
                bidders.append(bidder)
        for bidder in pos['no_intent'].index: 
            if bidder not in bidders: 
                bidders.append(bidder)
        for bidder in zero['intent'].index: 
            if bidders not in bidders: 
                bidders.append(bidder)
        for bidder in zero['no_intent'].index: 
            if bidder not in bidders: 
                bidders.append(bidder)



        header = pd.MultiIndex.from_product([['Positive Bids', 'Zero Bids'],
                                ['Intent','No Intent']],
                                names=['Bid Type', 'Bidders']
                                )
        summary = pd.DataFrame( 
                            columns=header,
                            index=bidders)                
        for bidder in bidders: 
            if bidder in pos.index: 
                summary.at[bidder, ('Positive Bids', 'Intent')] = pos.at[bidder, 'intent']
                summary.at[bidder, ('Positive Bids', 'No Intent')] = pos.at[bidder, 'intent']
            if bidder in zero.index: 
                summary.at[bidder, ('Zero Bids', 'Intent')] = zero.at[bidder, 'intent']
                summary.at[bidder, ('Zero Bids', 'No Intent')] = zero.at[bidder, 'intent']

        
        summary.fillna(0, inplace=True)
        print(summary)
        

        summary.sort_values(('Positive Bids', 'Intent'), inplace=True, ascending=False)
        path = os.path.join(csv_dir, 'summary.csv')
        # self.DBG.log(summary)

        summary.to_csv(path, index=False, index_label=('Positive Bids', 'Zero Bids'))

       
    def table_bid_value_stats(self):
        csv_dir = 'csv'
        intent_bids = []
        no_intent_bids = []
        with open('results/intent_stats.json') as f: 
            intent_bids = json.load(f)
        with open('results/no_intent_stats.json') as f: 
            no_intent_bids = json.load(f)
        rows = []
        df = pd.DataFrame.from_dict(intent_bids).transpose()
        df.index.name = "Bidders"
        df.fillna(0, inplace=True)
        bidders = df.index.values
        header = pd.MultiIndex.from_product([self.cols,
                                ['mean','std_dev','median']],
                            names=['Category','Bidders'])
        values = pd.DataFrame(index=bidders, 
                            columns=header)
        b = 0
        for bidder in bidders:
            values.fillna(0.0, inplace=True)

            c=0  

            for category in df:

            
                if df[category][bidder] != 0:
                    df_dict = dict(zip(df[category][bidder], df[category][bidder].values()))
                    mean = df_dict['mean']
                    std_dev = df_dict['std_dev']
                    median = df_dict['median']

                else: 
                    mean = 0.0
                    std_dev = 0.0
                    median = 0.0
                
                values.at[bidder, (category, 'mean')] = mean
                values.at[bidder, (category, 'median')] = median
                values.at[bidder, (category, 'std_dev')] = std_dev
       
        
        print(values)
        path = os.path.join(csv_dir, 'stats.csv')
        values.to_csv(path)
        


        # for bidder in self.all_bidders:
        #     line = ""
        #     if bidder in no_intent_bids: 
        #         line += "{}& ".format(bidder)
        #         for category in self.cols: 
        #             if category in no_intent_bids[bidder]:
        #                 line += "{}& {}& ".format(round(no_intent_bids[bidder][category]['mean'], 4),
        #                                           round(no_intent_bids[bidder][category]['std_dev'], 4))
        #             else:
        #                 line += "{}& {}& ".format(0.0, 0.0)
        #         line = line[:-2]+"\\hline"
        #         rows.append(line)
                
        #     if bidder in intent_bids: 
        #         line = ""
        #         line += "{}& ".format(bidder)
        #         for category in self.cols: 
        #             if category in intent_bids[bidder]:
        #                 line += "{}& {}& ".format(round(intent_bids[bidder][category]['mean'], 4),
        #                                         round(intent_bids[bidder][category]['std_dev'], 4))
        #             else:
        #                 line += "{}& {}& ".format(0.0, 0.0)
        #         line = line[:-2]+"\\hline"
        #         line = line.replace("&", "&\\cellcolor{light-gray}", 32)
        #         rows.append(line)
        # print('\nBids CPM Mean and Std. Dev. Summary Table\n--------------------------------------')                                              
        # for row in rows: 
        #     print(row)

    def table_bids_median(self): 
        intent_bids = []
        no_intent_bids = []
        with open('results/intent_stats.json') as f: 
            intent_bids = json.load(f)
        with open('results/no_intent_stats.json') as f: 
            no_intent_bids = json.load(f)
        rows = []
        for bidder in self.all_bidders:
            line = ""
            if bidder in no_intent_bids: 
                line += "{}& ".format(bidder)
                for category in self.cols: 
                    if category in no_intent_bids[bidder]:
                        line += "{}& ".format(round(no_intent_bids[bidder][category]['median'], 4))
                    else:
                        line += "{}& ".format(0.0)
                line = line[:-2]+"\\hline"
                rows.append(line)
                
            if bidder in intent_bids: 
                line = ""
                line += "{}& ".format(bidder)
                for category in self.cols: 
                    if category in intent_bids[bidder]:
                        line += "{}& ".format(round(intent_bids[bidder][category]['median'], 4))
                    else:
                        line += "{}& ".format(0.0)
                line = line[:-2]+"\\hline"
                line = line.replace("&", "&\\cellcolor{light-gray}", 16)
                rows.append(line)
        print('\nBids CPM Median Summary Table\n--------------------------------------')                                              
        for row in rows: 
            print(row) 

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
                if 'intent' == intent:
                    line = line.replace("&", "&\\cellcolor{light-gray}", 32)    
                rows.append(line)
        
        print('\nBids Full Count Table\n--------------------------------------')                                              
        for row in rows: 
            print(row)

    def bidders_site_table(self):
        line = ""
        rows = []
        bidders = []
        for bidder in self.all_bidders: 
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

        self.table_summary_count()
        # self.table_bid_value_stats()
        # self.table_full_count()
        # self.bidders_site_table()



     
                    
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--bids_dir')
    args = parser.parse_args()

    a = process_HB_bids(bids_dir=args.bids_dir)
    a.get_bid_stats()
    a.create_tables()