import os
import time
import json
import random
import logging
# from browsermobproxy import Server


class ScriptUtils: 
    def __init__(self): 
        self.allowVisit = 1

        self.available  = True
        self.GET_CPM =  "var output = [];" \
                        "function getCPM()" \
                        "{    " \
                        "    var responses = pbjs.getBidResponses();" \
                        "    var winners = pbjs.getAllWinningBids();" \
                        "    Object.keys(responses).forEach(function(adUnitCode) {" \
                        "    var response = responses[adUnitCode];" \
                        "        response.bids.forEach(function(bid) " \
                        "        {" \
                        "            output.push({" \
                        "            bid: bid," \
                        "            adunit: adUnitCode," \
                        "            adId: bid.adId," \
                        "            bidder: bid.bidder," \
                        "            time: bid.timeToRespond," \
                        "            cpm: bid.cpm," \
                        "            msg: bid.statusMessage," \
                        "            rendered: !!winners.find(function(winner) {" \
                        "                return winner.adId==bid.adId;" \
                        "            })" \
                        "            });" \
                        "        });" \
                        "    });" \
                        "}" \
                        "getCPM();" \
                        "return output;"

        self.PBJS_VERSION = "var version = '';" \
                            "function pbjsVersion() " \
                            "{" \
                            "    try" \
                            "    {" \
                            "        version = pbjs.version;" \
                            "        " \
                            "    }" \
                            "    catch(err)" \
                            "    {" \
                            "        " \
                            "    }" \
                            "}" \
                            "pbjsVersion();" \
                            "return version;"

    def getCpm(self, iab, visit, site, crawl_type, **kwargs):
        driver = kwargs['driver']
        data = []
        done = []
        writing = []
        timeout = 0

        with open('writing.json', 'w') as f: 
            json.dump({'writing':iab}, f)
        with open('writing.json') as f: 
            writing = json.load(f)
        
        while writing['writing'] != iab or timeout> 90:
            sleep =random.randint(1,4)
            print('[SCRIPT UTILS] {} sleeping for {} seconds'.format(iab, sleep))
            time.sleep(sleep)
            timeout+=sleep
            with open('writing.json') as f: 
                writing = json.load(f)
                if writing['writing'] == "":
                    writing['writing'] = iab
                    print('[SCRIPT UTILS] {}'.format(writing))
            


        try: 
            with open('testingDone.json') as f: 
                from subprocess import call
                call(['echo','printing file...'])
                call(['cat','testingDone.json'])
                done = json.load(f)
        except Exception as e: 
            print("[SCRIPT UTILS] {} Exception occurred trying to open file {}".format(time.asctime(), e))
            retry = 0
            
            while retry < 20: 
                try:
                    with open('testingDone.json') as f: 
                        done = json.load(f)  
                        break
                except: 
                    print("[SCRIPT UTILS] {} Exception occurred trying to open file {}, retrying: {}".format(time.asctime(), e, retry))
                    retry+=1
                    time.sleep(random.randint(1,3))
            if retry >= 20:
                print("[SCRIPT UTILS] {} Exception occurred trying to open file {}, quitting...: {}".format(time.asctime(), e, retry))
        domain = driver.current_url
        print(domain)
        domain = domain.split('//')[0]+'//'+domain.split('//')[1].split('/')[0]
        print('[SCRIPT UTILS] getCpm() domain: {} iab: {}'.format(domain, iab))
        output = []
        script = self.GET_CPM
        try: 
            output = driver.execute_script(script)
        except Exception as e: 
            output.append("error: {}".format(e))
        if output == []:
            msg = '[SCRIPT UTILS] No pbjs bids returned'
            print(msg)
            output.append(msg)   
            no_bids = []
            with open('no_bid_sites.json') as f: 
                no_bids = json.load(f)
                if site in no_bids: 
                    no_bids[site]['count'] +=1
                    no_bids[site]['crawl_info'].append({'site':site, 'visit':visit})
                else: 
                    no_bids[site] = {'crawl_info': [{'site':site, 'visit':visit}]}
                    no_bids[site] = {'count': 1}
            with open('no_bid_sites.json', 'w') as f: 
                json.dump(no_bids, f, separators=(',', ':'), indent=4)
        site = site.split('://')[1]
        # if iab != 'prebid_crawl':
        if crawl_type == "NO_INTENT":
            file = os.path.join('/home/johncook/headerBidding/TrackingProject/results/bids_no_intent/', site+"_{}.json".format(iab))
        else:
            file = os.path.join('/home/johncook/headerBidding/TrackingProject/results/bids_intent/', site+"_{}.json".format(iab))
        # else: 
        #     file =  os.path.join('/home/johncook/headerBidding/TrackingProject/results/pbjs_crawl_bids/', site+".json")
        try: 
            with open(file) as f:                  
                data = json.load(f) 
        except Exception as e: 
            with open(file) as f:    
                data = json.load(f) 
        # os.system('touch {}'.format(file))
        for i in output: 
            if iab in data: 
                if domain in data[iab]: 
                    if visit in data[iab][domain]:
                        data[iab][domain][visit].append(i)
                    else:
                        data[iab][domain].update({visit:[i]})
                else: 
                    data[iab].update({domain:{visit:[i]}})
            else: 
                data[iab] = {domain:{visit:[i]}}
        
        try: 
            with open(file, 'w') as f:
                print('[SCRIPT UTILS] {} writing to {}'.format(iab, file))
                json.dump(data, f, indent=True, separators=(',', ':'))
                os.system('cat {}'.format(file))
        except Exception as e: 
            print("[SCRIPT UTILS] Exception: {}. Attempting to write again".format(e))
            with open(file, 'w') as f: 
                print('[SCRIPT UTILS] {} writing to {}'.format(iab,  file))
                json.dump(data, f, indent=True, separators=(',', ':'))                         
        done[iab] = 1

        tmp = []
        with open('testingDone.json') as f: 
            tmp = json.load(f)
            print('[SCRIPT UTILS] {} tmp {}'.format(iab,tmp))
            for i in done:
                done[i] = done[i] or tmp[i]
            print('[SCRIPT UTILS] {} done {}'.format(iab,done))


        with open('testingDone.json', 'w') as f: 
            print('[SCRIPT UTILS] {} writing {} to {}'.format(iab,done, 'testingDone.json'))

            json.dump(done, f, indent=True, separators=(',', ':'))


        with open('writing.json', 'w') as f: 
            json.dump({'writing':""}, f)
            print('[SCRIPT UTILS] {} finished. '.format(iab))



    
    def pbjsVersion(self, **kwargs): 
        driver = kwargs['driver']
        version = driver.execute_script(self.PBJS_VERSION)
        data = []
        domain = driver.current_url
        
        with open('/home/johncook/headerBidding/TrackingProject/results/pbjs_sites_full.json') as f:      
            data = json.load(f)  
            data.update({domain: version})
                    
        with open('/home/johncook/headerBidding/TrackingProject/results/pbjs_sites_full.json', 'w') as f: 
            json.dump(data, f, indent=True, separators=(',', ':'))
       
        try:
            with open('pbjsDone.txt', 'w') as f: 
                f.write('1')
        except:
            pass

    def signalDone(self, iab, **kwargs):
        data = []
        try: 
            with open('trainingDone.json') as f:  
                data = json.load(f)
            data.update({iab:1})
        except: 
            time.sleep(random.randint(2,6))
            with open('trainingDone.json') as f:  
                data = json.load(f)
            data.update({iab:1})
            
        print(data)
        try: 
            with open('trainingDone.json', 'w') as f: 
                json.dump(data, f, indent=4, sort_keys=True, separators=(',', ':'))
        except: 
            time.sleep(random.randint(2,6))
            with open('trainingDone.json', 'w') as f: 
                json.dump(data, f, indent=4, sort_keys=True, separators=(',', ':'))


    # def write_out_har(self, url, timestamp, category, intent, ml_type, **kwargs):
    #     try:
    #         url = url.split('://')[1]       
            
    #         old_path = '/mnt/hgfs/archive_save/hars/HAR_{}.json'.format(url)
    #         new_path = ""
    #         intent_url_found= ""

    #         if ml_type == 'ml_training':
    #             new_path = '/mnt/hgfs/archive_save/ml_training/{}_{}/hars/'.format(timestamp, intent)
        
    #             if intent == "INTENT":
    #                 intent_check = ["www.hotels.com/", "www.zales.com/", "www.jamesedition.com/", "www.luxuryrealestate.com/"]
    #                 for intent_url in intent_check:
    #                     if intent_url in url: 
    #                         intent_url_found = intent_url.split('/')[0]
    #                 if intent_url_found != "":    
    #                     os.system('mv /mnt/hgfs/archive_save/hars/*_INTENT_URL.json {}'.format(new_path))                        
        
    #         if ml_type == 'ml_testing':
    #             new_path = '/mnt/hgfs/archive_save/ml_testing/ready_for_analysis/{}_{}/testing/hars/'.format(timestamp, intent)

    #             os.system('mv {} {}'.format(old_path, new_path))

    #         print("[SCRIPT UTILS] {} moving {} to {}".format(time.asctime(), old_path, new_path))

    #     except:
    #         print("[SCRIPT UTILS] {} moving {} to {} HAR file".format(time.asctime(), old_path, new_path))

        