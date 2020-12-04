import sys
sys.path.append('../')
import inspect
from lcdk import lcdk as LeslieChow
from  TestParams import TestParams
from crawling.getSitesToVisit import GetSitesToVisit # The code to test
import unittest   # The test framework

TRACE_ON = True

class Test_GetSitesToVisit(unittest.TestCase):        

    def test_getTrainingSites(self):
        
        self.DBG = LeslieChow.lcdk()
        self.DBG.trace_function(self)
        self.class_name = "GetSitesToVisit"
        function_name = "getTrainingSites"
        test_ = TestParams()
        test_params = test_.get_configs(self.class_name, function_name)
        params = test_params['param_info']
        test_cases = []
        
        test_cases = test_.gen_test_cases(params)
        case = 0
        for test_case in test_cases:
            sitesToVisit = []
            sitesToVisit_path = test_case[0]
            volume = test_case[1]
            intent = test_case[2]
            category = test_case[2]
            crawl_type = test_case[4]
            msg =  "\nTest Case: {}\
                    \n\tsitesToVisit_path: {} \
                    \n\tvolume: {} \
                    \n\tintent: {} \
                    \n\tcategory: {} \
                    \n\tcrawl_type: {}".format(case, sitesToVisit_path, volume, intent, category, crawl_type)
            case+=1
            self.DBG.log(msg)

            sitesToVisit = GetSitesToVisit().getTrainingSites(*test_case)
            intent_size = volume+4
            no_intent_size = volume
            site_size = sitesToVisit.__len__()

            if crawl_type == "ML_TRAIN":
                
                if intent == "INTENT":
                    
                    self.assertEqual(site_size, intent_size)
                else: 
                    self.assertEqual(site_size, no_intent_size)



if __name__ == '__main__':
    unittest.main()