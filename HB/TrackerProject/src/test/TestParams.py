import os 
import json 
from collections import OrderedDict 
import itertools
from lcdk import lcdk as LeslieChow
class TestParams: 
    def __init__(self):
        self.DBG = LeslieChow.lcdk()
        self.test_params = OrderedDict()
        with open('test_config.json') as f: 
            self.test_params = json.load(f, object_pairs_hook=OrderedDict)['tests']

        
    def get_configs(self, class_name, function_name):
        """ getParams: A function to read in test configuration's from a class
            Args: 
                class_name: The class name to test
                function_name: The function name within a class

            Returns: 
                A test configuration 

        """

        try: 
            return self.test_params[class_name][function_name] 
        except Exception as e: 
            self.DBG.error("Exception: {}".format(e))


    def gen_test_cases(self, params):

        values = []

        for p in params: 
            param = params[p]
            values.append(param)
        test_cases = list(itertools.product(*values))
        # for t in test_cases: 
        #     print(t)
        return test_cases
        
        
        
    
       
        
