import os
import json
import random
import itertools

from lcdk import lcdk as LeslieChow
class genParams: 
    def __init__(self):
        self.DBG = LeslieChow.lcdk(print_output=True)
        self.tracker_activations = []
        for j in list(itertools.product(list(range(10)), repeat=10)):
            print(j)



    def product_of_params(self, params,  generate=False, generate_output_size=10):
        process = []
        values = []
        
        if generate: 
            param_size = len(params)
            for j in list(itertools.product(list(range(param_size)), repeat=generate_output_size)):
                self.DBG.log(j)
                values.append(j)
        else:    
            for p in params: 
                param = params[p]
                process.append(param)
            values = list(itertools.product(*process))

        return values
        
        

if __name__ == "__main__":
    gp = genParams()
    training_config = []
    vic_profiles = []
    tracker_activations = []
    print("{}".format())
    with open("../config/training/crawl_config.json") as f: 
        training_config = json.load(f)
    
    params = [0,1]
    tracker_activations = gp.product_of_params(params, generate=True, generate_output_size=15)

    params = training_config["params"]
    vic_profiles = gp.product_of_params(params)
    profiles = []
    for t in tracker_activations: 
        vic_index = random.randint(0, vic_profiles.__len__()-1)
        crawl_profle = list(vic_profiles[vic_index])
        crawl_profle.append(list(t))
        profiles.append(crawl_profle)
    obj = {"generated_profiles": profiles}

    with open("generated_profiles.json","w") as f: 
        json.dump(obj, f, indent=4, separators=(",", ":"))


