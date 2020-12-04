import os
import json
import genParams as gp
from getSitesToVisit import GetSitesToVisit as GS

class Lancher:
    def __init__(self):
        pass
    def doABTraining(self):
        pass
    def doABTesting(self):
        pass
    def doMLTraining(self):
        config = self.getMLTrainingConfig()
        # sites = GS().getTrainingSites()
        gp.genParams()
    def doMLTesting(self):
        pass
    
    def getABTrainingConfig(self):
        pass
    def getABTestingConfig(self):
        pass
    def getMLTrainingConfig(self):
        cfg = []
        with open('../config/training/crawl_config.json') as f: 
            cfg = json.load(f)
        return cfg['config']
    def getMLTestingConfig(self):
        pass
if __name__ == "__main__":

    a = Lancher()
    a.doMLTraining()