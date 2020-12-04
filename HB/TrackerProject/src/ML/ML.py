

class ML: 
    def __init__(self, **kwargs):
        kwargs = dict(kwargs['kwargs'].get_kwargs())
        self.ml_type = kwargs.get('ml_type',"")
        if self.ml_type == 'training': 
            self.training()
        elif self.ml_type == 'testing': 
            self.testing()
        else: 
            print("invalid ml_type: {}\n\t must be 'training' or 'testing'",format(self.ml_type))
            raise Exception



    def training(self):
        pass
    
    def testing(self): 
        pass