class DataSettings:
    def __init__(self):
        self.NEW = False
        self.newRandomEntity = 0
        self.newEffectedEntity = 0
        self.newUneffectedEntity = 0
        self.newResponsesSelfHealing = 0
        self.responseUneffected = [0]
        self.responseRandom = 0
        self.helpingFactor = 20
        self.conflicts = 0
        self.metrics = {'cost': (500.00,1)}
	
