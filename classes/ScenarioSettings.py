class ScenarioSettings:
    def __init__(self, dbs):
        self.DB_NAMES = dbs
        self.SAFE = False
        self.FAST_SAFE = True
        self.SOLVER = []
        self.SolverConfigs = {}
        self.HOST_UPPER_BOUND = 500
        self.HOST_START = 500
        self.HOST_STEPS = 500
        self.RESPONSE_UPPER_BOUND = 100
        self.RESPONSE_START = 100
        self.RESPONSE_STEPS = 100
        self.CONFLICT_UPPER_BOUND = 100
        self.CONFLICT_START = 100
        self.CONFLICT_STEPS = 100
        self.BOUNDS = True
	self.METRICS = []
