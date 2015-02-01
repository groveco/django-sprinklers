class SprinklerRegistry(object):

    def __init__(self):
        self._registry = {}

    def register(self, sprinkler):
        self._registry[sprinkler.__class__.__name__] = sprinkler

    def __getitem__(self, key):
        return self._registry[key]

sprinkler_registry = SprinklerRegistry()