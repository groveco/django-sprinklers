class AlreadyRegistered(Exception):
    pass


class SprinkleRegistry(object):

    def __init__(self):
        self._registry = {}

    def register(self, sprinkle_action):
        if sprinkle_action.__name__ in self._registry:
            raise AlreadyRegistered('The sprinkle action %s is already registered' % sprinkle_action.__name__)
        self._registry[sprinkle_action.__name__] = sprinkle_action

    def __getitem__(self, key):
        return self._registry[key]

sprinkle_registry = SprinkleRegistry()