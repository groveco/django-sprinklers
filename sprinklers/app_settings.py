from django.conf import settings


SPRINKLER_DEFAULT_SHARD_SIZE = getattr(settings, 'SPRINKLER_DEFAULT_SHARD_SIZE', 20000)
