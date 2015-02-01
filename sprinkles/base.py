import logging
from celery import task, chord
from registry import sprinkler_registry as registry


logger = logging.getLogger(__name__)


@task
def _run_sprinkle(obj_pk, sprinkler_name, kwargs):
    sprinkler = registry[sprinkler_name](**kwargs)
    return sprinkler._run_subtask(obj_pk)


@task
def _sprinkler_finished_wrap(results, sprinkler):
    logger.info("SPRINKLER: Finished %s with results (length %s): %s" % (sprinkler, len(results), results))
    sprinkler.finished(results)


class ActionValidationException(Exception):
    pass


class SprinklerBase(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.klass = self.get_queryset().model
        c = chord(
            (_run_sprinkle.s(obj.pk, self.__class__.__name__, self.kwargs) for obj in self.get_queryset()),
            _sprinkler_finished_wrap.s(sprinkler=self)
        )
        self._job = c

    def start(self):
        logger.info("SPRINKLER: Started %s." % self)
        self._job.apply_async()

    def finished(self, results):
        pass

    def get_queryset(self):
        raise NotImplementedError

    def validate(self, obj):
        """Should raise ActionValidationException if validation fails."""
        pass

    def subtask(self, obj):
        """ Do work on obj and return whatever results are needed."""
        raise NotImplementedError

    def _run_subtask(self, obj_pk):
        """Executes the sprinkle pipeline. Should not be overridden."""
        obj = self.klass.objects.get(pk=obj_pk)
        try:
            self._log(self.validate, obj)
            return self._log(self.subtask, obj)
        except ActionValidationException as e:
            logger.log("SPRINKLE: %s validation exception for %s with id %s: %s"
                       % (self, self.klass.__name__, obj.pk, e))

    def _log(self, fn, obj):
        logger.info("SPRINKLE: %s.%s is starting for object <%s - %s>."
                    % (self, fn.__name__, self.klass.__name__, obj.pk))
        res = fn(obj)
        logger.info("SPRINKLE: %s.%s has finished for object <%s - %s>."
                    % (self, fn.__name__, self.klass.__name__, obj.pk))
        return res

    def __unicode__(self):
        return "%s - %s" % (str(self.__class__.__name__), self.kwargs)