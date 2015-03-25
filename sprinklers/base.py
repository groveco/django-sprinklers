from celery import chord, current_app
from registry import sprinkler_registry as registry
from django.db.models.query import ValuesQuerySet, QuerySet
import logging
from time import time

logger = logging.getLogger('')


@current_app.task()
def _async_subtask(obj_pk, sprinkler_name, kwargs):
    sprinkler = registry[sprinkler_name](**kwargs)
    return sprinkler._run_subtask(obj_pk)


@current_app.task()
def _sprinkler_finished_wrap(results, sprinkler):
    logger.info("SPRINKLER: Finished %s with results (length %s): %s" % (sprinkler, len(results), results))
    sprinkler.finished(results)


class SubtaskValidationException(Exception):
    pass


class SprinklerBase(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.klass = self.get_queryset().model

    def start(self):
        qs = self.get_queryset()
        if isinstance(qs, ValuesQuerySet):
            c = chord(
                (_async_subtask.s(obj['id'], self.__class__.__name__, self.kwargs) for obj in qs),
                _sprinkler_finished_wrap.s(sprinkler=self)
            )
        elif isinstance(qs, QuerySet):
            c = chord(
                (_async_subtask.s(obj.pk, self.__class__.__name__, self.kwargs) for obj in qs),
                _sprinkler_finished_wrap.s(sprinkler=self)
            )
        else:
            logger.error("SPRINKLER %s: Invalid queryset. Expected QuerySet of ValuesQuerySet, but got %s." % (self, type(qs)))
            return

        start_time = time()
        c.apply_async()
        end_time = time()

        duration = (end_time - start_time) * 1000
        logger.info("SPRINKLER %s: Started with %s objects (%sms)" % (self, len(qs), duration))

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
        try:
            obj = self.klass.objects.get(pk=obj_pk)
            self._log(self.validate, obj)
            # if subtask() doesn't return a value, return the object id so something more helpful than None
            # gets aggregated into the results object (passed to 'finish').
            return self._log(self.subtask, obj) or obj.id
        except self.klass.DoesNotExist:
            logger.info("SPRINKLER %s: Object <%s - %s> does not exist." % (self, self.klass.__name__, obj_pk))
        except SubtaskValidationException as e:
            logger.info("SPRINKLER %s: Validation failed for object %s: %s"
                       % (self, obj, e))

    def _log(self, fn, obj):
        fn_name = fn.__name__.split('.')[-1]
        logger.info("SPRINKLER %s: %s is starting for object %s."
                    % (self, fn_name, obj))
        res = fn(obj)
        logger.info("SPRINKLER %s: %s has finished for object %s."
                    % (self, fn_name, obj))
        return res

    def __repr__(self):
        return "%s - %s" % (str(self.__class__.__name__), self.kwargs)