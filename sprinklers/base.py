from celery import chord, current_app
from registry import sprinkler_registry as registry
from django.db.models.query import QuerySet
import logging
from time import time

logger = logging.getLogger('')


@current_app.task()
def _async_subtask(obj_pk, sprinkler_name, kwargs):
    sprinkler = registry[sprinkler_name](**kwargs)
    return sprinkler._run_subtask(obj_pk)


@current_app.task()
def _sprinkler_finished_wrap(results, sprinkler):
    sprinkler.log("Finished with results (length %s): %s" % (len(results), results))
    sprinkler.finished(results)


class SubtaskValidationException(Exception):
    pass


class SprinklerBase(object):
    subtask_queue = current_app.conf.CELERY_DEFAULT_QUEUE

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.klass = self.get_queryset().model

    def start(self):
        qs = self.get_queryset()
        if qs and isinstance(qs[0], dict):  # A ValuesQuerySet (Django < 1.9) or a QuerySet of dict objects (Django 1.9+)
            ids = [obj['id'] for obj in qs]
        elif isinstance(qs, QuerySet):
            ids = [obj.pk for obj in qs]
        else:
            self.log("Invalid queryset. Expected QuerySet or ValuesQuerySet, but got %s." % type(qs))
            return

        c = chord(
            (
                _async_subtask.s(i, self.__class__.__name__, self.kwargs).set(queue=self.get_subtask_queue())
                for i in ids
            ),
            _sprinkler_finished_wrap.s(sprinkler=self).set(queue=self.get_subtask_queue())
        )

        start_time = time()
        c.apply_async()
        end_time = time()

        duration = (end_time - start_time) * 1000
        self.log("Started with %s objects in %sms." % (len(qs), duration))
        self.log("Started with objects: %s" % ids)

    def finished(self, results):
        pass

    def get_queryset(self):
        raise NotImplementedError

    def validate(self, obj):
        """Should raise SubtaskValidationException if validation fails."""
        pass

    def subtask(self, obj):
        """ Do work on obj and return whatever results are needed."""
        raise NotImplementedError

    def get_subtask_queue(self):
        return self.subtask_queue

    def on_error(self, obj, e):
        """ Called if an unexpected exception, e, occurs while running the subtask on obj.
            Results from this function will be aggregated into the results passed to the
            .finished() method. To emulate default Celery behavior, just reraise e here.
            Note that raising an exception in subtask execution will prevent the chord from
            ever firing its callback (though other subtasks will continue to execute)."""
        raise e

    def on_validation_exception(self, obj, e):
        """ Called if validate raises a SubtaskValidationException."""
        return None

    def _run_subtask(self, obj_pk):
        """Executes the sprinkle pipeline. Should not be overridden."""
        try:
            obj = self.klass.objects.get(pk=obj_pk)
            self._log_execution_step(self.validate, obj)
            # if subtask() doesn't return a value, return the object id so something more helpful than None
            # gets aggregated into the results object (passed to 'finish').
            return self._log_execution_step(self.subtask, obj) or obj.id
        except self.klass.DoesNotExist:
            self.log("Object <%s - %s> does not exist." % (self.klass.__name__, obj_pk))
        except SubtaskValidationException as e:
            self.log("Validation failed for object %s: %s" % (obj, e))
            return self.on_validation_exception(obj, e)
        except Exception as e:
            self.log("Unexpected exception for object %s: %s" % (obj, e))
            return self.on_error(obj, e)

    def _log_execution_step(self, fn, obj):
        fn_name = fn.__name__.split('.')[-1]
        self.log("%s is starting for object %s." % (fn_name, obj))
        res = fn(obj)
        self.log("%s has finished for object %s." % (fn_name, obj))
        return res

    def __repr__(self):
        return "%s - %s" % (str(self.__class__.__name__), self.kwargs)

    def log(self, msg):
        logger.info("SPRINKLER %s: %s" % (self, msg))