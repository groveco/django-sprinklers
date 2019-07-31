import app_settings
from celery import chord, current_app
from .registry import sprinkler_registry as registry
from django.db.models.query import QuerySet
import logging
import uuid
from time import time

logger = logging.getLogger('')


@current_app.task()
def _async_subtask(obj_pk, sprinkler_name, kwargs):
    sprinkler = registry[sprinkler_name](**kwargs)
    return sprinkler._run_subtask(obj_pk)


@current_app.task()
def _async_shard_start(shard_id, from_pk, to_pk, sprinkler_name, kwargs):
    sprinkler = registry[sprinkler_name](**kwargs)
    return sprinkler.shard_start(shard_id, from_pk, to_pk)


@current_app.task()
def _sprinkler_shard_finished_wrap(results, shard_id, sprinkler_name, kwargs):
    sprinkler = registry[sprinkler_name](**kwargs)
    sprinkler.log(f"shard finished: {shard_id}")
    sprinkler.shard_finished(shard_id, results)

@current_app.task()
def _sprinkler_finished_wrap(results, sprinkler_name, kwargs):
    sprinkler = registry[sprinkler_name](**kwargs)
    sprinkler.log("Finished with results (length %s): %s" % (len(results), results))
    sprinkler.finished(results)


class SubtaskValidationException(Exception):
    pass


class SprinklerBase(object):
    subtask_queue = current_app.conf.CELERY_DEFAULT_QUEUE
    klass = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        if self.klass is None:
            self.klass = self.get_queryset().model

    def start(self):
        qs = self.get_queryset()
        ids = [o['id'] if isinstance(o, dict) else o.id for o in qs]

        c = chord(
            (
                _async_subtask.s(i, self.__class__.__name__, self.kwargs).set(queue=self.get_subtask_queue())
                for i in ids
            ),
            _sprinkler_finished_wrap.s(sprinkler_name=self.__class__.__name__, kwargs=self.kwargs).set(queue=self.get_subtask_queue())
        )

        start_time = time()
        c.apply_async()
        end_time = time()

        duration = (end_time - start_time) * 1000
        self.log("Started with %s objects in %sms." % (len(ids), duration))
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


class ShardedSprinkler(SprinklerBase):
    shard_size = app_settings.SPRINKLER_DEFAULT_SHARD_SIZE

    def start(self):
        shards = list(self.build_shards())

        # the sharded sprinkler calls finished on output of shard_start for each shard, passing the shard ID,
        # rather than the results of the completed shard tasks

        c = chord(
            (
                _async_shard_start.s(shard_id, from_pk, to_pk, self.__class__.__name__, self.kwargs).set(queue=self.get_subtask_queue())
                for shard_id, from_pk, to_pk in shards
            ),
            _sprinkler_finished_wrap.s(sprinkler_name=self.__class__.__name__, kwargs=self.kwargs).set(queue=self.get_subtask_queue())
        )

        start_time = time()
        c.apply_async()
        end_time = time()

        duration = (end_time - start_time) * 1000
        shard_ids = [shard[0] for shard in shards]
        self.log(f"Started with {len(shards)} shards in {duration}ms.")
        self.log(f"Started with shards: {[str(shard_id) for shard_id in shard_ids]}")
        return shard_ids

    def shard_start(self, shard_id, from_pk=None, to_pk=None):
        pks = self.get_queryset_pks(from_pk, to_pk)

        c = chord(
            (
                _async_subtask.s(pk, self.__class__.__name__, self.kwargs).set(queue=self.get_subtask_queue())
                for pk in pks
            ),
            _sprinkler_shard_finished_wrap.s(sprinkler_name=self.__class__.__name__, shard_id=shard_id, kwargs=self.kwargs).set(queue=self.get_subtask_queue())
        )

        start_time = time()
        c.apply_async()
        end_time = time()

        duration = (end_time - start_time) * 1000
        self.log(f"Started shard {shard_id} in {duration}ms.")

        return shard_id

    def shard_finished(self, shard_id, results):
        pass

    def get_queryset_pks(self, from_pk=None, to_pk=None):
        queryset = self.get_queryset().only('pk').order_by('pk')

        if from_pk is not None:
            queryset = queryset.filter(pk__gt=from_pk)

        if to_pk is not None:
            queryset = queryset.filter(pk__lte=to_pk)

        # values_list in django 1.11 is broken and will run out of memory when iterating over a large queryset, even with .iterator()
        # the following code does basically the same thing as values_list, without running out of memory
        db = queryset.db
        compiler = queryset.query.get_compiler(db)
        results = compiler.execute_sql(chunked_fetch=True)

        for row in compiler.results_iter(results):
            yield row[0]

    def build_shards(self):
        last_pk = None
        next_pk = None

        for i, pk in enumerate(self.get_queryset_pks(), 1):
            if i % self.shard_size == 0:
                last_pk = next_pk
                next_pk = pk
                yield uuid.uuid4(), last_pk, next_pk

        yield uuid.uuid4(), next_pk, None
