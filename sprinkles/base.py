import logging
from celery import task
from django.db import models
from registry import sprinkle_registry


logger = logging.getLogger(__name__)


@task
def run_sprinkle(obj_pk, action_name):
    a = sprinkle_registry[action_name](obj_pk)
    a.run()


class ActionValidationException(Exception):
    pass


class Sprinkle(object):

    def __init__(self, obj_id, *args, **kwargs):
        self.obj = self.klass.objects.get(pk=obj_id)

    def validate(self):
        """Should raise ActionValidationException if validation fails."""
        pass

    def failed_validation(self):
        pass

    def perform(self):
        """Must return True for success or False for failure. This method should not throw exceptions."""
        raise NotImplementedError

    def failed(self):
        pass

    def succeeded(self):
        pass

    def finished(self):
        pass

    def run(self):
        """Executes the sprinkle pipeline. Should not be overridden."""
        try:
            self._log(self.validate)
            res = self._log(self.perform)
            self._log(self.finished)
            if res:
                self._log(self.succeeded)
            else:
                self._log(self.failed)
        except ActionValidationException as e:
            logger.log("SPRINKLE: %s validation exception for %s with id %s: %s"
                       % (self, self.klass.__name__, self.obj.pk, e))
            self._log(self.failed_validation)

    def _log(self, fn):
        logger.info("SPRINKLE: %s.%s is starting for object <%s - %s>."
                    % (self, fn.__name__, self.klass.__name__, self.obj.pk))
        res = fn()
        logger.info("SPRINKLE: %s.%s has finished for object <%s - %s>."
                    % (self, fn.__name__, self.klass.__name__, self.obj.pk))
        return res

    def __unicode__(self):
        str(self.__class__.__name__)

    #####
    # Below is metadata and metamethods to handle the Sprinkle job. Everything above relates to a single sprinkle.
    #####

    klass = models.Model

    @classmethod
    def qs(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def create_sprinkles(cls, *args, **kwargs):
        for obj in cls.qs(*args, **kwargs):
            run_sprinkle.delay(obj.pk, cls.__name__)

