from sprinkles.base import Sprinkle
from sprinkles.registry import sprinkle_registry
from sample.models import DummyModel
from celery import task


@task
def run_sample_sprinkle(**kwargs):
    SampleSprinkle.create_sprinkles(**kwargs)


class SampleSprinkle(Sprinkle):

    klass = DummyModel

    @classmethod
    def qs(cls, **kwargs):
        return DummyModel.objects.filter(**kwargs).all()

    def perform(self):
        self.obj.name = "Sprinkled!"
        self.obj.save()
        return True


sprinkle_registry.register(SampleSprinkle)