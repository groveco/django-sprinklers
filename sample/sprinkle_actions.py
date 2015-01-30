from sprinkles.base import Sprinkle
from sprinkles.registry import sprinkle_registry
from sample.models import DummyModel
from celery import task


@task
def run_sample_sprinkle():
    SampleSprinkle.create_sprinkles()


class SampleSprinkle(Sprinkle):

    klass = DummyModel
    qs = DummyModel.objects.all()

    def perform(self):
        self.obj.name = "Sprinkled!"
        self.obj.save()
        return True


sprinkle_registry.register(SampleSprinkle)