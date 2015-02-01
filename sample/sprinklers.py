from sprinkles.base import SprinklerBase
from sample.models import DummyModel
from celery import task


@task
def run_sample_sprinkle(**kwargs):
    SampleSprinkler(**kwargs).start()


class SampleSprinkler(SprinklerBase):

    def get_queryset(self):
        """
        This IF normally wouldn't be necessary because you either would or wouldn't be passing kwargs
        into the sprinkler. But I have tests that sometimes do and sometimes don't use kwargs, hence the
        check.
        """
        if self.kwargs.get('name', None):
            return DummyModel.objects.filter(name=self.kwargs['name']).all()
        return DummyModel.objects.all()

    def subtask(self, obj):
        obj.name = "Sprinkled!"
        obj.save()
        return True

    def finished(self, results):
        self.results = results