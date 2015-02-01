from sprinklers.base import SprinklerBase, registry, SubtaskValidationException
from sample.models import DummyModel
from celery import task


@task
def run_sample_sprinkler(**kwargs):
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

    def validate(self, obj):
        if self.kwargs.get('fail'):
            raise SubtaskValidationException

    def finished(self, results):
        self.results = results

registry.register(SampleSprinkler)