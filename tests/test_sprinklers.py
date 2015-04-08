from django.test import TransactionTestCase
from tests.models import DummyModel
from tests.tasks import run_sample_sprinkler, SampleSprinkler
from django.conf import settings
import time


class SprinklerTest(TransactionTestCase):

    @classmethod
    def tearDown(self):
        if not settings.CELERY_ALWAYS_EAGER:
            time.sleep(2)

    def _run(self, **kwargs):
        run_sample_sprinkler.delay(**kwargs)
        if not settings.CELERY_ALWAYS_EAGER:
            time.sleep(2)

    def test_objects_get_sprinkled(self):
        DummyModel(name="foo").save()
        DummyModel(name="foo").save()
        self._run()
        for d in DummyModel.objects.all():
            self.assertEqual(d.name, "Sprinkled!")

    def test_works_with_values_queryset(self):
        DummyModel(name="foo").save()
        DummyModel(name="foo").save()
        self._run(values=True)
        for d in DummyModel.objects.all():
            self.assertEqual(d.name, "Sprinkled!")

    def test_queryset_refreshes_on_each_sprinkling(self):

        DummyModel(name="foo").save()
        self._run()

        # Make sure we don't incorrectly pass this test through sheer luck by generating the number
        # of models that happens to match the results cache of the SampleSprinkler queryset.
        # This was a bigger issue in an earlier version of sprinklers, but it still makes me feel good
        # knowing that this tests pass and sprinklers will always refresh their querset when they run.
        cur_len = len(SampleSprinkler().get_queryset())
        for i in xrange(cur_len + 5):
            DummyModel(name="foo").save()

        self._run()

        for d in DummyModel.objects.all():
            self.assertEqual(d.name, "Sprinkled!")

    def test_parameters_in_qs(self):

        DummyModel(name="qux").save()
        DummyModel(name="mux").save()

        self._run(name="qux")
        self.assertFalse(DummyModel.objects.filter(name="qux").exists())
        self.assertTrue(DummyModel.objects.filter(name="mux").exists())

    def test_sprinkler_finished(self):
        DummyModel(name="qux").save()
        DummyModel(name="mux").save()
        self._run(persist_results=True, special_return=True)
        self.assertEqual(DummyModel.objects.filter(name=str([True, True])).count(), 1)

    def test_validation_exception(self):
        DummyModel(name="foo").save()
        self._run(fail=True)
        self.assertTrue(DummyModel.objects.filter(name="foo").exists())

    def test_default_return_value_for_subtask(self):
        d1 = DummyModel(name="qux")
        d1.save()
        d2 = DummyModel(name="mux")
        d2.save()
        self._run(persist_results=True)
        self.assertEqual(DummyModel.objects.filter(name=str([d1.id, d2.id])).count(), 1)

    def test_error_on_subtask_calls_on_error(self):
        DummyModel(name="fail").save()
        DummyModel(name="succeed").save()
        self._run(raise_error=True, persist_results=True, special_return=True)
        self.assertEqual(DummyModel.objects.filter(name=str([False, True])).count(), 1)