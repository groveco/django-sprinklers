from django.test import TransactionTestCase
from sample.models import DummyModel
from sample.tasks import run_sample_sprinkler, SampleSprinkler
import time


class SprinklerTest(TransactionTestCase):

    @classmethod
    def tearDown(self):
        time.sleep(1)

    def _run(self, **kwargs):
        run_sample_sprinkler.delay(**kwargs)
        time.sleep(1)

    def test_objects_get_sprinkled(self):
        DummyModel(name="foo").save()
        DummyModel(name="foo").save()
        self._run()
        for d in DummyModel.objects.all():
            self.assertEqual(d.name, "Sprinkled!")

    def test_queryset_refreshes_on_each_sprinkling(self):

        DummyModel(name="foo").save()
        run_sample_sprinkler()

        # Make sure we don't incorrectly pass this test through sheer luck by generating the number
        # of models that happens to match the results cache of SampleSprinkle.qs. Trust me on this one...
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

        s = SampleSprinkler(persist_results=True)
        s.start()
        time.sleep(2)
        self.assertEqual(DummyModel.objects.filter(name=str([True, True])).count(), 1)

    def test_validation_exception(self):
        DummyModel(name="foo").save()
        self._run(fail=True)
        self.assertTrue(DummyModel.objects.filter(name="foo").exists())