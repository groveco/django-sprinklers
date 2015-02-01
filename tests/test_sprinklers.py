from django.test import TestCase
from sample.models import DummyModel
from sample.sprinklers import run_sample_sprinkler, SampleSprinkler
from mock import patch


class SprinklerTest(TestCase):

    def test_objects_get_sprinkled(self):
        DummyModel(name="foo").save()
        DummyModel(name="foo").save()
        run_sample_sprinkler()

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

        run_sample_sprinkler()

        for d in DummyModel.objects.all():
            self.assertEqual(d.name, "Sprinkled!")

    def test_parameters_in_qs(self):

        DummyModel(name="qux").save()
        DummyModel(name="mux").save()

        run_sample_sprinkler(name="qux")
        self.assertFalse(DummyModel.objects.filter(name="qux").exists())
        self.assertTrue(DummyModel.objects.filter(name="mux").exists())

    def test_sprinkler_finished(self):
        DummyModel(name="qux").save()
        DummyModel(name="mux").save()

        s = SampleSprinkler()
        s.start()

        self.assertItemsEqual(s.results, [True, True])

    @patch('sample.sprinklers.SampleSprinkler._log')
    def test_logging_succeeded(self, mocked_log):
        d = DummyModel(name="foo")
        d.save()
        run_sample_sprinkler()
        self.assertIn('subtask', str(mocked_log.call_args[0][0]))
        self.assertEqual(d, mocked_log.call_args[0][1])
