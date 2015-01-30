from django.test import TestCase
from sample.models import DummyModel
from sample.sprinkle_actions import run_sample_sprinkle, SampleSprinkle


class SprinkleTest(TestCase):

    def test_objects_get_sprinkled(self):
        DummyModel(name="foo").save()
        DummyModel(name="foo").save()
        run_sample_sprinkle()

        for d in DummyModel.objects.all():
            self.assertEqual(d.name, "Sprinkled!")

    def test_queryset_refreshes_on_each_sprinkling(self):

        DummyModel(name="foo").save()
        run_sample_sprinkle()

        # Make sure we don't incorrectly pass this test through sheer luck by generating the number
        # of models that happens to match the results cache of SampleSprinkle.qs. Trust me on this one...
        cur_len = len(SampleSprinkle.qs())
        for i in xrange(cur_len + 5):
            DummyModel(name="foo").save()

        run_sample_sprinkle()

        for d in DummyModel.objects.all():
            self.assertEqual(d.name, "Sprinkled!")

    def test_parameters_in_qs(self):

        DummyModel(name="qux").save()
        DummyModel(name="mux").save()

        run_sample_sprinkle(name="qux")
        self.assertFalse(DummyModel.objects.filter(name="qux").exists())
        self.assertTrue(DummyModel.objects.filter(name="mux").exists())