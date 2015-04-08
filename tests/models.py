from django.db import models


class DummyModel(models.Model):
    name = models.CharField(max_length=128, default="Not sprinkled :(")

    def __str__(self):
        return self.name