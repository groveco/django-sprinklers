TL;DR

Django-Sprinklers is a high-level way to specify a queryset, and an async function which should be applied to each model in the queryset.

By using it, you can avoid a bunch of fiddly bits of celery code, and common async mistakes (like forgetting to actually call .delay() on an async function, passing full models into your messaging queue and running out of memory, etc.)

It also separates the task-handling and async logic from your business logic, allowing simple, idiomatic testing.

For instance:

At a high level celery cron jobs often take the form of "run this queryset, and take an action on each object." For example:

- Find all customers that should be billed for our recurring service, and bill them.
- Find all users who should receive a welcome email, and send them each a message.
- For each product listing, update the details from an external service.
- etc. etc.

Typically code for these cron tasks would look something like:

```python
from celery import task

@task
def refresh_objects():
    logger.log("Starting refresh objects...")
    qs = Item.objects.all()
    for obj in qs:
        get_updated_item_from_slow_external_service.delay(obj.id)

@task
def get_updated_field_from_slow_external_service(id):
    item = Item.objects.get(pk=id)
    item.field = ExternalServiceWrapper().get(id)['field']
    item.save()
```

This is fine, but as logic gets more complex and as you add more jobs that follow a similar pattern, you'll find that you handle logging slightly differently from job to job, that you want to run code after all the subtasks have completed, and that

```python

from sprinklers.base import SprinklerBase, registry

class ItemUpdateSprinkler(SprinklerBase):

    def get_queryset():
        return Item.objects.all()

    def subtask(obj):
        item.field = ExternalServiceWrapper().get(id)['field']
        item.save()
registry.register(ItemUpdateSprinkler)

@task
def start_item_sprinkler(): ItemUpdateSprinkler().start()

```

You can now register a crontab in settings.py like:

```python

CELERYBEAT_SCHEDULE = {
    'item.tasks.start_item_sprinkler': {
        'task': 'item.tasks.start_item_sprinkler',
        'schedule': crontab(hour=24, minute=0),
    },
```

FAQ

- Q: Will this work on any iterable? Does it have to be a Django queryset?
- A: It has to be a queryset. Sprinklers relies on some introspection to