## TL;DR

Django-Sprinklers imposes structure on jobs that perform asynchronous processing of Django models.

Specify a queryset and a function to perform on each object in the queryset and Sprinklers will distribute your queryset into a group of asynchronous jobs, perform logging, and track the status of each job.

Using this wrapper you can avoid repeatedly writing fiddly bits of celery code and repetitive logging so you can focus on your business logic.

## For example

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
    logger.info("Starting refresh objects...")
    qs = Item.objects.all()
    for obj in qs:
        if obj.needs_update():
            # Remember to pass 'id' and not the object itself!
            # Remember to call .delay on the task!
            get_updated_item_from_slow_external_service.delay(obj.id)
    logger.info("I would love to say 'finished' here, but I spawned a bunch of async tasks and can't actually do that...")

@task
def get_updated_field_from_slow_external_service(id):
    try:
        item = Item.objects.get(pk=id)
    except Item.DoesNotExist:
        logger.info("grrr...")
    logger.info("Starting update of item %s..." % id)
    item.field = ExternalServiceWrapper().get(id)['field']
    item.save()
    logger.info("Successful update of item %s." % id)
```

This is fine, but as logic gets more complex and as you add more jobs that follow a similar pattern, you'll find that you handle logging slightly differently from job to job, that you want to run code after all the subtasks have completed, and in general things are looking a bit messy.

Use Sprinklers to impose structure and make these jobs testable.

```python

# tasks.py

from sprinklers.base import SprinklerBase, registry, SubtaskValidationException

class ItemUpdateSprinkler(SprinklerBase):

    def get_queryset():
        return Item.objects.all()

    def validate(obj)
        if not obj.needs_update():
            raise SubtaskValidationException()

    def subtask(obj):
        obj.field = ExternalServiceWrapper().get(obj.id)['field']
        obj.save()
        return obj.id  # gets aggregated into a results argument

    def finished(results):
        logger.info("Updated %s items." % len(results))
registry.register(ItemUpdateSprinkler)


# This is the entry point to the job. You can use it in your crontab configuration as normal:

# CELERYBEAT_SCHEDULE = {
#     'item.tasks.start_item_sprinkler': {
#         'task': 'item.tasks.start_item_sprinkler',
#         'schedule': crontab(hour=24, minute=0),
#     },

@task
def start_item_sprinkler():
    ItemUpdateSprinkler().start()


```

You can also pass **kwargs into the Sprinkler's start() function, which will be accessible downstream to all Sprinkler methods. See tasks.py and models.py in /tests for how this works.

## Testing

The Sprinkler tests are a bit trickier to run that just 'manage.py test' because every attempt has been made to mimic an async production celery environment.

In order to run the tests you will need:

0. Change CELERY_ALWAYS_EAGER = False in settings.py
1. A local postgres server set up in line with the DB config in test.settings
2. A running redis server with default localhost config (redis://localhost:6379/0)
3. A running celery daemon/worker.
4. Then run 'python manage.py test'

I find it easiest to run 2 & 3 via:

```
screen -d -S 'redis' -m redis-server
screen -d -S 'celery' -m python manage.py celeryd
```

This will run each in the background, and you can 'screen -r redis' or 'screen -r celery' to view them (Ctrl-a-d to detach).

If you are working on this project directly, **remember to restart celery after code changes** to django-sprinklers. Celery does not live reload!

## FAQ

- Q: Will this work on any iterable? Does it have to be a Django queryset?
- A: It has to be a queryset (or a valuesqueryset). Sprinklers relies on some introspection to determine which model class to use for individual object retrieval.