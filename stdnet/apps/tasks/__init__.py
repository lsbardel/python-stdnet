from pulsar.apps import data, tasks

from .models import TaskData


class Store(data.Store):
    pass


class TaskBackend(tasks.TaskBackend):
    def get_task(self, task_id=None, timeout=1):
        task_manager = self.task_manager()
        #
        if not task_id:
            task_id = yield task_manager.queue.block_pop_front(timeout=timeout)
        if task_id:
            task_data = yield self._get_task(task_id)
            if task_data:
                yield task_data.as_task()


tasks.task_backends["stdnet"] = TaskBackend


data.register_store("redis", "stdnet.apps.tasks.Store")
