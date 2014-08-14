from scheduler.SuperTask import SuperTask
from scheduler.Task import Task

__author__ = 'mirrorcoder'


class SuperTaskExportInstance(SuperTask):

    def __init__(self, instance=None, namespace=None):
        self.instance = instance
        if not namespace:
            super(SuperTaskExportInstance, self).__init__()
        else:
            super(SuperTaskExportInstance, self).__init__(namespace=namespace)

    def run(self, inst_exporter=None, **kwargs):
        tasks_export = []
        exporter_builder = inst_exporter.export(self.instance)
        tasks = exporter_builder.get_tasks()
        tasks_export.append(TaskSetExportBuilder(exporter_builder))
        tasks_export.extend(tasks)
        tasks_export.append(TaskGetState())
        return tasks_export


class TaskSetExportBuilder(Task):

    def __init__(self, exporter_builder, namespace=None):
        self.exporter_builder = exporter_builder
        if not namespace:
            super(TaskSetExportBuilder, self).__init__()
        else:
            super(TaskSetExportBuilder, self).__init__(namespace=namespace)

    def run(self, **kwargs):
        return {
            'exporter_builder': self.exporter_builder
        }


class TaskGetState(Task):

    def run(self, exporter_builder=None, **kwargs):
        return {
            'data_export_inst': exporter_builder.get_state()['data']
        }
