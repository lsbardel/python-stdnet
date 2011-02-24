import os
from optparse import make_option

from djpcms import sites
from djpcms.apps.management.base import BaseCommand

from stdnet.orm import register_applications
from stdnet.contrib.monitor.utils import register_models


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('-a','--using',
                     action='store',
                     dest='using',
                     default='simple',
                     help='HTTP library to use when serving the application.'),
    )
    help = "Flush models in the data-server."
    args = '[appname appname.ModelName ...]'
    
    def handle(self, *args, **options):
        settings = sites.settings
        if args:
            models = register_models(apps, app_defaults = settings.DATASTORE)
        else:
            models = register_applications(settings.INSTALLED_APPS,
                                           app_defaults = settings.DATASTORE)
        for model in models:
            model.flush()