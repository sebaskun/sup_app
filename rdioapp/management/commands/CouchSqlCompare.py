from django.core.management.base import BaseCommand, CommandError
from ...views import CouchSqlCompareServer

class Command(BaseCommand):
    help = 'Actualiza la base de datos Couch con información de SQL de Hapiqa'

    def handle(self, *args, **options):
        print(CouchSqlCompareServer([
            "clima",
            "cuadrilla",
            "rubro",
            "locacion",
            "sector",
            "operativo",
            "persona",
            "equipo",
            "proyecto",
            "partidas",
            "pxe",
            "pxm",
            "pxp"
        ]))
        # self.stdout.write(self.style.SUCCESS('Successfully closed poll "%s"' % poll_id))