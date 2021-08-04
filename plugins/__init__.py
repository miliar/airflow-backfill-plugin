from flask import Blueprint
from airflow.plugins_manager import AirflowPlugin
from backfill.main import Backfill

#backfill_admin_view = Backfill(category="Admin", name="Backfill")


v_appbuilder_view = Backfill()
v_appbuilder_package = {"name": "Backfill",
                        "category": "Admin",
                        "view": v_appbuilder_view}

backfill_blueprint = Blueprint(
    "backfill_blueprint", __name__,
    template_folder='templates')


class AirflowBackfillPlugin(AirflowPlugin):
    name = "backfill_plugin"
    flask_blueprints = [backfill_blueprint]
    appbuilder_views = [v_appbuilder_package]
