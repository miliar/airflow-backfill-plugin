import json
import subprocess

import flask
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag
from flask import request
from flask_admin import BaseView, expose


class Backfill(BaseView):
    @expose("/")
    def base(self):
        return self.render("backfill_page.html", dags=self._list_dags())

    @expose("/backfill")
    def run_backfill(self):
        cmd = self._get_backfill_command(
            request.args.get("dag_name"),
            request.args.get("task_name"),
            request.args.get("start_date"),
            request.args.get("end_date"),
        )

        subprocess.Popen(cmd, shell=True)
        response = json.dumps({"submitted": True})
        return flask.Response(response, mimetype="text/json")

    def _list_dags(self):
        dagbag = DagBag()
        dags = []

        for dag_id in dagbag.dags:
            orm_dag = DagModel.get_current(dag_id)
            # inactive DAGs can't be backfilled....
            is_active = (not orm_dag.is_paused) if orm_dag is not None else False

            if is_active:
                dags.append(dag_id)

        return dags

    def _get_backfill_command(self, dag_name, task_name, start_date, end_date):
        if task_name:
            return 'yes | airflow backfill --reset_dagruns --rerun_failed_tasks -x -i -s {} -e {} -t "{}" {}'.format(
                start_date, end_date, task_name, dag_name
            )
        else:
            return "yes | airflow backfill --reset_dagruns --rerun_failed_tasks -x -s {} -e {} {}".format(
                start_date, end_date, dag_name
            )
