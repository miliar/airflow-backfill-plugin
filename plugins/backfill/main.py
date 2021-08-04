import subprocess
import json
import flask
from flask import request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView


class Backfill(AppBuilderBaseView):

    @expose('/')
    def list(self):
        output = subprocess.check_output(
            'airflow dags list -o json', shell=True)
        json_output = json.loads(output)
        dags = [item["dag_id"] for item in json_output if item["paused"] is not None]
        return self.render_template("backfill_page.html",
                           dags=dags)

    @expose('/backfill')
    def run_backfill(self):
        cmd = self._get_backfill_command(request.args.get("dag_name"),
                                         request.args.get("task_name"),
                                         request.args.get("start_date"),
                                         request.args.get("end_date"))

        subprocess.Popen(cmd, shell=True)
        response = json.dumps({'submitted': True})
        return flask.Response(response, mimetype='text/json')

    def _get_backfill_command(self, dag_name, task_name, start_date, end_date):
        if task_name:
            return 'yes | airflow dags backfill --reset-dagruns --rerun-failed-tasks -x -i -s "{}" -e "{}" -t "{}" {}'.format(
                start_date, end_date, task_name, dag_name)
        else:
            return 'yes | airflow dags backfill --reset-dagruns --rerun-failed-tasks -x -s "{}" -e "{}" {}'.format(
                start_date, end_date, dag_name)
