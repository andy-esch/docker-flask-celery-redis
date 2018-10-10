from flask import Flask, url_for, request, jsonify
from worker import celery
import celery.states as states
import logging

app = Flask(__name__)


@app.route('/add/<int:param1>/<int:param2>')
def add(param1, param2):
    task = celery.send_task('tasks.add', args=[param1, param2], kwargs={})
    response = "<a href='{url}'>check status of {id} </a>".format(id=task.id,
                                                                  url=url_for('check_task', task_id=task.id, external=True))
    return response


@app.route('/hive')
def hive():
    username = request.args.get('user')
    key = request.args.get('key')
    database = request.args.get('database')
    table = request.args.get('table')

    logging.info(
            "New data transfer request received with details: {username}, "
            "{key}, {database}, {table}".format(
                username=username, key=key, database=database, table=table
            )
        )

    task = celery.send_task(
            'tasks.hive2carto',
            args=[database, table, username, key],
            kwargs={}
        )
    return jsonify({
        'status': 'pending',
        'task_id': task.id,
        'status_url': url=url_for('check_task', task_id=task.id, external=True)
    })

@app.route('/check/<string:task_id>')
def check_task(task_id):
    res = celery.AsyncResult(task_id)
    if res.state == states.PENDING:
        return jsonify({'state': res.state})
    else:
        return jsonify({'state': res.state, 'table_name': res.result})
