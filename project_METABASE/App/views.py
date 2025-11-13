from flask import *
from flask_cors import CORS
from flask_apscheduler import APScheduler
from datetime import datetime
import App.lockfile as lockfile
import time
import os
import App.utils as utils
import App.S3_manager as S3_manager

app = Flask(__name__)
CORS(app)
scheduler = APScheduler()


@app.route('/is_alive', methods=["GET"])
def api_is_alive():
    print("[INFO] alive")
    return "alive"


@app.route('/utils/list_S3_contents', methods=["GET"])
def api_list_S3_contents():
    lockfile_name = './LOCKFILE_list_S3_contents.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        result = S3_manager.list_S3_contents(prefix="metabase_db/")
        return result
    finally:
        lockfile.release_lock(fd, lockfile_name)

# Scheduler runs daily at 20:00 French local time (22:00 French local time when running inside Docker).
@scheduler.task('cron', id='save_metabase_db_to_S3',year='*', month='*', day='*', week='*', day_of_week='*', hour='20', minute='0', second='0')
@app.route('/utils/save_metabase_db_to_S3', methods=["GET"])
def api_save_metabase_db_to_S3():
    print("[INFO] save_metabase_db_to_S3")
    lockfile_name = './LOCKFILE_save_metabase_db_to_S3.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        result = utils.save_metabase_db_to_S3()
        return result
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/utils/restore_metabase_db_from_S3', methods=["POST"])
def api_restore_metabase_db_from_S3():
    lockfile_name = './LOCKFILE_restore_metabase_db_from_S3.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        zip_s3_path = request.form.get('zip_name')
        if zip_s3_path is None:
            print("You need to fill zip_name parameter")
            return "You need to fill zip_name parameter"
        if not zip_s3_path.startswith("metabase_db/"):
            zip_s3_path = f"metabase_db/{zip_s3_path}"
        result = utils.restore_metabase_db_from_S3(zip_s3_path)
        return result
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/stop_metabase', methods=["GET"])
def api_stop_metabase():
    utils.stop_metabase()
    return "done"

@app.route('/launch_metabase', methods=["GET"])
def api_launch_metabase():
    utils.start_metabase()
    return "done"
