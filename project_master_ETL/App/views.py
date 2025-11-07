from flask import *
from flask_cors import CORS
from flask_apscheduler import APScheduler
from datetime import datetime
import logging
import App.lockfile as lockfile
import App.gas_stations_oils_prices as gas_stations_oils_prices
import App.official_oils_prices as official_oils_prices
import App.denormalize_station_prices as denorm_station_prices
import App.denorm_station_vs_official_prices as denorm_station_vs_official_prices
import App.utils as utils
import App.S3_manager as S3_manager
import App.mongo_manager as mongo_manager

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
CORS(app)
scheduler = APScheduler()


@app.route('/is_alive', methods=["GET"])
def api_is_alive():
    print("[INFO] alive")
    return "alive"


# Scheduler runs daily at 10:00 French local time (12:00 French local time when running inside Docker).
@scheduler.task('cron', id='complete_pipeline_oil_prices',year='*', month='*', day='*', week='*', day_of_week='*', hour='10', minute='0', second='0')
@app.route('/etl/launch_complete_pipeline_oil_prices', methods=["POST"])
def api_launch_complete_pipeline_oil_prices():
    lockfile_name = './LOCKFILE_launch_complete_pipeline_oil_prices.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        try:
            # Into API
            year_to_load = request.form.get('year_to_load')
            drop_mongo_collections = request.form.get('drop_mongo_collections')
        except:
            # Into Scheduler
            year_to_load = None
            drop_mongo_collections = None

        gas_stations_oils_prices.launch_etl_gas_stations_oils_prices(year_to_load, drop_mongo_collections)
        official_oils_prices.launch_etl_official_oils_prices(year_to_load, drop_mongo_collections)
        denorm_station_prices.denormalize_station_prices_for_dataviz(year_to_load, drop_mongo_collections)
        denorm_station_vs_official_prices.merge_denorm_station_vs_official_prices(year_to_load, drop_mongo_collections)
        return 'done'
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/etl/launch_etl_gas_stations_oil_prices', methods=["POST"])
def api_launch_etl_gas_stations_oil_prices():
    lockfile_name = './LOCKFILE_launch_etl_gas_stations_oil_prices.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        year_to_load = request.form.get('year_to_load')
        drop_mongo_collections = request.form.get('drop_mongo_collections')

        gas_stations_oils_prices.launch_etl_gas_stations_oils_prices(year_to_load, drop_mongo_collections)
        return "done"
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/etl/launch_etl_official_oils_prices', methods=["POST"])
def api_launch_etl_official_oils_prices():
    lockfile_name = './LOCKFILE_launch_etl_official_oils_prices.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        year_to_load = request.form.get('year_to_load')
        drop_mongo_collections = request.form.get('drop_mongo_collections')

        official_oils_prices.launch_etl_official_oils_prices(year_to_load, drop_mongo_collections)
        return "done"
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/dataviz/denormalize_station_prices_for_dataviz', methods=["POST"])
def api_denormalize_station_prices_for_dataviz():
    lockfile_name = './LOCKFILE_denormalize_station_prices_for_dataviz.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        year_to_load = request.form.get('year_to_load')
        drop_mongo_collections = request.form.get('drop_mongo_collections')
        denorm_station_prices.denormalize_station_prices_for_dataviz(year_to_load, drop_mongo_collections)
        return "done"
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/dataviz/merge_denorm_station_vs_official_prices', methods=["POST"])
def api_merge_denorm_station_vs_official_prices():
    lockfile_name = './LOCKFILE_merge_denorm_station_vs_official_prices.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        year_to_load = request.form.get('year_to_load')
        drop_mongo_collections = request.form.get('drop_mongo_collections')
        denorm_station_vs_official_prices.merge_denorm_station_vs_official_prices(year_to_load, drop_mongo_collections)
        return "done"
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/mongo/list_all_mongo_collections', methods=["GET"])
def api_list_all_mongo_collections():
    lockfile_name = './LOCKFILE_api_list_all_mongo_collections.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        result = mongo_manager.list_all_collections()
        return result
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/mongo/drop_one_collection', methods=["POST"])
def api_drop_one_collection():
    lockfile_name = './LOCKFILE_api_drop_one_collection.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        db_name = request.form.get('db_name')
        collection_name = request.form.get('collection_name')
        result = utils.drop_one_collection_to_mongo(db_name, collection_name)
        return result
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/mongo/drop_one_bdd', methods=["POST"])
def api_drop_one_bdd():
    lockfile_name = './LOCKFILE_api_drop_one_bdd.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        db_name = request.form.get('db_name')
        result = utils.drop_one_bdd_to_mongo(db_name)
        return result
    finally:
        lockfile.release_lock(fd, lockfile_name)


# Scheduler runs daily at 13:00 French local time (15:00 French local time when running inside Docker).
@scheduler.task('cron', id='save_mongo_dump_to_S3',year='*', month='*', day='*', week='*', day_of_week='*', hour='13', minute='0', second='0')
@app.route('/utils/save_mongo_dump_to_S3', methods=["POST"])
def api_save_mongo_dump_to_S3():
    lockfile_name = './LOCKFILE_save_mongo_dump_to_S3.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        try:
            # Into API
            db_name = request.form.get('db_name')
        except:
            # Into Scheduler
            db_name = None
        utils.save_mongo_dump_to_S3(db_name)
        return "done"
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/utils/list_S3_contents', methods=["GET"])
def api_list_S3_contents():
    lockfile_name = './LOCKFILE_list_S3_contents.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        result = S3_manager.list_S3_contents()
        return result
    finally:
        lockfile.release_lock(fd, lockfile_name)


@app.route('/utils/restore_mongo_dump_from_S3', methods=["POST"])
def api_restore_mongo_dump_from_S3():
    lockfile_name = './LOCKFILE_restore_mongo_dump_from_S3.lock'
    fd = lockfile.acquire_lock(lockfile_name)
    if fd is None:
        print(f"Job is already running. Skipping execution at {datetime.now()}")
        return {'message': 'Job already running'}, 200
    try:
        zip_name = request.form.get('zip_name')
        new_bdd_name = request.form.get('new_bdd_name')
        if zip_name is None:
            print("You need to fill zip_name parameter")
            return "You need to fill zip_name parameter"
        if new_bdd_name is None:
            print("You need to fill new_bdd_name parameter")
            return "You need to fill new_bdd_name parameter"
        result = utils.restore_mongo_dump_from_S3(zip_name, new_bdd_name)
        return result
    finally:
        lockfile.release_lock(fd, lockfile_name)
