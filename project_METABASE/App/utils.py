import shutil
from pathlib import Path
from datetime import datetime
import subprocess
import os
import App.S3_manager as S3_manager


def save_metabase_db_to_S3():
    # clean working folder and recreate it
    if os.path.exists("outputs/metabase_db_to_S3"):
        shutil.rmtree("outputs/metabase_db_to_S3")
    os.makedirs("outputs/metabase_db_to_S3", exist_ok=True)

    str_current_date = datetime.now().strftime("%Y_%m_%d")
    folder_path_to_zip = "outputs/metabase_db_to_S3/metabase.db_" + str_current_date

    stop_metabase()
    copy_metabase_db_to_local(out_path=folder_path_to_zip)
    start_metabase()
    folder_path_zipped = compress_local_metabase_db_to_zip(folder_path_to_zip)
    folder_zipped = os.path.basename(folder_path_zipped)
    S3_manager.upload_file_to_s3(file_path=folder_path_zipped, object_name=f"metabase_db/{folder_zipped}")
    return "done"


def restore_metabase_db_from_S3(folder_path_S3_zipped):
    # clean working folder and recreate it
    if os.path.exists("outputs/restore_metabase_db"):
        shutil.rmtree("outputs/restore_metabase_db")
    os.makedirs("outputs/restore_metabase_db", exist_ok=True)

    if os.path.basename(folder_path_S3_zipped) == "metabase_db_example":
        folder_example_source = "metabase_db_example/metabase.db"
        folder_destination = os.path.join("outputs/restore_metabase_db", "metabase.db")
        shutil.copytree(folder_example_source, folder_destination, dirs_exist_ok=True)
        official_db_name = "metabase.db"
    else:
        # verify if mongo_dump name is existing in S3
        result, logs = S3_manager.check_existence_into_S3(folder_path_S3_zipped)
        if result != True:
            return f'fail, {logs}'
        # download zip to S3
        S3_manager.download_file_from_s3_to_path(folder_path_S3_zipped, out_path="outputs/restore_metabase_db")
        local_folder_name_zipped = os.path.basename(folder_path_S3_zipped)
        # dezip metabase_db
        local_folder_path_dezipped, official_db_name = decompress_zip_to_outputs(local_folder_name_zipped, outputs_folder="outputs/restore_metabase_db")
        os.rename(local_folder_path_dezipped, "outputs/restore_metabase_db/"+official_db_name)
    # restore metabase_db
    copy_local_metabase_db_to_docker("outputs/restore_metabase_db/"+official_db_name)
    stop_metabase()
    start_metabase()
    return "done"


def decompress_zip_to_outputs(zip_name, outputs_folder="outputs"):
    zip_path = os.path.join(outputs_folder, zip_name)
    if not os.path.exists(zip_path):
        print(f"[ERROR]: Zip file for '{zip_name}' not found at {zip_path}")
    folder_path = os.path.join(outputs_folder, zip_name.split(".zip")[0])
    shutil.unpack_archive(zip_path, folder_path)
    # Remove the zip dump file
    os.remove(zip_path)
    old_db_name = "_".join(zip_name.split("_")[:-3])
    return folder_path, old_db_name


def stop_metabase():
    print("[INFO] Stop Metabase docker container")
    subprocess.run(["docker", "stop", "metabase"], check=True)
    return "done"


def start_metabase():
    print("[INFO] Start Metabase docker container")
    subprocess.run(["docker", "start", "metabase"], check=True)
    return "done"


def delete_metabase_db_in_container(container_name="metabase", db_path="/metabase.db"):
    print(f"[INFO] Delete directory {db_path} inside container {container_name}...")
    try:
        subprocess.run(["docker", "exec", container_name, "rm", "-rf", db_path], check=True)
        print("[INFO] Deletion successful inside the container.")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to delete inside the container: {e}")
    return "done"


def copy_metabase_db_to_local(out_path):
    destination_path = Path(out_path)
    print(f"[INFO] Copying '/metabase.db' from container metabase to {destination_path} on host...")
    try:
        # Copy folder from container to host
        subprocess.run(["docker", "cp", "metabase:/metabase.db", str(destination_path)], check=True)
        print("[INFO] Copy completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to copy directory from container: {e}")
    return "done"


def copy_local_metabase_db_to_docker(metabase_db_folder_path):
    print(f"[INFO] Try remove '/metabase.db' from container metabase")
    subprocess.run(["docker", "exec", "metabase", "rm", "-rf", "/metabase.db"], check=True)
    print(f"[INFO] Success remove '/metabase.db' from container metabase")
    try:
        # Copy folder from container to host
        print(f"[INFO] Try copie local {metabase_db_folder_path} to container folder '/metabase.db'")
        subprocess.run(["docker", "cp", str(metabase_db_folder_path), "metabase:/"], check=True)
        print("[INFO] Copy completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to copy directory from container: {e}")
    return "done"


def compress_local_metabase_db_to_zip(folder_to_zip):
    if not os.path.exists(folder_to_zip):
        print(f"[ERROR]: local Metabase db not found at {folder_to_zip}")

    shutil.make_archive(folder_to_zip, 'zip', folder_to_zip)
    print(f"[INFO] Success zip the metabase_db: {folder_to_zip}.zip")
    # Remove the uncompressed metabase_db folder
    shutil.rmtree(folder_to_zip)
    print(f"[INFO] Removed original uncompressed metabase_db folder: {folder_to_zip}")
    return folder_to_zip + ".zip"