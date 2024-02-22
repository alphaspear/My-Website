from collections import defaultdict
from typing import List, Tuple

import paramiko

from src.poller.file_watcher import FileWatcher
from src.utils.utils import get_uuid, get_current_timestamp_local
from src.poller.file_downloader import download_single_file_and_update_entry
from src.persistence import (
    get_poller_job_config_list,
    get_poller_job_exec_in_waiting_status_by_profile_id,
    create_poller_job_exec,
    create_poller_job_file_entries,
    get_poller_job_files_in_pending_status,
    update_poller_job_exec,
    update_poller_job_file_info_by_exec_id_and_file_path,
)
from src.pubsub.pubsub import send_pubsub
from src.data.data_models import JobFileInfo, FileTypeValue, FileStatus
from src.config import BASE_PATH


def main():
    run_id = get_uuid()
    current_ts = get_current_timestamp_local()
    run_id_with_current_ts = f"{run_id}_{current_ts}"

    logging = initialize_logging(run_id_with_current_ts)

    start_ts = "some timestamp"

    host_name = platform.node()
    log_parameters(logging, host_name, start_ts)

    poll_files(logging)


def initialize_logging(run_id_with_current_ts):
    file_log_folder = f"{BASE_PATH}/logs"
    create_folder(file_log_folder)
    file_log_path = f"{file_log_folder}/{run_id_with_current_ts}"
    logging = get_logger(file_log_path)
    logging.info(f"The file log is: {file_log_path}.log")
    return logging


def log_parameters(logging, host_name, start_ts):
    logging.info(f"HOST_NAME={host_name}")
    logging.info(f"sys.argv={sys.argv}")
    logging.info(f"start_ts={start_ts}")


def poll_files(logging):
    logging.info(f"Start File Watcher ...")
    logging.info(f"File Watcher - Start polling for files...!")

    file_watcher = FileWatcher()
    file_list: List[paramiko.SFTPAttributes] = file_watcher.poll_file_list(None)

    if not file_list or len(file_list) == 0:
        logging.info(f"File Watcher - Found no files. Exit!")
        print("No files matched with any profile's file name regular expression. Exit!")
        sys.exit(0)
    else:
        logging.info("File Watcher - Found below files:")
        log_file_list(logging, file_list)

    profile_filename_dict = extract_profile_filename_dict(file_list)

    process_profiles(logging, profile_filename_dict)


def log_file_list(logging, file_list):
    for file in file_list:
        logging.info(f"filename={file.filename} ; size={file.st_size}; mtime={file.st_mtime}")


def extract_profile_filename_dict(file_list):
    profile_filename_dict = defaultdict(list)
    for file in file_list:
        data = file.filename.split("-", 1)
        profile_filename_dict[data[0]].append(file.filename)
    return profile_filename_dict


def process_profiles(logging, profile_filename_dict):
    for profile_id, file_name_list in profile_filename_dict.items():
        logging.info(f"File Watcher - File name matching for profile_id={profile_id}:")
        try:
            handle_data_for_each_profile_id(logging, profile_id, file_name_list)
        except Exception as ex:
            logging.error(ex)
            print(f"error = {sys.exc_info()[-1].tb_lineno}")
            break
    print("Done")
    logging.info("Done :)")


def handle_data_for_each_profile_id(logging, profile_id, file_name_list):
    poller_job_config_list = get_poller_job_config_list(profile_id)
    waiting_poller_job_exec_list = get_poller_job_exec_in_waiting_status_by_profile_id(profile_id)

    process_waiting_jobs(logging, waiting_poller_job_exec_list, file_name_list)


def process_waiting_jobs(logging, waiting_poller_job_exec_list, file_name_list):
    if len(waiting_poller_job_exec_list) > 0:
        for prev_execution in waiting_poller_job_exec_list:
            prev_exec_id = prev_execution.exec_id
            logging.info(f"Prev exec_id={prev_exec_id}")

            pending_job_files = get_pending_job_files(prev_exec_id, logging)

            present_pending_files = get_present_pending_files(file_name_list, pending_job_files)

            update_poller_job_files(prev_exec_id, present_pending_files)

            if len(pending_job_files) == len(present_pending_files):
                end_ts = get_current_timestamp_utc_sql()
                update_poller_job_exec(prev_exec_id, prev_execution.start_ts, end_ts, JobStatusValue.DOWNLOAD_COMPLETE.name, status_message="")
                handle_control_file(prev_execution, present_pending_files)

            logging.info("Done")
        logging.info("================================PREV EXECUTION WAITING END================================")


def get_pending_job_files(prev_exec_id, logging):
    pending_job_files = get_poller_job_files_in_pending_status(prev_exec_id)
    pending_job_file_names = [job_file.file_path for job_file in pending_job_files]
    logging.info(f"pending_job_file_names={pending_job_file_names}")
    return pending_job_files


def get_present_pending_files(file_name_list, pending_job_files):
    present_pending_files = list(set(file_name_list).intersection(pending_job_files))
    logging.info(f"present_pending_files={present_pending_files}")
    return present_pending_files


def update_poller_job_files(prev_exec_id, present_pending_files):
    for file_name in present_pending_files:
        job_file_info = construct_poller_file_info_obj(file_name, file_attr_dict, prev_exec_id, is_data_file=True)
        logging.info(f"constructed poller file info: {job_file_info}")
        poller_job_file = update_poller_job_file_info_by_exec_id_and_file_path(prev_exec_id, file_name, job_file_info)
        download_single_file_and_update_entry(file_name, prev_exec_id, is_data_file=True, logging=logging)


def handle_control_file(prev_execution, present_pending_files):
    staging_path = get_staging_base_path() + "/" + prev_exec_id
    if prev_execution.control_file_count == 0:
        control_file_path = ""
    else:
        control_job_file_list = get_control_poller_job_file_by_exec_id(prev_exec_id)
        if len(control_job_file_list) == 0:
            raise ValueError(f"No persistent control file found by the exec_id={prev_exec_id}")
        control_job_file = control_job_file_list[0]
        logging.info(f"The persistent control file is {control_job_file}")
        control_file_path = f"{staging_path}/control/{control_job_file.file_path.split('/')[-1]}"

    if control_file_path:
        logging.info("sending pubsub message")
        send_pubsub(
            poller_id=prev_execution.poller_id,
            control_file_path=control_file_path,
            data_file_path=f"{staging_path}/data",
            dataset_id=poller_job_config.dataset_id,
            parent_dataset_id=poller_job_config.parent_dataset_id,
            execution_id=prev_exec_id
        )
