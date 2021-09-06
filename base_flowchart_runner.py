# Change this config by the config
CONFIG = {
    "UNIQUE_LABEL": "",
    "PROJECT_OBJECT": "",
    "INSTANCE_LIST": "",
    "MODULE_MAP": "",

    "GQL_CREDENTIALS": "",

    "AWS_ACCESS_KEY_ID": "",
    "AWS_SECRET_ACCESS_KEY": "",
    "AWS_REGION_NAME": "",
    "AWS_SMARTPREDICT_BUCKET": "",

    "REDIS_HOST": "",
    "REDIS_PORT": "",
    "REDIS_DB": "",
    "REDIS_PASSWORD": "",

    "AMQP_URL": "",
    "LOGS_QUEUE_NAME": "",
    "AUTOFLOW_MESSAGING_QUEUE": "",

    "SENTRY_DSN": "",
    "SENTRY_ENV": "",

    "PROJECT_ID": "",
    "SP_TEAM_ID": "",
    "USER_ID": "",
    "AUTHORIZATION": "",
    "WORKSPACE_JOB_ID": "",
    "LOG_ID": "",

    "REST_SERVER_HOST": "",
    "REST_SERVER_PORT": "",
    "G_SERVICE_ACCOUNT_CRED_JSON": "",
    "G_BIG_QUERY_PROJECT_ID": ""
}

# ============== x =============== #

import logging
import shutil
import sys
from logging import getLogger
from os import makedirs, mkdir
from os.path import abspath, isdir, join

import sentry_sdk
import ujson
from smartpredict_common_lib.config import SclConfig
from smartpredict_common_lib.ext.redis import set_webservice_running_state
from smartpredict_common_lib.projects.web_service import WebServiceDeployment
from smartpredict_common_lib.sp_constants import WebserviceRunningState

logging.basicConfig(level="DEBUG")
logger = logging.getLogger('API_SERVER')

# Transform configurations
UNIQUE_LABEL = CONFIG["UNIQUE_LABEL"]
LOG_ID = CONFIG["LOG_ID"]
USER_STORAGE_ROOT = "/"
GQL_CRED = ujson.loads(CONFIG["GQL_CREDENTIALS"]) if \
    CONFIG["GQL_CREDENTIALS"] else {}

assert CONFIG["AWS_ACCESS_KEY_ID"] is not None, \
    "AWS_ACCESS_KEY_ID is not set"
assert CONFIG["AWS_SECRET_ACCESS_KEY"] is not None, \
    "AWS_SECRET_ACCESS_KEY is not set"
assert CONFIG["AWS_REGION_NAME"] is not None, \
    "AWS_REGION_NAME is not set"
assert CONFIG["AWS_SMARTPREDICT_BUCKET"] is not None, \
    "AWS_SMARTPREDICT_BUCKET is not set"
assert CONFIG["AMQP_URL"] is not None, \
    "AMQP_URL must be set"
assert CONFIG["G_SERVICE_ACCOUNT_CRED_JSON"] is not None, \
    "G_SERVICE_ACCOUNT_CRED_JSON must be set"
assert CONFIG["G_BIG_QUERY_PROJECT_ID"] is not None, \
    "G_BIG_QUERY_PROJECT_ID must be set"


def configure_storage_root():
    storage_root = abspath(join("..", ".storage"))
    # Folder where to store modules when they are loaded.
    temp_storage_root = abspath(join("/tmp", "smart_predict.engine"))
    if isdir(temp_storage_root):
        getLogger("smart_predict").info(f"Cleaning {temp_storage_root}")
        shutil.rmtree(temp_storage_root)
    if not isdir(temp_storage_root):
        mkdir(temp_storage_root)
    sys.path.append(temp_storage_root)
    # User storage folder.
    global USER_STORAGE_ROOT
    USER_STORAGE_ROOT = join(storage_root, "users")
    if not isdir(USER_STORAGE_ROOT):
        makedirs(USER_STORAGE_ROOT)


def configure_scl():
    SclConfig.USER_STORAGE_ROOT = CONFIG["USER_STORAGE_ROOT"]
    SclConfig.AMQP_URL = CONFIG["AMQP_URL"]
    SclConfig.AWS_ACCESS_KEY_ID = CONFIG["AWS_ACCESS_KEY_ID"]
    SclConfig.AWS_SECRET_ACCESS_KEY = CONFIG["AWS_SECRET_ACCESS_KEY"]
    SclConfig.AWS_REGION_NAME = CONFIG["AWS_REGION_NAME"]
    SclConfig.AWS_SMARTPREDICT_BUCKET = CONFIG["AWS_SMARTPREDICT_BUCKET"]
    SclConfig.REDIS_DB = int(CONFIG["REDIS_DB"])
    SclConfig.REDIS_HOST = CONFIG["REDIS_HOST"]
    SclConfig.REDIS_PASSWORD = CONFIG["REDIS_PASSWORD"]
    SclConfig.REDIS_PORT = int(CONFIG["REDIS_PORT"])
    if GQL_CRED:
        SclConfig.GQL_TOKEN = GQL_CRED["GQL_TOKEN"]
        SclConfig.GQL_ENDPOINT = GQL_CRED["GQL_ENDPOINT"]
    SclConfig.TEMP_MODULE_STORAGE = CONFIG["TEMP_MODULE_STORAGE"]
    SclConfig.SP_TEAM_ID = CONFIG["SP_TEAM_ID"]
    SclConfig.G_SERVICE_ACCOUNT_CRED_JSON = CONFIG[
        "G_SERVICE_ACCOUNT_CRED_JSON"]
    SclConfig.G_BIG_QUERY_PROJECT_ID = CONFIG["G_BIG_QUERY_PROJECT_ID"]
    SclConfig.WORKSPACE_JOB_ID = CONFIG["WORKSPACE_JOB_ID"]
    SclConfig.USER_ID = CONFIG["USER_ID"]
    SclConfig.AUTHORIZATION = CONFIG["AUTHORIZATION"]
    SclConfig.PROJECT_ID = CONFIG["PROJECT_ID"]


def init_sentry():
    sentry_sdk.init(
        CONFIG["SENTRY_DSN"],
        environment=CONFIG["SENTRY_ENV"] or "production",
        traces_sample_rate=1.0
    )


def run_flowchart(input_data=None):
    """Run the flowchart using the configs"""
    # Init configurations
    configure_storage_root()
    init_sentry()
    configure_scl()
    logger.info("\u25C8 Starting the project load and run.")
    logger.info("\u25C8 Running as webservice.")

    try:

        deployment = WebServiceDeployment(
            workspace_job_id=SclConfig.WORKSPACE_JOB_ID,
            log_id=LOG_ID,
            project_object=CONFIG["PROJECT_OBJECT"],
            instances_list=CONFIG["INSTANCE_LIST"],
            modules_map=CONFIG["MODULE_MAP"],
            user_id=SclConfig.USER_ID,
            authorization=SclConfig.AUTHORIZATION,
            unique_label=CONFIG["UNIQUE_LABEL"]
        )

    except Exception as webservice_load_error:
        # Capture all unexpected errors
        getLogger(__name__).exception(
            f"\u25C8 Unexpected error: {webservice_load_error}, exiting..."
        )

        # Set workspace job running state as "FAILED"
        set_webservice_running_state(
            WebserviceRunningState.FAILED,
            SclConfig.WORKSPACE_JOB_ID,
            SclConfig.USER_ID,
            UNIQUE_LABEL,
            SclConfig.AUTHORIZATION,
            SclConfig.PROJECT_ID
        )
        deployment = None

    logger.debug("Accessing the webservice from REST endpoint.")
    logger.debug("Verifying access token...")
    try:
        deployment.verify_token(input_data.access_token)
    except ValueError as access_error:
        logger.debug(f"{access_error}")
        return {
            "success": False,
            "error": str(access_error)
        }

    logger.debug("Access granted.")
    output_data = deployment.access(input_data.input_data)
    logger.debug("Accessing successful.")
    return output_data
