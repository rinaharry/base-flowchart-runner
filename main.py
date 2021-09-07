 # settings.py
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

WORKSPACE_JOB_ID = os.environ.get("WORKSPACE_JOB_ID")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD")

def base_flowchart_runner(event, context):
     print("Env with func kub",WORKSPACE_JOB_ID)

     return "Good job" 
