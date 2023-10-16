import boto3
import botocore
import pprint
import time
import sys
import pytest
from datetime import datetime, timezone
from jinja2 import Environment, FileSystemLoader
import shutil
import sys
import os
current_dir = os.getcwd()
parent_dir=os.path.abspath(os.path.join(current_dir,'..'))
sys.path.append(parent_dir)
from Utilties.readProperties import ReadConfig

session=boto3.Session(profile_name='RT')
PrescenceCheck=ReadConfig.getData("Path of the file","FilePath")

#List out the buckets in AWS and move the chosen file to destination location.

def list_bucket():
    s3_object=session.resource('s3)
    print(s3_object)
    source_path=ReadConfig.getData("Path of the file","filepath")
    destinationpath=ReadConfig.getData("Path of the file", "Finaloutputpath")
    shutil.copy(source_path,destinationpath)

#List out the buckets in AWS and move the chosen file with trailer file to destination location.
def list_bucket_Trl():
  s3_object=session.resource('s3')
  print(s3_object)
- source_path=ReadConfig.getData("Path of the file","filepath")
  trailer_path=ReadConfig.getData("Path of the file","trailerpath")
  destinationpath=ReadConfig.getData("Path of the file","Finaloutputpath")
  shutil.copy(source_path,destinationpath)
  shutil.copy(trailer_path,destinationpath)

#generate HTML report for Ingestion Process
def generate_html_report(jobname, status, starttime, Completedtime, run_id, current_time):
    output_file=Read.config.getData('path of the file','report_path')
    report_dir=os.path.join(output_file, "Archive Ingestion reports")
    os.makedirs(report_dir, exist_ok=True)
    current_time1= datatime.now().strftime("%Y-%m-%d_%H-%M-%S")
    env=Environment(;oader=FileSystemLoader('.))
    template = env.get_template('report_template.html)
    html_report=template.render(job_name=jobname, status=status, starttime=starttime, ingestion_time=Completedtime, run_id=run_id, current_time=current_time)
    report_filename=f"report_{jobname}_{current_time1}.html"
    report_path=os.path.join(output_file, report_filename)
    with open (report_path,'w') as file:
        file.write(html_report)
   print("HTML report generated successfully")
   return report_path

#Moving the Archive report to another folder
def move_old_reports(output_path, archive_path):
   os.makedirs(archive_path,exist_ok=True)
   reports=[file for file in os.listdir(output_path)if file.startswith("report)")]
   for report in reports:
       report_path=os.path.join(output_path,report)
       destination_path=os.path.join(archive_path, report)
       shutil.move(report_path, destination_path)

# ingestion the file into specified folder
def ingest_file(filename, primarypath, secondarypath):
    s3_client=session.client('s3)

    try:
        local_file_name=filename
        aws_bucket_name=primarypath
        aws_obj_name=secondarypath+filename
        response=s3_client.upload_file(local_file_name, aws_bucket_name, aws_obj_name)
   except botocore.exceptions.ClientError as e:
      print(e)

#Capturing Gluestatus for the each run
def gluestatus(jobname):
    try:
        glue_client=session.client('glue')
        time.sleetp(10)
        response=glue_client.get_job_runs(JobName=jobname)
        job_run=response['JobRuns'][0]

        status = job_run['JobRunState']

        starttime=job_run['StartedOn']
        ingestion_time=job_run['CompletedOn']
        run_id=job_run['Id']
        current_time=datetime.now().replace(microsecond=0)
        if ingestion_time>starttime:
           print("Intestion is still in progress.Watiting.....")
        print("File got uploaded Successfully. Ingestion is in progress.....")

        while status not in ['SUCCEEDED','FAILED'];
            response=glue_client.get_job_runs(JobName=jobname)
            if 'JobRuns' not in response or len(response[JobRuns'])==0:
                print("No job runs found")
                return False
            job_run=response['JobRuns'][0]
            status=job_Run['JobRunState']
            if status in [SUCCEEDED','FAILED']:
                break

       print("Ingestion Status:", status)
       generate_html_report(jobname, status, starttime, ingestion_time, run_id, current_Time)
       print(status)
       return True
 except botocore.exceptions.ParamValidationError as e:
     print(e)

#ingesting file with trailer file into specified folder
def ingest_filewithtrailer(filename, primarypath, secondarypath, trailerfile):
    s3_client=session.client('s3')

    try:
        local_file_name=filename
        trailerfile=trailerfile
        aws_bucket_name=primarypath
        aws_obj_name=secondarypath+filename
        aws_obj_name1=secondarypath+trailerfile
        time.sleep(3)
        s3_client.upload_file(local_file_name, aws_bucket_name, aws_obj_name)
        time.sleep(4)
        s3_client_upload_file(trailerfile,aws_bucket_name,aws_obj_name1)
    except botocore.exceptions.ClientError as e:
        print(e)


#Ingesting of memberroster file alone
def ingest_memberroster(filename, primarypath):
    s3_client=session.client('s3')

    try:
        print('Ingest member roster')
        local_file_name=filename

        aws_bucket_name=primarypath
        aws_bucket_name1='provider/provider-intake/roster/'+filename

        response=s3_client.upload_file(local_file_name,aws_bucket_name,aws_bucket_name1)
        print(response)
    except botocore.exceptions.ClientError as e:
        print(e)

#condition to pick specific file and path
if "CUST" in PrescenceCheck:
   list_bucket()
   print("Patient file is getting Ingested")
   
        
        
        

