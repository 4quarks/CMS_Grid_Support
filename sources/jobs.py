import datetime
import json
from query_utils import get_data_grafana, get_str_lucene_query


query = "data.DESIRED_Sites:T2_US_MIT AND data.Type:test"

cmssst_index = {"name": "monit_prod_condor_raw_metric*", "id": "8787"}

"""
------------------------------------------------
data.CRAB_PostJobStatus 	Running
data.Status 	Running

_id 	crab3@vocms0107.cern.ch#59356655.0#1601911341#1601912162
data.CMSGroups 	/cms/GGUSExpert, /cms, T3_US_TAMU, /cms/TEAM, T3_US_FNALLPC, /cms/ALARM
data.CMSPrimaryDataTier 	AODSIM
data.CMSSWVersion 	9_2_6
data.CMS_CampaignType 	UNKNOWN
data.CMS_JobType 	Analysis
data.CMS_Pool 	Global
data.CMS_SubmissionTool 	CRAB
data.CMS_WMTool 	HammerCloud
data.CRAB_DataBlock 	/GenericTTbar/HC-CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/AODSIM#3517e1b6-76e3-11e7-a0c8-02163e00d7b3
data.CRAB_Retry 	0
data.CRAB_UserHN 	sciaba
data.CRAB_Workflow 	201004_231110:sciaba_crab_HC-95-T2_US_MIT-93085-20201005005702
data.DESIRED_CMSDataset 	/GenericTTbar/HC-CMSSW_9_2_6_91X_mcRun1_realistic_v2-v2/AODSIM
data.DESIRED_Sites 	T2_US_MIT
data.GlobalJobId 	crab3@vocms0107.cern.ch#59356655.0#1601911341
data.HasSingularity 	true
data.InputData 	Onsite
data.JobCurrentStartDate 	Oct 5, 2020 @ 17:22:38.000
data.RemoteHost 	slot1_8@glidein_412354_105995905@HIBAT0074.CMSAF.MIT.EDU
data.RequestCpus 	1
data.RequestMemory 	2,000
data.ScheddName 	crab3@vocms0107.cern.ch
data.Site 	T2_US_MIT
data.Tier 	T2
data.Type 	test

------------------------------------------------
data.CRAB_PostJobStatus 	failed
data.Status 	Completed

data.ErrorClass 	Application
data.ErrorType 	Executable
data.ExitCode 	139
data.ExitStatus 	0
data.CRAB_HC 	true
data.CRAB_JobArch 	slc6_amd64_gcc530
data.CRAB_JobLogURL 	https://cmsweb.cern.ch/scheddmon/0121/cmsprd/201005_013813:sciaba_crab_HC-101-T1_DE_KIT-93088-20201005033703/job_out.163.0.txt
data.CRAB_PostJobLogURL 	https://cmsweb.cern.ch/scheddmon/0121/cmsprd/201005_013813:sciaba_crab_HC-101-T1_DE_KIT-93088-20201005033703/postjob.163.0.txt
data.CRAB_PrimaryDataset 	GenericTTbar
data.CRAB_Retry 	0
data.CRAB_SaveLogsFlag 	false
data.CondorExitCode 	139
data.EstimatedWallTimeMins 	300
data.JobExitCode 	139
data.JobFailed 	1
data.VO 	cms
data.Chirp_CRAB3_Job_ExitCode 	8,020
data.Chirp_WMCore_cmsRun_Exception_Message 	b"An exception of category 'FileOpenError' occurred ..."
------------------------------------------------
data.CRAB_PostJobStatus 	finished
data.Status 	Completed

data.ErrorClass 	Success
data.ErrorType 	Success
data.ExitCode 	0
data.ExitStatus 	0
data.JobFailed 	0
------------------------------------------------
"""

now_datetime = datetime.datetime.now()
yesterday_datetime = now_datetime - datetime.timedelta(hours=1)
max_time = round(datetime.datetime.timestamp(now_datetime)) * 1000
min_time = round(datetime.datetime.timestamp(yesterday_datetime)) * 1000


clean_str_query = get_str_lucene_query(cmssst_index['name'], min_time, max_time, query)
response = json.loads(get_data_grafana(cmssst_index['id'], clean_str_query).text.encode('utf8'))

print(response)




































































