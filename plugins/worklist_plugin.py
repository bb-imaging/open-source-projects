import json
import orthanc
import os
import boto3
from datetime import date

WORKLIST_BUCKET_NAME = os.getenv("WORKLIST_BUCKET_NAME")

def get_worklist_answers(prefix, query, answers):
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource("s3")
    s3_bucket = s3_resource.Bucket(WORKLIST_BUCKET_NAME)
    files = s3_bucket.objects.filter(Prefix=prefix)

    for file in files:
        s3_response_object = s3_client.get_object(Bucket=WORKLIST_BUCKET_NAME, Key=file.key)
        object_content = s3_response_object['Body'].read()

        # Test whether the query matches the current worklist
        if query.WorklistIsMatch(object_content):
            orthanc.LogInfo(f'Matching worklist: {file.key}')
            answers.WorklistAddAnswer(query, object_content)


def worklist_callback(answers, query, issuerAet, calledAet):
    '''
    Signature of a callback function that is triggered when Orthanc receives a C-Find SCP request against modality worklists.

            Parameters:
                    answers (OrthancPluginWorklistAnswers *): The target structure where answers must be stored.
                    query (OrthancPluginWorklistQuery *): The worklist query.
                    issuerAet (const char *): The Application Entity Title (AET) of the modality from which the request originates.
                    calledAet (const char *): The Application Entity Title (AET) of the modality that is called by the request.

            Returns:
                    0 if success, other value if error.
    '''
    PLUGINS_ENABLED = os.getenv("PLUGINS_ENABLED", "false")
    if not PLUGINS_ENABLED == "true":
        orthanc.LogInfo("Plugins disabled. Skipping worklist retrieval")
        return

    # TODO: Handle if the path doesn't exist (No worklist created before FIND query)
    orthanc.LogInfo(
        f"Received incoming C-FIND worklist request from {issuerAet} calling {calledAet}:")

    # Get a memory buffer containing the DICOM instance
    dicom = query.WorklistGetDicomQuery()

    # Get the DICOM tags in the JSON format from the binary buffer
    json_tags = json.loads(orthanc.DicomBufferToJson(
        dicom, orthanc.DicomToJsonFormat.SHORT, orthanc.DicomToJsonFlags.NONE, 0))

    orthanc.LogInfo(f"C-FIND worklist request to be handled in Python: " +
                    json.dumps(json_tags, indent=4, sort_keys=True))

    orthanc.LogInfo(f"will look for worklist in {WORKLIST_BUCKET_NAME}")

    _, tenant_id, clinic_id, ultrasound_machine_id = calledAet.split("_")
    today = date.today().strftime("%d_%B_%Y") # '03_August_2022'
    prefix = f'{tenant_id}/{clinic_id}/{ultrasound_machine_id}/{today}'
    get_worklist_answers(prefix, query, answers)


orthanc.RegisterWorklistCallback(worklist_callback)
