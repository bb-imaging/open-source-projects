import json
import orthanc
import os

# Path to the directory containing the DICOM worklists
WORKLIST_DIR = os.getenv('WORKLIST_DIR', '/mnt/efs')


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

    aet_worklist_dir = os.path.join(WORKLIST_DIR, calledAet)

    orthanc.LogInfo(f"will look for worklist in {aet_worklist_dir}")

    # Loop over the available DICOM worklists
    for path in os.listdir(aet_worklist_dir):
        if os.path.splitext(path)[1] == '.wl':
            with open(os.path.join(aet_worklist_dir, path), 'rb') as f:
                content = f.read()

                # Test whether the query matches the current worklist
                if query.WorklistIsMatch(content):
                    orthanc.LogInfo('Matching worklist: %s' % path)
                    answers.WorklistAddAnswer(query, content)


orthanc.RegisterWorklistCallback(worklist_callback)
