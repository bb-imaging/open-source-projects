import orthanc
import json
import redis
import os


def OnStoredInstance(dicom, instanceId):
    STAGE = os.getenv("STAGE") or "dev"

    host = "localhost" if STAGE == "local" else "redis"
    redis_client = redis.Redis(host=host, port=6379, db=0, decode_responses=True)
    if redis_client.ping():
        print(f"redis connected at {host}:6379")
    else:
        print(f"Unable to connect to redis at  {host}:6379")

    dicom_data = json.loads(dicom.GetInstanceSimplifiedJson())
    current_sweep_direction = redis_client.get(
        f'{dicom_data["StudyInstanceUID"]}_currentSweepDirection'
    )
    if current_sweep_direction:
        current_sweep_direction = current_sweep_direction.replace('"', "")
    print(
        f'{dicom_data["StudyInstanceUID"]}_currentSweepDirection = {current_sweep_direction}'
    )

    redis_client.publish(
        "SWEEP_STORED",
        json.dumps(
            {
                "dicom": json.loads(dicom.GetInstanceSimplifiedJson()),
                "instanceId": instanceId,
                "current_sweep_direction": current_sweep_direction,
            }
        ),
    )

    print(
        "Received instance %s of size %d (transfer syntax %s, SOP class UID %s)"
        % (
            instanceId,
            dicom.GetInstanceSize(),
            dicom.GetInstanceMetadata("TransferSyntax"),
            dicom.GetInstanceMetadata("SopClassUid"),
        )
    )

    # Print the origin information
    if dicom.GetInstanceOrigin() == orthanc.InstanceOrigin.DICOM_PROTOCOL:
        print("This instance was received through the DICOM protocol")
    elif dicom.GetInstanceOrigin() == orthanc.InstanceOrigin.REST_API:
        print("This instance was received through the REST API")


orthanc.RegisterOnStoredInstanceCallback(OnStoredInstance)
