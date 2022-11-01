import orthanc
import json
import pika
import os
import ssl
import traceback
from time import sleep


def OnStoredInstance(dicom, instanceId):
    TENANT_ID = os.getenv("TENANT_ID")
    ORTHANC_TENANT_PATH = os.getenv("ORTHANC_TENANT_PATH")
    RMQ_HOST = os.getenv("RMQ_HOST")
    RMQ_PASSWORD = os.getenv("RMQ_PASSWORD")
    RMQ_USERNAME = os.getenv("RMQ_USERNAME")
    RMQ_PORT = os.getenv("RMQ_PORT")
    PLUGINS_ENABLED = os.getenv("PLUGINS_ENABLED", "false")

    if not PLUGINS_ENABLED == "true":
        orthanc.LogInfo(
            "Received instance %s of size %d (transfer syntax %s, SOP class UID %s)"
            % (
                instanceId,
                dicom.GetInstanceSize(),
                dicom.GetInstanceMetadata("TransferSyntax"),
                dicom.GetInstanceMetadata("SopClassUid"),
            )
        )
        orthanc.LogInfo("Plugins disabled. Skipping sending event")
        return

    if ORTHANC_TENANT_PATH == "orthanc-local-main":
        credentials = pika.PlainCredentials(RMQ_USERNAME, RMQ_PASSWORD)
        parameters = pika.ConnectionParameters("rabbitmq", RMQ_PORT, "/", credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
    else:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

        credentials = pika.PlainCredentials(RMQ_USERNAME, RMQ_PASSWORD)
        parameters = pika.ConnectionParameters(
            RMQ_HOST, RMQ_PORT, '/', credentials)
        parameters.ssl_options = pika.SSLOptions(context=ssl_context)
        connection = pika.BlockingConnection(parameters)

    if not connection or connection.is_closed:
        connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(exchange='example_exchange',
            exchange_type="direct",
            durable=True
        )

    retries = 1
    while retries <= 5:
        try:

            channel.basic_publish(exchange='example_exchange',
                                routing_key='EXAMPLE_ROUTE_KEY',
                                body=json.dumps(
                                    {
                                        "dicom": json.loads(dicom.GetInstanceSimplifiedJson()),
                                        "instanceId": instanceId,
                                        "tenantId": TENANT_ID,
                                        "orthancTenantPath": ORTHANC_TENANT_PATH
                                    }
                                ))

                                              
            break
        except Exception as e:
            orthanc.LogError(
                "Unable to send EXAMPLE_ROUTE_KEY event. Error: %s\nTry %s/5" %
                (e, retries)
            )
            if retries == 5:
                orthanc.LogError(
                    "Traceback: %s" %
                    (traceback.format_exc())
                )
                raise e
            retries += 1
            sleep(1)

    orthanc.LogInfo(
        "Received instance %s of size %d. Tenant: %s. (transfer syntax %s, SOP class UID %s)"
        % (
            instanceId,
            dicom.GetInstanceSize(),
            ORTHANC_TENANT_PATH,
            dicom.GetInstanceMetadata("TransferSyntax"),
            dicom.GetInstanceMetadata("SopClassUid"),
        )
    )

    # Print the origin information
    if dicom.GetInstanceOrigin() == orthanc.InstanceOrigin.DICOM_PROTOCOL:
        orthanc.LogInfo(
            "This instance was received through the DICOM protocol")
    elif dicom.GetInstanceOrigin() == orthanc.InstanceOrigin.REST_API:
        orthanc.LogInfo("This instance was received through the REST API")


orthanc.RegisterOnStoredInstanceCallback(OnStoredInstance)
