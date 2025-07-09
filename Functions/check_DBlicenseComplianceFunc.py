import io
import json
import oci

from fdk import response

def handler(ctx, data: io.BytesIO = None):
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()

    # Create a Database Client
    database_client = oci.database.DatabaseClient(config={}, signer=signer)

    # Fetch OCPUs for specified compartments
    compartments = ["ocid1.compartment.oc1..aaaaaaaag7i5udfevx5ken6cizlvmaxavs4bwowhu5uju525gjng6bv7java",
                    "ocid1.compartment.oc1..aaaaaaaazj7knskadrghyj3u5ydzfpqtxdbhijbh5nbukpkhl2mx46a6wada",
                    "ocid1.compartment.oc1..aaaaaaaaeuw5rxabf4gquj3v3f6fnznxcs4otnckkvm6tdjuyuf3uouplq6q"]

    total_dbsc_ocpus = 0
    total_exacs_ocpus = 0
    total_ecpu = 0

    for compartment_id in compartments:
        dbsc_ocpus, exacs_ocpus, ecpu = fetch_ocpus(database_client, compartment_id)
        total_dbsc_ocpus += dbsc_ocpus
        total_exacs_ocpus += exacs_ocpus
        total_ecpu += ecpu

    total_ocpus = total_dbsc_ocpus + total_exacs_ocpus + total_ecpu

    # Capture the latest DB License Count from Table
    nosql_client = oci.nosql.NosqlClient(config={}, signer=signer)
    query_statement = "SELECT * FROM BYOL_TRACKING"
    query_details = oci.nosql.models.QueryDetails(
        statement=query_statement,
        compartment_id="ocid1.compartment.oc1..aaaaaaaaf224un52mucus6itsvarcqqcekwwfp2rrgfpqyntm3k37u3kcs3q"
    )
    query_response = nosql_client.query(query_details)
    sorted_data = sorted(query_response.data.items, key=lambda x: x['Date'], reverse=True)
    Db_LicenseCount = sorted_data[0]['OCPU_Count']

    # Check if the total OCPUs exceed the licensed count
    if total_ocpus > Db_LicenseCount:
        # Publish a message to the specified topic if there is a violation
        ons_client = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)
        topic_ocid = "ocid1.onstopic.oc1.phx.aaaaaaaams3o5atjlwu6xshjaap6difh7njz54dh52gsnw33hkhdkpkfx6za"
        message = "Hello team,\n\nCurrent OCPU and ECPU Count is {} , which is greater than the actual License Count {}.\n\nThanks".format(total_ocpus, Db_LicenseCount)
        publish_message_response = ons_client.publish_message(
            topic_id=topic_ocid,
            message_details=oci.ons.models.MessageDetails(
                body=message,
                title="OCI Tenancy not in compliance with Oracle DB licensing"),
            message_type="RAW_TEXT")

        if publish_message_response.status == 200:
            print("Message published successfully!")
        else:
            print("Error publishing message. Status code:", publish_message_response.status)

    # Return a JSON response
    resp = {"total_ocpus": total_ocpus}
    return response.Response(
        ctx,
        response_data=json.dumps(resp),
        headers={"Content-Type": "application/json"}
    )

def fetch_ocpus(database_client, compartment):
    dbsc_ocpus = 0
    exacs_ocpus = 0
    ecpu = 0

    # Fetching OCPUs of base database
    list_db_systems_response = database_client.list_db_systems(
        compartment_id=compartment)
    for db_system in list_db_systems_response.data:
        # Check for the license type
        if db_system.license_model == "BRING_YOUR_OWN_LICENSE":
            dbsc_ocpus += db_system.cpu_core_count
    
    # Fetching OCPUs of Exa VMCluster database
    list_cloud_vm_clusters_response = database_client.list_cloud_vm_clusters(
        compartment_id=compartment)
    for vm_cluster in list_cloud_vm_clusters_response.data:
        # Check for the license type
        if vm_cluster.license_model == "BRING_YOUR_OWN_LICENSE":
            exacs_ocpus += vm_cluster.cpu_core_count
    
    # Fetching OCPUs of Autonomous Databases
    list_autonomous_databases_response = database_client.list_autonomous_databases(
        compartment_id=compartment)
    for autonomous_db in list_autonomous_databases_response.data:
        if autonomous_db.license_model == "BRING_YOUR_OWN_LICENSE":
            if autonomous_db.is_auto_scaling_enabled:
                # If autoscaling is enabled, use current OCPU and multiply by 3
                ecpu += autonomous_db.cpu_core_count * 3
            else:
                # If autoscaling is not enabled, use the specified CPU core count
                ecpu += autonomous_db.cpu_core_count
    return dbsc_ocpus, exacs_ocpus, ecpu


