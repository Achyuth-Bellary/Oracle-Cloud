# This is an automatically generated code sample.
# To make this code sample work in your Oracle Cloud tenancy,
# please replace the values for any parameters whose current values do not fit
# your use case (such as resource IDs, strings containing ‘EXAMPLE’ or ‘unique_id’, and
# boolean, number, and enum parameters with values not fitting your use case).

from datetime import datetime
import oci

# Create a default config using DEFAULT profile in default location
# Refer to
# https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm#SDK_and_CLI_Configuration_File
# for more info
config = oci.config.from_file('~/.oci/config')


# Initialize service client with default config file
core_client = oci.core.ComputeClient(config)


# Send the request to service, some parameters are not required, see API
# doc for more info
# update_instance_response = core_client.update_instance(
#     instance_id="ocid1.instance.oc1.phx.anyhqljsbzarz4ycp6ylsolqge32k24utbzof7h325ctp6rtv4zaiddgqhoq",
#     update_instance_details=oci.core.models.UpdateInstanceDetails(
#         display_name="poc_ans"),
#     update_instanceagentdetails=oci.core.models.UpdateInstanceAgentConfigDetails(
#             is_monitoring_disabled=True,
#             is_management_disabled=True)
#    )
instances= oci.core_client.ComputeClient.list_instances(compartment_id="ocid1.compartment.oc1..aaaaaaaal4bxhzyfx7srmxl2s6p7nyqvxi5wwg42lq23i3ue3utj5aumevqq")
update_instance_response = core_client.update_instance(
    instance_id="ocid1.instance.oc1.phx.anyhqljsbzarz4ycbxqifr2rlsa5pbujwx2mxl2cosjr27u73tlh5xjmaopa",
    update_instance_details=oci.core.models.UpdateInstanceDetails(
        display_name="xocidc2ma01a",
        agent_config=oci.core.models.UpdateInstanceAgentConfigDetails(
            
            is_management_disabled=False,
            )))

# Get the data from response
print(update_instance_response.data)
