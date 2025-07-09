
import oci


config = oci.config.from_file('~/.oci/config',profile_name='rnd-phx')


# Initialize service client with default config file
compute_instance_agent_client = oci.compute_instance_agent.ComputeInstanceAgentClient(
    config)
with open('/Users/achyuthnaidu/Desktop/Work/PGE/Code/updateetchosts.sh', 'r') as f:
    script = f.read()

# Send the request to service, some parameters are not required, see API
# doc for more info
create_instance_agent_command_response = compute_instance_agent_client.create_instance_agent_command(
    create_instance_agent_command_details=oci.compute_instance_agent.models.CreateInstanceAgentCommandDetails(
        compartment_id="ocid1.compartment.oc1..aaaaaaaa2qluxj6n6c2mqdmxoogph6dmbfgd6ftwmh2pzy67wxuty6orrofa",
        execution_time_out_in_seconds=54085,
        target=oci.compute_instance_agent.models.InstanceAgentCommandTarget(
            instance_id="ocid1.instance.oc1.phx.anyhqljshprl2dicl5sssiv2wdy7kscuxujbhvtnf7rida5uymi3ev5gkocq"),
        content=oci.compute_instance_agent.models.InstanceAgentCommandContent(
            source=oci.compute_instance_agent.models.InstanceAgentCommandSourceViaTextDetails(
                source_type="TEXT",
                text=script),
            output=oci.compute_instance_agent.models.InstanceAgentCommandOutputViaTextDetails(
                output_type="TEXT")),
        display_name="RunCommand-updateHostNames"))


print(create_instance_agent_command_response.data)
