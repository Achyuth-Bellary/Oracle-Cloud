import oci
import json

# Authenticate to the OCI using an API signing key
config = oci.config.from_file(
    file_location="~/.oci/config",
    profile_name="rnd-phx"
)
identity = oci.identity.IdentityClient(config)
root_compartment_id = config["tenancy"]

# Get the details of the VCN and its associated security lists and subnets
source_network_client = oci.core.VirtualNetworkClient(config)
vcn_id = "ocid1.vcn.oc1.phx.amaaaaaahprl2diazvxmhmvi432ah6mkvwbqou7c2ggnf2brmhxdpgoqsrpa"  #Provide the VCN OCI id
vcn_compartment_id = "ocid1.compartment.oc1..aaaaaaaa7gcivlyzwhp4xcaoyzordsony757nw4xlyao4jzgtjczhrkv6j4a"
source_vcn = source_network_client.get_vcn(vcn_id).data
source_security_lists = []
source_subnets = []
source_route_tables =[]
source_internet_gateways= []
source_drgs=[]
source_nat_gateways=[]
source_service_gateways=[]
for source_security_list in source_network_client.list_security_lists(compartment_id=vcn_compartment_id,vcn_id=vcn_id).data:
    source_security_lists.append(source_security_list)
for source_subnet in source_network_client.list_subnets(compartment_id=vcn_compartment_id,vcn_id=vcn_id).data:
    source_subnets.append(source_subnet)
for source_route_table in source_network_client.list_route_tables(compartment_id=vcn_compartment_id,vcn_id=vcn_id).data:
    source_route_tables.append(source_route_table)
for source_internet_gateway in source_network_client.list_internet_gateways(compartment_id=vcn_compartment_id,vcn_id=vcn_id).data:
    source_internet_gateways.append(source_internet_gateway)
for source_drg in source_network_client.list_drgs(compartment_id=vcn_compartment_id,vcn_id=vcn_id).data:
    source_drgs.append(source_drg)
for source_nat_gateway in source_network_client.list_nat_gateways(compartment_id=vcn_compartment_id,vcn_id=vcn_id).data:
    source_nat_gateways.append(source_nat_gateway)
for source_service_gateway in source_network_client.list_service_gateways(compartment_id=vcn_compartment_id,vcn_id=vcn_id).data:
    source_service_gateways.append(source_service_gateway)



# Serialize the VCN and its associated security lists and subnets as JSON
vcn_data = {
    "vcn": source_vcn,
    "security_lists": source_security_lists,
    "subnets": source_subnets,
    "route_tables": source_route_tables,
    "internet_gateways": source_internet_gateways,
    "drgs": source_drgs,
    "nat_gateways": source_nat_gateways,
    "service_gateways": source_service_gateways
}
vcn_json = json.dumps(vcn_data, default=str)

# Write the JSON to a file
with open("vcn_data.json", "w") as f:
    f.write(vcn_json)

# Switch to a different region
# Change the configuration to use the target region
config["region"] = "us-ashburn-1"

# Create the VCN and its components in the target region
target_network_client = oci.core.VirtualNetworkClient(config)
target_compartment =""

# Create the VCN and its associated security lists and subnets in the new region
network_client = oci.core.VirtualNetworkClient(config)
new_vcn = network_client.create_vcn(
    oci.core.models.CreateVcnDetails(
        cidr_block=source_vcn.cidr_block,
        display_name=source_vcn.display_name,
        compartment_id=target_compartment
    )
).data

new_security_lists = []
for security_list in source_security_lists:
    new_security_lists.append(
        network_client.create_security_list(
            oci.core.models.CreateSecurityListDetails(
                compartment_id=new_vcn.compartment_id,
                vcn_id=new_vcn.id,
                display_name=security_list.display_name,
                egress_security_rules=security_list.egress_security_rules,
                ingress_security_rules=security_list.ingress_security_rules
            )
        ).data
    )
    
new_route_tables= []
for route_table in source_route_tables:
    new_route_tables.append(
        network_client.create_route_table(
            oci.core.models.CreateRouteTableDetails(
                compartment_id=new_vcn.compartment_id,
                vcn_id=new_vcn.id,
                display_name=route_table.display_name,
                route_rules=route_table.route_rules
                )
        ).data
    )


new_subnets = []
for subnet in source_subnets:
    new_subnets.append(
        network_client.create_subnet(
            oci.core.models.CreateSubnetDetails(
                compartment_id=new_vcn.compartment_id,
                vcn_id=new_vcn.id,
                display_name=subnet.display_name,
                cidr_block=subnet.cidr_block
            )
        ).data
    )


print("VCN and its associated security lists and subnets created")
