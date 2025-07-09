import oci
import time

source_region = 'us-phoenix-1'
target_region = 'us-ashburn-1'


config = oci.config.from_file("~/.oci/config")
core_client = oci.core.ComputeClient(config)
source_client = oci.core.BlockstorageClient(config)
target_client = oci.core.BlockstorageClient(config, region=target_region)
list_instances_response=[]
list_boot_volume_attachments_response=[]
# Get the list of instances in the source region
list_instances_response = core_client.list_instances(
    compartment_id="ocid1.compartment.oc1..aaaaaaaagxzb3ytnmfjiggqby36pjhrcn35kcp5eea4xlgtwv26e73b7rl2q"
    )
for item in list_instances_response.data:
    # Get the list of boot volume attachments for the instance
    boot_volume_attachments_response = core_client.list_boot_volume_attachments(
        availability_domain=item.availability_domain,
        compartment_id=item.compartment_id,
        instance_id= item.id
    )
    list_boot_volume_attachments_response.append(boot_volume_attachments_response.data[0])

# Get the latest boot volume backup for the VM
backup_details=[]
for item in list_boot_volume_attachments_response:
    backups = source_client.list_boot_volume_backups(compartment_id=item.compartment_id,boot_volume_id=item.boot_volume_id,sort_by='TIMECREATED',sort_order='DESC')
    if backups.data:
        for backup in backups.data:
            if backup.type == "FULL":
                backup_details.append(backup)
                # Since we are interested in the latest FULL backup, we break the loop
                break
# Create the copy request
copy_details = oci.core.models.CopyBootVolumeBackupDetails(destination_region=target_region)
for item in backup_details:
    try:
        copy_boot_volume_backup_response = source_client.copy_boot_volume_backup(
            boot_volume_backup_id=item.id,
            copy_boot_volume_backup_details=copy_details
        )
    except oci.exceptions.ServiceError as e:
        if e.code == "LimitExceeded" and "copy_boot_volume_backup" in e.operation_name:
            print("Reached limit for parallel cross-region copies. Waiting and retrying...")
            time.sleep(60)  # Wait for 60 seconds before retrying
            continue  # Retry the copy operation
        else:
            print(f"An error occurred while copying backup {item.display_name}: {e}")
            continue  # Skip this backup and continue with the next one
        
    while True:
        try:
            copy_operation = target_client.get_boot_volume(boot_volume_id=copy_boot_volume_backup_response.data.boot_volume_id)
            if copy_operation.data.lifecycle_state == 'AVAILABLE':
                print(f"Backup {item.display_name} copied to {target_region} as {copy_operation.data.display_name}")
                break
            else:
                print(f"Backup {item.display_name} is in state {copy_operation.data.lifecycle_state} in {target_region}")
                time.sleep(5)
        except oci.exceptions.ServiceError as e:
            print(f"An error occurred while checking copy status for backup {item.display_name}: {e}")
            time.sleep(5)