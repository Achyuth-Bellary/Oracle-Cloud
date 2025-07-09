#ocid1.drprotectiongroup.oc1.iad.aaaaaaaapdall6seeroitb4ncdkpeg7t5ue6f3opeaoi2an6jcyqf76xc4za -DR
#ocid1.drprotectiongroup.oc1.phx.aaaaaaaaxn5dzcr47ugn6htumuyhdozt44k4rtayhhxaslrf6wghdtucjjna -prod
[CmdletBinding()] 
param (
    [Parameter(Mandatory = $True)]
        [string]$compartment,
    [Parameter(Mandatory = $True)]
        [ValidateSet("reg-ash", "reg-phx")]
        [string]$configprofile
)
$pg_list=$(oci disaster-recovery dr-protection-group list --compartment-id $compartment --profile $configprofile | ConvertFrom-Json).data.items.ID
foreach ($pg in $pg_list) {
    $pg_oci_id = $pg
$pgdetails=(oci disaster-recovery dr-protection-group get --dr-protection-group-id $pg_oci_id  --profile $configprofile | ConvertFrom-Json).data
$pgdetails_members= $pgdetails.members
$updated_instanceDetails=@()
$updated_VolumeGroup_Details=@()
$updated_blockVolume_Details=@()
$updated_bootVolume_Details=@()
foreach ($pgdetails_member in $pgdetails_members) {
    Write-Host "==============================================================================="
    # to update of type "COMPUTE_INSTANCE"
    if ($pgdetails_member."member-type" -eq "COMPUTE_INSTANCE") {
        $instance_details= oci compute instance get --instance-id $pgdetails_member.'member-id' --profile $configprofile | ConvertFrom-Json
        Write-Host "Working on oci_id: '$($instance_details.data.id)'  Name: '$($instance_details.data.'display-name')'..."
        if ($($instance_details.data.'display-name') -match "_DR_")
        {
            $new_instance_name= $instance_details.data.'display-name' -split("_DR")
            Write-Host ("Updating the Display-name from '$($instance_details.data.'display-name')' to '$($new_instance_name[0])'") -ForegroundColor Yellow
            $updated_instance_name=oci compute instance update --instance-id $pgdetails_member.'member-id' --display-name $new_instance_name[0] --profile $configprofile | ConvertFrom-Json

            $updated_instanceDetails+= New-Object -TypeName psobject -Property @{
                "Old Instance Name" = $instance_details.data.'display-name'
                "New instance Name" = $updated_instance_name.data.'display-name'
                "Instance OCID"     = $updated_instance_name.data.id
                "Protection Group ID"=$pgdetails.id
            }
        }
        else {
            write-host "Not renaming the instance : $($instance_details.data.'display-name')"
        }
    }
    # to update if type "VOLUME_GROUP"
    if ($pgdetails_member."member-type" -eq "VOLUME_GROUP") {
        $Volume_Group_details = oci bv volume-group get --volume-group-id $pgdetails_member.'member-id' --profile $configprofile | ConvertFrom-Json
        Write-Host "Working on oci_id: '$($Volume_Group_details.data.id)'  Name: '$($Volume_Group_details.data.'display-name')'..."
        if ($($Volume_Group_details.data.'display-name') -match "_DR_")
        {
            $new_VolumeGroup_name= $Volume_Group_details.data.'display-name' -split("_DR")
            Write-Host ("Updating the Display-name from '$($Volume_Group_details.data.'display-name')' to '$($new_VolumeGroup_name[0])'") -ForegroundColor Yellow
            $updated_VolumeGroup_name= oci bv volume-group update --volume-group-id $pgdetails_member.'member-id' --display-name $new_VolumeGroup_name[0] --profile $configprofile | ConvertFrom-Json
            $updated_VolumeGroup_Details+= New-Object -TypeName psobject -Property @{
                "Old Instance Name" = $Volume_Group_details.data.'display-name'
                "New instance Name" = $updated_VolumeGroup_name.data.'display-name'
                "Instance OCID"     = $updated_VolumeGroup_name.data.id
                "Protection Group ID"=$pgdetails.id
            }
        }
        else {
            write-host "Not renaming the Volume Group : $($Volume_Group_details.data.'display-name')"
        }
        # to update individual 
        if($($Volume_Group_details.data.'volume-ids'.Length) -gt 0)
        {
            foreach ($Volume_id in $Volume_Group_details.data.'volume-ids') {
                if ($Volume_id -match "ocid1.bootvolume*" ) 
                {
                    $bootvolume_details = oci bv boot-volume get --boot-volume-id $Volume_id --profile $configprofile | ConvertFrom-Json
                    Write-Host "        Working on oci_id: '$($bootvolume_details.data.id)'  Name: '$($bootvolume_details.data.'display-name')'..."
                    if ($($bootvolume_details.data.'display-name') -match "_DR_")
                    {
                        $new_bootVolume_name= $bootvolume_details.data.'display-name' -split("_DR_")
                        Write-Host ("       Updating the Display-name from '$($bootvolume_details.data.'display-name')' to '$($new_bootVolume_name[0])'") -ForegroundColor Yellow
                        $updated_bootVolume_name= oci bv boot-volume update --boot-volume-id $Volume_id --display-name $new_bootVolume_name[0] --profile $configprofile | ConvertFrom-Json

                        $updated_bootVolume_Details+= New-Object -TypeName psobject -Property @{
                            "Old boot volume Name" = $bootvolume_details.data.'display-name'
                            "New boot volume Name" = $updated_bootVolume_name.data.'display-name'
                            "boot volume OCID"     = $updated_bootVolume_name.data.id
                            "Protection Group ID"=$pgdetails.id
                        }
                    }
                    else {
                        write-host "        Not renaming the Boot Volume : $($bootvolume_details.data.'display-name')"
                    } 
                }
                elseif ($Volume_id -match "ocid1.volume*" ) {
                    $blockvolume_details = oci bv volume get --volume-id $Volume_id --profile $configprofile | ConvertFrom-Json
                    Write-Host "        Working on oci_id: '$($blockvolume_details.data.id)'  Name: '$($blockvolume_details.data.'display-name')'..." 
                    if ($($blockvolume_details.data.'display-name') -match "_DR_")
                    {
                        $new_blockVolume_name= $blockvolume_details.data.'display-name' -split("_DR_")
                        Write-Host ("       Updating the Display-name from '$($blockvolume_details.data.'display-name')' to '$($new_blockVolume_name[0])'") -ForegroundColor Yellow
                        $updated_blockVolume_name= oci bv volume update --volume-id $Volume_id --display-name $new_blockVolume_name[0] --profile $configprofile | ConvertFrom-Json

                        $updated_blockVolume_Details+= New-Object -TypeName psobject -Property @{
                            "Old boot volume Name" = $blockvolume_details.data.'display-name'
                            "New boot volume Name" = $updated_blockVolume_name.data.'display-name'
                            "boot volume OCID"     = $updated_blockVolume_name.data.id
                            "Protection Group ID"=$pgdetails.id
                        }
                    }
                    else {
                        write-host "        Not renaming the Block Volume : $($blockvolume_details.data.'display-name')"
                    }
                }
            }
        }
    }
}
}
