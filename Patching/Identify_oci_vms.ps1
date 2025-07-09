#!/bin/pwsh
# Achyuth Naidu 11/09/22
# Script to pull the list of vms.
# To run the script pwsh oci_vm.ps1 -PATCH_VM_TYPE " "
# run file with verbose pwsh oci_vm.ps1 -PATCH_VM_TYPE " " -verbose
<#
This is a PowerShell script that pulls a list of virtual machines (VMs) to be patched on Oracle Cloud Infrastructure (OCI) based on tags, and exports their IP addresses to a YAML file. Here are some comments for the code:
The script is defined as a PowerShell script and is expected to be run with the pwsh command.
The script author and date are mentioned in the comments.
The script is defined to take four parameters: $PATCH_VM_TYPE, $PATCH_TIER, $filepath, and $filepath1.
The script declares several empty arrays ($patch_vms, $vms_not_being_patched, $not_patch_vms, $vms_to_be_patched, $blue_green_vms, and $standalonevms).
The script checks if the $PATCH_TIER parameter is "Dev" or not, and pulls a list of VMs based on the appropriate tags accordingly. If it is "Dev", only the $PATCH_TIER tag is used to fetch the list of VMs. If not, both the $PATCH_TIER and $PATCH_VM_TYPE tags are used.
The script pulls a list of VMs that are not being patched by comparing their $PATCH_VM_TYPE tag to the one specified in the parameter.
The script fetches the IP addresses of the VMs to be patched and exports them to a YAML file specified in the $filepath parameter.
The script also generates verbose output for some of the steps.
Some comments in the script are a bit redundant or don't add much value, while others are missing. It would be good to review and improve the comments to make the code more readable and understandable.
#>
[CmdletBinding()] 
param (

    [Parameter(Mandatory = $false)]
        [ValidateSet("BLUE", "GREEN")]
        [string]$PATCH_VM_TYPE,

    [Parameter(Mandatory = $True)]
        [ValidateSet("DEV", "TEST","PROD")]
        [array]$PATCH_TIER,

    [Parameter(Mandatory = $false)]
        [string]$filepath='./patching_inventory.yml',
    
    [Parameter(Mandatory = $false)]
    [string]$filepath1='./not_patching.csv'
    
)

function get-ipdetails {
    param (
        [Parameter(Mandatory = $True)]
        [System.Array]$vmdetails
    )
    $patching_vmsdetails=@()
    Write-Host "Fetching Ip-deatils deatils.." -ForegroundColor $PATCH_VM_TYPE -ErrorAction SilentlyContinue
    Write-Verbose -Message "The total list of VM's is created at $filepath"
    $vmdetails.data.items | ForEach-Object { 
        $patching_vmsdetails += New-Object -TypeName psobject -Property @{
            Name = $_."display-name"
            "IP Address"= $(oci compute instance list-vnics --instance-id $_.identifier |ConvertFrom-Json ).data."private-ip"
          }
          Write-Verbose -Message "fetching IP for $($_."display-name")"
    }
    return $patching_vmsdetails 
}

# declaring empty arrays
$patch_vms=@()
$vms_not_being_patched=@()
$not_patch_vms=@()
$vms_to_be_patched=@()
$blue_green_vms=@()
$standalonevms=@()

if ($PATCH_TIER -eq "Dev") {
    Write-host "Pulling the list of VM's from OCI portal to be patched with tag '$PATCH_TIER '."
    $vms_to_be_patched= oci search resource structured-search --query-text "query instance resources where (definedTags.namespace = 'PATCHING-TAG' &&  definedTags.key = 'PATCH_TIER' && definedTags.value = '$PATCH_TIER') && lifeCycleState = 'running'" | ConvertFrom-Json
    [Collections.Generic.List[String]]$patch_vms +=$($vms_to_be_patched.data.items."display-name")
    Write-Verbose -Message "Total number of VM's to be patched '$PATCH_TIER': '$($patch_vms.Count)'"
    $patching_vmsdetails= get-ipdetails -vmdetails $vms_to_be_patched
}
else {
    Write-host "Pulling the list of VM's from OCI portal to be patched with tag '$PATCH_TIER'."
    $blue_green_vms = oci search resource structured-search --query-text "query instance resources where ((definedTags.namespace = 'PATCHING-TAG' &&  definedTags.key = 'PATCH_TIER' && definedTags.value = '$PATCH_TIER') && (definedTags.namespace = 'PATCHING-TAG' &&  definedTags.key = 'PATCH_VM_TYPE' && definedTags.value = '$PATCH_VM_TYPE'))" | ConvertFrom-Json
    $standalonevms = oci search resource structured-search --query-text "query instance resources where ((definedTags.namespace = 'PATCHING-TAG' &&  definedTags.key = 'PATCH_TIER' && definedTags.value = '$PATCH_TIER') && (definedTags.namespace = 'PATCHING-TAG' &&  definedTags.key != 'PATCH_VM_TYPE')) && lifeCycleState = 'running'" | ConvertFrom-Json
    [Collections.Generic.List[String]]$patch_vms_bg =$($blue_green_vms.data.items."display-name")
    [Collections.Generic.List[String]]$patch_vms_standalone =$($standalonevms.data.items."display-name")
    $patch_vms = $patch_vms_bg + $patch_vms_standalone
    $ips1= get-ipdetails -vmdetails $blue_green_vms
    $ips2= get-ipdetails -vmdetails $standalonevms
    $patching_vmsdetails =$ips1 + $ips2
    Write-Verbose -Message "Total number of VM's to be patched '$PATCH_TIER': '$($patch_vms.Count)'"
}
Write-Output "Exporting IP-Address of the VM's to be patched to  $($PWD.path)/$($($filepath -split "/")[1])."
$patching_vmsdetails."IP Address"  | Out-File $filepath

# Write-Host "============================================================================"
# to remove the powered off Vm's
# $list_of_powered_off_vms= oci search resource structured-search --query-text "query instance resources where ((freeformTags.key = 'vm_type' && freeformTags.value = '$vmtype' )|| (freeformTags.key != 'vm_type')) && lifeCycleState != 'RUNNING'" | ConvertFrom-Json
# if($($list_of_powered_off_vms.data.items."lifecycle-state").count -gt 0){
#     Write-Host "Looks like there are $($($list_of_powered_off_vms.data.items."lifecycle-state").count) VMs in Stopped state. Below are the list" -ForegroundColor Yellow
#     $list_of_powered_off_vms.data.items."display-name"
#     Write-host "============================================================================"
#     $msg = 'Do you want to remove the powered off Vms of the patching list? [Y/N]'
#     do {
#         $response = Read-Host -Prompt $msg
#         if ($response -eq 'y') {
#             Write-Host "Removing the VMs from the patching list."
#             $patch_vms = $patch_vms | Where-Object {$list_of_powered_off_vms.data.items."display-name" -notcontains $_}
#             break;
#         }
#     } until ($response -eq 'n' )
#     {
#     "Please look into the powered off Vm's list and power them on , if the list of servers was intentionally  powered off ,please remove the powered off VM's from 'patching_inventory.yml' file."
#     }
# }
Write-host "============================================================================"

Write-Host "Pulling the list of VM's from OCI portal not being patched" 
$vms_not_being_patched=oci search resource structured-search --query-text "query instance resources where ((definedTags.namespace = 'PATCHING-TAG' &&  definedTags.key = 'PATCH_TIER' && definedTags.value = '$PATCH_TIER') && (definedTags.namespace = 'PATCHING-TAG' &&  definedTags.key = 'PATCH_VM_TYPE' && definedTags.value != '$PATCH_VM_TYPE')) && lifeCycleState = 'running'" | ConvertFrom-Json
$vms_not_being_patched.data.items | ForEach-Object {
$not_patch_vms += New-Object -TypeName psobject -Property @{
    Name = $_.'display-name'
    compartmentid = $_."compartment-id"
    "Compartment Name" =$(oci iam compartment get --compartment-id $_."compartment-id" |ConvertFrom-Json).data.name
    instanceID = $_.identifier
    "Power state"= $_."lifecycle-state"
    "IP Address"= $(oci compute instance list-vnics --instance-id $_.identifier |ConvertFrom-Json ).data."private-ip"
  }
}

Write-Verbose -Message "Total number Vm's not being patched: '$($not_patch_vms.Count)'" 
Write-Output "Exporting VM's not to be patched to  $($PWD.path)/$($($filepath1 -split "/")[1]).."
$not_patch_vms | Export-Csv $filepath1
Write-Host "Exporting Completed."