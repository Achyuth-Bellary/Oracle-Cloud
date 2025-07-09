[CmdletBinding()] 
param (

    [Parameter(Mandatory = $True)]
        [ValidateSet("reg-ash", "reg-phx")]
        [string]$configprofile
)
$compartment = "ocid1.compartment.oc1..aaaaaaaagxzb3ytnmfjiggqby36pjhrcn35kcp5eea4xlgtwv26e73b7rl2q"
$pg_list=$(oci disaster-recovery dr-protection-group list --compartment-id $compartment --profile $configprofile | ConvertFrom-Json).data.items.ID
$ips=@()
foreach ($pg in $pg_list) {
    $pg_oci_id = $pg
    $pgdetails = (oci disaster-recovery dr-protection-group get --dr-protection-group-id $pg_oci_id --profile $configprofile | ConvertFrom-Json).data
    foreach ($pgdetails_member in $pgdetails.members) {
        if ($pgdetails_member."member-type" -eq "COMPUTE_INSTANCE") {
            $private_ips = (oci compute instance list-vnics --instance-id $pgdetails_member."member-id" --profile $configprofile | ConvertFrom-Json).data."private-ip"
            $ips += $private_ips
        }
    }
}
Write-Output $ips