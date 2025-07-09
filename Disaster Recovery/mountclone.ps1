$availabilitydomain = 'hvtU:US-ASHBURN-AD-2'
$compartmentid = 'ocid1.compartment.oc1..aaaaaaaagxzb3ytnmfjiggqby36pjhrcn35kcp5eea4xlgtwv26e73b7rl2q'
$mounttargetid ='ocid1.mounttarget.oc1.iad.aaaaacvippy7b7cdnfqwillqojxwiotjmfsc2ylefuyqaaaa'
$profile = 'reg-ash'
$mountcommands=@()
$filesystemlist = $(oci fs file-system list --availability-domain $availabilitydomain --compartment-id $compartmentid --all --profile $profile | ConvertFrom-Json).data
foreach ($filesystem in $filesystemlist) {
    write-host "Pulling the list of snapshots for filesystem $($filesystem.'display-name')"
    $snasptid = $(oci fs snapshot list --file-system-id $filesystem.id --all --profile $profile | ConvertFrom-Json).data
    if($snasptid.'snapshot-type' -eq "REPLICATION"){
        write-host "Snapshot display-name: $($snasptid.name)"
        write-host "Creating a clone of the snapshot $($snasptid.name)"
        $clone_fs = $(oci fs file-system create --availability-domain $availabilitydomain --compartment-id $filesystem.'compartment-id' --snapshot-id $snasptid.id --display-name "clone_$($filesystem.'display-name')" --wait-for-state "ACTIVE" --profile $profile | ConvertFrom-Json).data
        Write-Host "Created clone of the filesystem $($filesystem.'display-name') with the name clone_$($filesystem.'display-name') using the snapshot $($snasptid.name)"
        write-host "Creating an export for the clone filesystem"
        $mounttargetexportid = $(oci fs mount-target get --mount-target-id $mounttargetid --profile $profile | ConvertFrom-Json).data.'export-set-id'
        $($exportcreation |convertfrom-json).data.path = oci fs export create --file-system-id $clone_fs.id --export-set-id $mounttargetexportid --path "/$($filesystem.'display-name')" --export-options '[{"source":"10.197.241.0/24","require-privileged-source-port":"false","access":"Read_Write",“allowedAuth”: [“SYS”],"identity-squash":"None","isAnonymousAccessAllowed":"false"}]' --profile $profile
        Write-Host "Created export for the clone filesystem $($exportcreation.path)"
        Write-Host "Generating mount targets commands for the clone filesystem"
        $mounttargetlist = $(oci fs mount-target get --mount-target-id $mounttargetid --profile $profile | ConvertFrom-Json).data
        $mountcommands += "sudo mount $($(oci network private-ip get --private-ip-id $mounttargetlist.'private-ip-ids' --profile $profile | ConvertFrom-Json).data.'ip-address'):$($($exportcreation |convertfrom-json).data.path) /mnt$($($exportcreation |convertfrom-json).data.path)"
    }
}

$mountcommands | Out-File -FilePath ./mountcommands.txt -Encoding ASCII

