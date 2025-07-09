# Perform initial resource search
$searchQuery = "QUERY all resources where lifeCycleState != 'TERMINATED' && lifeCycleState != 'FAILED'"
$searchLimit = 1000
$searchResult = oci search resource structured-search --query-text $searchQuery --limit $searchLimit

# Save search results to a JSON file
$searchResultFile = "resourceSearchResult.json"
$searchResult | Out-File $searchResultFile

# Retrieve next page ID from search results
$pageIdSearchResult = $searchResult | Select-String "opc-next-page"
[string]$nextPageId = ($pageIdSearchResult -split " ")[3].Replace('"', '')

# Loop through search results pages and save to separate JSON files
$pageCount = 1
while ($nextPageId) {
    $pageFileName = "resourceSearchResult_$pageCount.json"
    $pagedSearchResult = oci search resource structured-search --query-text $searchQuery --limit $searchLimit --page $nextPageId
    $pagedSearchResult | Out-File $pageFileName
    $newPageIdSearchResult = $pagedSearchResult | Select-String "opc-next-page"
    if ($null -ne $newPageIdSearchResult) {
        $nextPageId = ($newPageIdSearchResult -split " ")[3].Replace('"', '')
        $pageCount++
    } else {
        Write-Host "Resource search complete..!"
        break
    }
}

# Retrieve current date for use in CSV file name
$currentDate = Get-Date -Format "MM-dd-yyyy"

# Create CSV file to export resource data
$outputCsvFile = "$((Get-Location).Path)"+"/resources_$currentDate.csv"

# Retrieve list of JSON files to parse for resource data
$jsonFiles = (Get-ChildItem ./resourceSearchResult*.json).Name

# Parse JSON files and extract relevant resource data
$resourceList = @()
foreach ($jsonFile in $jsonFiles) {
    $jsonContent = Get-Content $jsonFile | ConvertFrom-Json
    $totalResources = $jsonContent.data.items.Count
    $progress = 0
    Write-Output "Exporting from $jsonFile..!"
    foreach ($resource in $jsonContent.data.items) {
        $parsedResource = [PSCustomObject]@{
            "compartment-id" = $resource."compartment-id"
            "compartment-name" = $(oci iam compartment get --compartment-id $resource."compartment-id" | ConvertFrom-Json).data.name
            "resource-type" = $resource."resource-type"
            "display-name" = $resource."display-name"
            "lifecycle-state" = $resource."lifecycle-state"
            "identifier" = $resource."identifier"
            "availability-domain" = $resource."availability-domain"
            "IP Address"= If($resource."resource-type" -eq "Instance") {$(oci compute instance list-vnics --instance-id $resource.identifier | ConvertFrom-Json ).data."private-ip" } Else {"NA"}
            "defined-tags" = $resource."defined-tags"
            "Patching-Tag" = $resource."defined-tags"."PATCHING-TAG"
            "enterprise-tag" = $resource."defined-tags"."ENTERPRISE-TAG"
            "freeform-tags" = $resource."freeform-tags"
            "time-created" = $resource."time-created"
        }
        $resourceList += $parsedResource
        $Progress++ 
        $PercentComplete = [math]::Round(($Progress / $totalResources) * 100, 2) 
        $Status = "Exporting to CSV File: $PercentComplete% complete"
        Write-Progress -Activity "Exporting to CSV File" -Status $Status -PercentComplete $PercentComplete
        Start-Sleep -Milliseconds 50
    }
    Write-Progress -Activity "Exporting to CSV File" -Status "Completed" -Completed
}


$resourceList | Export-Csv -Path $outputCsvFile -NoTypeInformation

# Removing the JSON files
(Get-ChildItem ./resource*.json).Name | Remove-Item