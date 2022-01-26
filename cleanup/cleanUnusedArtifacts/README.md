Artifactory Unused Artifacts Cleanup User Plugin
================================================

Artifactory Properties support
----------
Default cleanup schedule that can be defined for any Repository has the following json format:
```
'{"timeUnit": "day", "timeInterval": 30, "dryRun": false, "paceTimeMS": 0, "disablePropertiesSupport": false}'
```
- `timeUnit`: The unit of the time interval. *year*, *month*, *day*, *hour* ,  *minute* ,  *seconds*  are allowed values. Default *day*.
- `timeInterval`: The time interval to look back before deleting an artifact. Default *30*.
- `dryRun`: If this parameter is passed, artifacts will not actually be deleted. Default *false*.
- `paceTimeMS`: The number of milliseconds to delay between delete operations. Default *0*.
- `disablePropertiesSupport`: Disable the support of Artifactory Properties (see below *Artifactory Properties support* section). Default *false*.

Some Artifactory [Properties](https://www.jfrog.com/confluence/display/RTF/Properties) are supported if defined on *artifacts* or *folders* or on the repository itself:

- `cleanup.skip`: Skip the artifact deletion if property defined on artifact's path ; artifact itself or in a parent folder(s) or on the repository itself.



This plugin can  schedule a regular cleanup cron job that runs once a day to :

a) Cleanup all docker tags in Local repos based on the AQL:
```
"items.find":{
     "name": {
        "$eq": "manifest.json"
    },
    "$or":[
        {
            "$and": [
                { "stat.downloads": { "$eq":null } },
                { "updated": { "$before": "30d" } }
            ]
        },
        {
            "$and": [
                { "stat.downloads": { "$gt": 0 } },
                { "stat.downloaded": { "$before": "30d" } }
            ]
        }
    ]
}).include("repo", "name", "path", "updated", "created", "sha256", "stat")
```

b) Cleanup all docker tags in Federated repos based on the AQL:
```
items.find({
                     "name": {
                        "$eq": "manifest.json"
                    },
                    "$or":[
                        {
                            "$and": [
                                { "stat.downloads": { "$eq":null } },
                                { "updated": { "$before": "30d" } }
                            ]
                        },
                        {
                            "$and": [
                                { "stat.downloads": { "$gt": 0 } },
                                { "updated": { "$before": "30d" } }
                            ]
                        }
                    ]
                }).include("repo", "name", "path", "updated", "created", "sha256", "stat")
```
c) For all non Docker repos the cleanup is done based on 
searches.artifactsNotDownloadedSince API

1. Reload the Plugin.
curl -X POST -n "$URL/artifactory/api/plugins/reload"

2. Repos needing cleanup
curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listReposToCleanup"

3. Repos with cleanup.skip = true i.e skip cleanup . 

curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listReposToSkipCleanup"


4. List repos with cleanup schedules that have artifacts that should not be cleaned up  .

curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listSkippedPaths"

Example output:
```
{
    "paths_skipped_cleanup": [
        {
            "docker": "[jfrog/sample_docker2-10/]"
        },
        {
            "maven-fed-local": "[org/jfrog/test/multi1/, org/jfrog/test/multi2/]"
        },
        {
            "mvn-fed-local": "[org/jfrog/test/multi1/1.2-SNAPSHOT/]"
        }
    ]
}
```

5. Do the cleanup  

curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanUnusedArtifacts"

6. You can stop the cleanup

curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=stopCleanup"

7. You can resume the cleanup.
curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=resumeCleanup"
