Artifactory Unused Artifacts Cleanup User Plugin
================================================
To apply the plugin to a Artifactory HA cluster for the first time , copy the cleanUnusedArtifacts.groovy to ${ARTIFACTORY_HOME}/etc/artifactory/plugins folder any node and restart the Artifactory service on that node. Now you will see the plugin appear on ${ARTIFACTORY_HOME}/etc/artifactory/plugins folder on all other nodes in the HA cluster. Then do a rolling restart of the Artifactory service on all other nodes.

After the plugin is activated in the Artifactory cluster ,  any new changes to the plugin can be applied by just invoking the "reload" API

`curl -X POST -n "$URL/artifactory/api/plugins/reload"`


A custom cleanup schedule can be defined for any Repository by setting a  
[Property](https://www.jfrog.com/confluence/display/RTF/Properties)
`cleanup.schedule` on the repository. It has the following json format:

```
'{"timeUnit": "day", "timeInterval": 30, "dryRun": false, "paceTimeMS": 0, "disablePropertiesSupport": false}'
```

If a `cleanup.schedule` property is not set for a repository , the default cleanup schedule is:
```
'{"timeUnit": "day", "timeInterval": 30, "dryRun": false, "paceTimeMS": 0, "disablePropertiesSupport": false}'
```

- `timeUnit`: The unit of the time interval. *year*, *month*, *day*, *hour* ,  *minute* ,  *seconds*  are allowed values. Default *day*.
- `timeInterval`: The time interval to look back before deleting an artifact. Default *30*.
- `dryRun`: If this parameter is passed, artifacts will not actually be deleted. Default *false*.
- `paceTimeMS`: The number of milliseconds to delay between delete operations. Default *0*.
- `disablePropertiesSupport`: Disable the support of Artifactory Properties (see below *Artifactory Properties support* section). Default *false*.

Many delete operations can affect performance due to disk I/O occurring. The  `paceTimeMS` parameter now allows a delay per delete operation. 

Following Artifactory  Property is supported if defined on *artifacts* or *folders* or on the repository itself:

- `cleanup.skip`: Skip the artifact deletion if property defined on artifact's path ; artifact itself or in a parent folder(s) or on the repository itself.

To ensure logging for this plugin, edit ${ARTIFACTORY_HOME}/etc/logback.xml to add:
```xml
    <logger name="cleanUnusedArtifacts">
        <level value="info"/>
    </logger>
```

This plugin schedules a regular cleanup cron job that runs `once a day` to :

a) Cleanup all docker tags in `Local` repos based on the AQL:
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

b) Cleanup all docker tags in `Federated` repos based on the AQL:
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
[searches](https://releases.jfrog.io/artifactory/oss-releases-local/org/artifactory/artifactory-papi/%5BRELEASE%5D/artifactory-papi-%5BRELEASE%5D-javadoc.jar!/org/artifactory/search/Searches.html).artifactsNotDownloadedSince API

Operation
---------

-  Reload the Plugin.

`curl -X POST -n "$URL/artifactory/api/plugins/reload"`

- Repos needing cleanup

`curl -X GET -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listReposToCleanup"`

- Repos with cleanup.skip = true i.e skip cleanup . 

`curl -X GET -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listReposToSkipCleanup"`


- List repos with cleanup schedules that have artifacts that should not be cleaned up  .

`curl -X GET -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listSkippedPaths"`

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

- Do on-demand cleanup  

`curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanUnusedArtifacts"`

- Stop the cleanup

`curl -X GET -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=stopCleanup"`

- Resume the cleanup.

`curl -X GET -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=resumeCleanup"`
