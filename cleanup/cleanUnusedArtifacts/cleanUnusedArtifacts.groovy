/*
 * Copyright (C) 2014 JFrog Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.artifactory.fs.ItemInfo

import org.artifactory.repo.RepoPath
import org.artifactory.api.repo.exception.ItemNotFoundRuntimeException
import org.artifactory.repo.RepoPathFactory
import org.artifactory.search.aql.AqlResult
import groovy.json.JsonSlurper
import org.artifactory.exception.CancelException

import groovy.time.TimeCategory
import groovy.time.TimeDuration
import groovy.transform.Field

import java.text.SimpleDateFormat
import org.apache.commons.lang3.StringUtils

import groovy.json.JsonOutput
import groovy.json.JsonBuilder


//@Field final def DEFAULT_SCHEDULE_JSON = new JsonSlurper().parseText('{"timeUnit": "day", "timeInterval": 30, "dryRun": true, "paceTimeMS": 500, "disablePropertiesSupport": false}')
@Field final def DEFAULT_SCHEDULE_TEXT = '{"timeUnit": "minute", "timeInterval": 1, "dryRun": true, "paceTimeMS": 500, "disablePropertiesSupport": false}'
@Field final def DEFAULT_SCHEDULE_JSON = new JsonSlurper().parseText(DEFAULT_SCHEDULE_TEXT)

class Global {
    static Boolean stopCleaning = false
    //static Boolean pauseCleaning = false
    //static int paceTimeMS = 0
}

def pluginGroup = 'cleaners'

executions {

    //curl -X POST -n "$URL/artifactory/api/plugins/reload"
    //curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanUnusedArtifacts"
    cleanUnusedArtifacts(groups: [pluginGroup]) {
        cleanupAllLocalandFederatedRepos()

    }
    
 
    //curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=stopCleanup"
    //curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=resumeCleanup"
   //curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listReposToCleanup"
    //curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listReposToSkipCleanup"
    //curl -X POST -n  "$URL/artifactory/api/plugins/execute/cleanupPolicies?params=command=listSkippedPaths"

    cleanupPolicies(groups: [pluginGroup]) { params ->
        def command = params['command'] ? params['command'][0] as String : ''

        switch ( command ) {
            case "stopCleanup":
                Global.stopCleaning = true
                log.info "Stop Cleanup request detected"
                break
            case "resumeCleanup":
                Global.stopCleaning = false
                log.info "Resume Cleanup request detected"
                break
            case "listReposToCleanup":
                    log.info "listReposToCleanup request detected"
 
                    message = getPolicyForReposWithCleanup()
                    status = 200
                    break
           case "listReposToSkipCleanup":
                    log.info "listReposToSkipCleanup request detected"
 
                    message = getPolicyForReposToSkipCleanup()
                    status = 200
                    break
            case "listSkippedPaths":
                    log.info "listSkippedPaths request detected"
                    message =  getSkippedPathsInAllCleanupRepos()
                    status = 200
                    break

            default:
                log.info "Missing or invalid command, '$command'"
          
        }


    }
}

/*==================
// create cron job on Plugin  startup

jobs {
    "scheduled_from_cleanUnusedArtifacts"(cron: "0/30 * * * * ?") {
        log.info "Creating  cron cleanup Job"

        cleanupAllLocalandFederatedRepos()

    }
}   
======================*/

private String getSkippedPathsInAllCleanupRepos()
{
            List<String> local_repos = getLocalReposNeedingCleanup()
            List<String> federated_repos = getFederatedReposNeedingCleanup()

            def jsonSlurper = new JsonSlurper()

            def allrepos = []
            def builder  = new JsonBuilder()
            local_repos.each { repoKey ->
                                
                                def scheduleMap = [:]
                                scheduleMap = getCleanupScheduleForRepo(repoKey)

                                if (!scheduleMap.disablePropertiesSupport){
                                    def skipped_paths = [:]
                                    skipped_paths = getSkippedPaths(repoKey)
                                    if(skipped_paths) {// There are some artifacts in the repo with "cleanup.skip" set to true  as "skipped_paths" is not empty
                                        //log.info "test1 ${JsonOutput.toJson(skipped_paths[repoKey])}" -> Output:   ["org/jfrog/test/multi1/","org/jfrog/test/multi2/"]
                                        //log.info "test2 ${skipped_paths[repoKey].toString()}" --> Output : [org/jfrog/test/multi1/, org/jfrog/test/multi2/]
                                        //log.info "test4 ${jsonSlurper.parseText(JsonOutput.toJson(skipped_paths[repoKey]))}" -> Output : [org/jfrog/test/multi1/, org/jfrog/test/multi2/]
                                        
                                         builder {
                                            "$repoKey"  skipped_paths[repoKey].toString()
                                         }
                                         allrepos.add(builder.toString())
                                    }
                                }
                            
            }

            federated_repos.each { repoKey ->
                               
                                def scheduleMap = [:]
                                scheduleMap = getCleanupScheduleForRepo(repoKey)

                                if (!scheduleMap.disablePropertiesSupport){
                                    def skipped_paths = [:]
                                    skipped_paths = getSkippedPaths(repoKey)
                                    if(skipped_paths) {// There are some artifacts in the repo with "cleanup.skip" set to true  as "skipped_paths" is not empty
                                        
                                         builder {
                                            "$repoKey"  skipped_paths[repoKey].toString()
                                         }
                                         allrepos.add(builder.toString())
                                    }
                                }
            }

             builder {
                paths_skipped_cleanup  jsonSlurper.parseText(allrepos.toString())
            }

            builder.toPrettyString()


}

private def cleanupAllLocalandFederatedRepos()
{
        def timeStart = new Date()
        log.info "=========================Starting  cleanupAllLocalandFederatedRepos"

        // The aql for the cleanup of Docker repos for  local and Federated differ
        cleanupLocalDockerRepos() 
        cleanupFederatedDockerRepos()

        // For the non-docker repos I just use the  "searches.artifactsNotDownloadedSince" API.
        cleanupNonDockerRepos(getLocalNonDockerReposNeedingCleanup()) 
        cleanupNonDockerRepos(getFederatedNonDockerReposNeedingCleanup()) 
        //cleanupFederatedNonDockerRepos()
        
        def timeStop = new Date()
        TimeDuration duration = TimeCategory.minus(timeStop, timeStart)
        log.info "========================= Total Cleanup Elapsed time for all repos : " + duration

}

// Returns a json list if repos with "cleanup.skip" set to true
private String getPolicyForReposToSkipCleanup()
{

            List<String> local_repos = getLocalReposToSkipCleanup()
            List<String> federated_repos = getFederatedReposToSkipCleanup()
            
            def jsonSlurper = new JsonSlurper()

            def allrepos = []
            def builder  = new JsonBuilder()
            local_repos.each { repoKey ->
                                builder {
                                    "$repoKey" "cleanup.skip"
                                }
                            allrepos.add(builder.toString())
            }

            federated_repos.each { repoKey ->
                                builder {
                                    "$repoKey" "cleanup.skip"
                                }
                            allrepos.add(builder.toString())
            }

            builder {
                policies  jsonSlurper.parseText(allrepos.toString())
            }

            builder.toPrettyString()
}


// Returns a json list if repos with custom or default cleanup policy 
private String getPolicyForReposWithCleanup()
{
            List<String> local_repos = getLocalReposNeedingCleanup()
            List<String> federated_repos = getFederatedReposNeedingCleanup()
            
            def jsonSlurper = new JsonSlurper()

            def allrepos = []
            def builder  = new JsonBuilder()
            local_repos.each { repoKey ->
                                builder {
                                    "$repoKey" jsonSlurper.parseText(repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.schedule")?:DEFAULT_SCHEDULE_TEXT)
                                }
                            allrepos.add(builder.toString())
            }

            federated_repos.each { repoKey ->
                                builder {
                                    "$repoKey" jsonSlurper.parseText(repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.schedule")?:DEFAULT_SCHEDULE_TEXT)
                                }
                            allrepos.add(builder.toString())
            }

            builder {
                policies  jsonSlurper.parseText(allrepos.toString())
            }

            builder.toPrettyString()

}

//Returns a List of Local  repos  to skip cleanup i.e  have no "cleanup.skip" or  "cleanup.skip" is  "true"
private List<String> getLocalReposToSkipCleanup() {
    List<String> localRepoKeys = repositories.getLocalRepositories()
    

    localRepoKeys.findAll { String repoKey ->
        repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.skip")?.equalsIgnoreCase("true")          
    }

}

//Returns a List of Federated  repos  to skip cleanup i.e  have no "cleanup.skip" or  "cleanup.skip" is  "true"

private List<String> getFederatedReposToSkipCleanup() {
    List<String> federatedRepoKeys = repositories.getFederatedRepositories()
    

    federatedRepoKeys.findAll { String repoKey ->
        repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.skip")?.equalsIgnoreCase("true")          
    }


}

//Returns a List of Local  repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
private List<String> getLocalReposNeedingCleanup() {
    List<String> localRepoKeys = repositories.getLocalRepositories()
    

    localRepoKeys.findAll { String repoKey ->
        !repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.skip")?.equalsIgnoreCase("true")          
    }

}



//Returns a List of Federated  repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
private List<String> getFederatedReposNeedingCleanup() {
    List<String> federatedRepoKeys = repositories.getFederatedRepositories()
    

    federatedRepoKeys.findAll { String repoKey ->
        !repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.skip")?.equalsIgnoreCase("true")          
    }


}


//Returns a List of Local Docker repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
private List<String> getLocalDockerReposNeedingCleanup() {
    List<String> localRepoKeys = repositories.getLocalRepositories()

    localRepoKeys.findAll { String repoKey ->
        if(repositories.getRepositoryConfiguration(repoKey)?.isEnableDockerSupport()){
            !repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.skip")?.equalsIgnoreCase("true")
               

        }
          
    }
}

//Returns a List of  Federated Docker repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
private List<String> getFederatedDockerReposNeedingCleanup() {
    List<String> federatedRepoKeys = repositories.getFederatedRepositories()

    federatedRepoKeys.findAll { String repoKey ->
        if(repositories.getRepositoryConfiguration(repoKey)?.isEnableDockerSupport()){
            !repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.skip")?.equalsIgnoreCase("true")
               

        }
          
    }
}

//Returns a List of Local Non-Docker repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
private List<String> getLocalNonDockerReposNeedingCleanup() {
    List<String> localRepoKeys = repositories.getLocalRepositories()

    localRepoKeys.findAll { String repoKey ->
        if(!repositories.getRepositoryConfiguration(repoKey)?.isEnableDockerSupport()){
            !repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.skip")?.equalsIgnoreCase("true")
               

        }
          
    }
}

//Returns a List of  Non-Docker Federated repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
private List<String> getFederatedNonDockerReposNeedingCleanup() {
    List<String> federatedRepoKeys = repositories.getFederatedRepositories()

    federatedRepoKeys.findAll { String repoKey ->
        if(!repositories.getRepositoryConfiguration(repoKey)?.isEnableDockerSupport()){
            !repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.skip")?.equalsIgnoreCase("true")
               

        }
          
    }
}

// return the repo Cleanup Schedule as a map
private def getCleanupScheduleForRepo(String repoKey)
{
                def jsonSlurper = new JsonSlurper()
                def scheduleJson = jsonSlurper.parseText(repositories.getProperty(RepoPathFactory.create(repoKey),"cleanup.schedule")?:DEFAULT_SCHEDULE_TEXT)?: DEFAULT_SCHEDULE_JSON
                assert scheduleJson instanceof Map

                def scheduleMap = [:]

                // Here  I am using the aql Relative Time Operators as the "timeUnit". Default cleanup Schedule is 30 days
                scheduleMap.timeUnit = scheduleJson.containsKey("timeUnit") ? scheduleJson.timeUnit as String : DEFAULT_SCHEDULE_JSON.timeUnit  as String
                scheduleMap.timeInterval = scheduleJson.containsKey("timeInterval") ? scheduleJson.timeInterval as int : DEFAULT_SCHEDULE_JSON.timeInterval as int

                scheduleMap.paceTimeMS = scheduleJson.containsKey("paceTimeMS") ? scheduleJson.paceTimeMS as int : DEFAULT_SCHEDULE_JSON.paceTimeMS as int
                scheduleMap.dryRun = scheduleJson.containsKey("dryRun") ? new Boolean(scheduleJson.dryRun) : new Boolean(DEFAULT_SCHEDULE_JSON.dryRun)
                scheduleMap.disablePropertiesSupport = scheduleJson.containsKey("disablePropertiesSupport") ? new Boolean(scheduleJson.disablePropertiesSupport) :  new Boolean(DEFAULT_SCHEDULE_JSON.disablePropertiesSupport)


                //String cleanupInterval = "${scheduleJson.timeInterval}${scheduleJson.timeUnit}"
                scheduleMap.cleanupInterval = mapTimeUnitToAql(scheduleMap.timeUnit, String.valueOf(scheduleMap.timeInterval))
                log.info "cleanupInterval for this repo is --> ${scheduleMap.cleanupInterval}"

                return scheduleMap

}

private boolean cleanupLocalDockerRepos() {

        //  cleanup Docker local repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
        List<String> localDockerRepoKeys = getLocalDockerReposNeedingCleanup()
        log.info "==============Fetched list of docker  repos that need cleanup"

        //loop through each repo key
            localDockerRepoKeys.find { String repoKey ->

                log.info "Looking for  repos in the repo $repoKey"
                
                def scheduleMap = [:]
                scheduleMap = getCleanupScheduleForRepo(repoKey)

                // Done:  if this  repo should not be cleaned return false i.e continue to next "repoKey" -> This is already done in getLocalDockerReposNeedingCleanup()
                
                // else this repo has to be cleaned as below:
                // If tag was never downloaded and  "Last Modified" i.e "updated" is more than cleanupInterval  then delete it.
                // Also if tag was downloaded atleast once but was not downloaded for  more than cleanupInterval  then delete it.
               
                def aql = "items.find({ \"repo\": { \"\$eq\":\"${repoKey}\" }, \"name\": { \"\$eq\": \"manifest.json\" }, \"\$or\":[ { \"\$and\": [ { \"stat.downloads\": { \"\$eq\":null } }, { \"updated\": { \"\$before\": \"${scheduleMap.cleanupInterval}\" } } ] }, { \"\$and\": [ { \"stat.downloads\": { \"\$gt\": 0 } }, { \"stat.downloaded\": { \"\$before\": \"${scheduleMap.cleanupInterval}\" } } ] } ] }).include(\"repo\", \"name\", \"path\", \"updated\", \"sha256\", \"stat.downloads\", \"stat.downloaded\")"

               docker_artifactsCleanup(  scheduleMap.timeUnit,  scheduleMap.timeInterval,  aql,  repoKey, scheduleMap.paceTimeMS, scheduleMap.dryRun , scheduleMap.disablePropertiesSupport)

 
            }
            return true


} 

private def docker_artifactsCleanup( String timeUnit, int timeInterval, String aql,  String repokey, paceTimeMS, dryRun = true, disablePropertiesSupport = false) {
    log.info "Starting artifact cleanup for repository $repokey, until $timeInterval ${timeUnit}s ago with pacing interval $paceTimeMS ms, dryrun: $dryRun, disablePropertiesSupport: $disablePropertiesSupport"
    // Create Map(repo, paths) of skiped paths (or others properties supported in future ...)
    def skip = [:]
    if ( ! disablePropertiesSupport && repokey ){
        skip = getSkippedPaths(repokey)
    }

    def calendarUntil = Calendar.getInstance()

    calendarUntil.add(mapTimeUnitToCalendar(timeUnit), -timeInterval)

    def calendarUntilFormatted = new SimpleDateFormat("yyyy/MM/dd HH:mm").format(calendarUntil.getTime());
    log.info "Removing all artifacts not downloaded since $calendarUntilFormatted"

    int cntFoundArtifacts = 0
    int cntNoDeletePermissions = 0
    //long bytesFound = 0
    //long bytesFoundWithNoDeletePermission = 0

  
                  searches.aql(aql.toString()) { AqlResult artifactsCleanedUp ->
                    artifactsCleanedUp.each { dockertag ->

                            def DockerTag_in_repo = "$dockertag.repo/$dockertag.path"
                            RepoPath dockerTag_repopath = RepoPathFactory.create("$DockerTag_in_repo")

                        try {


                                if ( Global.stopCleaning ) {
                                    log.info "Stopping by request, ending loop"
                                    return true
                                }

                                if ( ! disablePropertiesSupport && skip[ repokey ] && StringUtils.startsWithAny(dockertag.path, skip[ repokey ])){
                                    if (log.isDebugEnabled()){
                                        log.debug "Skip $DockerTag_in_repo"
                                    }
                                    return false
                                }

                                log.info "Testing Deleting tag $DockerTag_in_repo"

                                if(repositories.getItemInfo(dockerTag_repopath)?.isFolder()) { // Docker tag is always a folder

                                    //bytesFound += repositories.getItemInfo(dockerTag_repopath)?.getSize()  --> commented because .getSize()  not available for folder
                                    cntFoundArtifacts++
                                    if (!security.canDelete(dockerTag_repopath)) {
                                        //bytesFoundWithNoDeletePermission += repositories.getItemInfo(dockerTag_repopath)?.getSize()
                                        cntNoDeletePermissions++
                                    }
                                }
                                if (dryRun) {
                                   // log.info "Found $dockerTag_repopath, $cntFoundArtifacts/$artifactsCleanedUp.size , total $bytesFound bytes"
                                   log.info "Found $dockerTag_repopath, $cntFoundArtifacts/$artifactsCleanedUp.total"
                                    log.info "\t==> currentUser: ${security.currentUser().getUsername()}"
                                    log.info "\t==> canDelete: ${security.canDelete(dockerTag_repopath)}"

                                } else {
                                    if (security.canDelete(dockerTag_repopath)) {
                                        //log.info "Deleting tag $DockerTag_in_repo, $cntFoundArtifacts/$artifactsCleanedUp.size , total $bytesFound bytes"
                                        log.info "Deleting tag $DockerTag_in_repo, $cntFoundArtifacts/$artifactsCleanedUp.total"
                                        repositories.delete dockerTag_repopath
                                    } else {
                                        log.info "Can't delete $DockerTag_in_repo (user ${security.currentUser().getUsername()} has no delete permissions), $cntFoundArtifacts/$artifactsCleanedUp.total" //+
                                                //"$cntFoundArtifacts/$artifactsCleanedUp.size , total $bytesFound bytes"
                                    }
                                }


                        }
                        catch (ItemNotFoundRuntimeException ex) {
                                log.info "Failed to find $DockerTag_in_repo, skipping deletion"
                        }


                        if (paceTimeMS > 0) {
                            sleep( paceTimeMS )
                        }

                        return false

                    }
               }

                   if (dryRun) {
                        //log.info "Dry run - nothing deleted. Found $cntFoundArtifacts artifacts consuming $bytesFound bytes"
                        log.info "Dry run - nothing deleted. Found $cntFoundArtifacts artifacts"
                        if (cntNoDeletePermissions > 0) {
                            //log.info "$cntNoDeletePermissions artifacts cannot be deleted due to lack of permissions ($bytesFoundWithNoDeletePermission bytes)"
                            log.info "$cntNoDeletePermissions artifacts cannot be deleted due to lack of permissions"
                        }
                    } else {
                        //log.info "Finished cleanup, deleting $cntFoundArtifacts artifacts that took up $bytesFound bytes"
                        log.info "Finished cleanup, deleting $cntFoundArtifacts artifacts"
                        if (cntNoDeletePermissions > 0) {
                            //log.info "$cntNoDeletePermissions artifacts could not be deleted due to lack of permissions ($bytesFoundWithNoDeletePermission bytes)"
                            log.info "$cntNoDeletePermissions artifacts could not be deleted due to lack of permissions"
                        }
                    }

}


private boolean cleanupFederatedDockerRepos() {
       //  cleanup Federated Docker repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
        List<String> federatedDockerRepoKeys = getFederatedDockerReposNeedingCleanup()
        log.info "==============Fetched list of Federated docker repos that need cleanup"
        //loop through each repo key
        federatedDockerRepoKeys.find { String repoKey ->
                log.info "Looking for  repos in the Federated repo $repoKey"

                def scheduleMap = [:]
                scheduleMap = getCleanupScheduleForRepo(repoKey)

                def aql = "items.find({ \"repo\": { \"\$eq\":\"${repoKey}\" }, \"name\": { \"\$eq\": \"manifest.json\" }, \"\$or\":[ { \"\$and\": [ { \"stat.downloads\": { \"\$eq\":null } }, { \"updated\": { \"\$before\": \"${scheduleMap.cleanupInterval}\" } } ] }, { \"\$and\": [ { \"stat.downloads\": { \"\$gt\": 0 } }, { \"updated\": { \"\$before\": \"${scheduleMap.cleanupInterval}\" } } ] } ] }).include(\"repo\", \"name\", \"path\", \"updated\", \"sha256\", \"stat.downloads\", \"stat.downloaded\")"

               docker_artifactsCleanup(  scheduleMap.timeUnit,  scheduleMap.timeInterval,  aql,  repoKey, scheduleMap.paceTimeMS, scheduleMap.dryRun , scheduleMap.disablePropertiesSupport)
        }
        return true

}


private boolean cleanupNonDockerRepos(List<String> NonDockerRepoKeys ) {
       //  cleanup NOn-Docker local repos that need cleanup i.e  Have no "cleanup.skip" or  "cleanup.skip" is not "true"
        //List<String> localNonDockerRepoKeys = 
        log.info "==============Fetched list of Non-docker  repos that need cleanup"
                //loop through each repo key
        NonDockerRepoKeys.find { String repoKey ->
                log.info "Looking for Non-Docker repos in the repo $repoKey"

                def scheduleMap = [:]
                scheduleMap = getCleanupScheduleForRepo(repoKey)
  
                non_docker_artifactCleanup( scheduleMap.timeUnit,  scheduleMap.timeInterval,   repoKey, scheduleMap.paceTimeMS, scheduleMap.dryRun , scheduleMap.disablePropertiesSupport )
        }
        return true

}


private def non_docker_artifactCleanup(String timeUnit, int timeInterval,  String repokey, paceTimeMS, dryRun = true, disablePropertiesSupport = false) {
    log.info "Starting artifact cleanup for repository $repokey, until $timeInterval ${timeUnit}s ago with pacing interval $paceTimeMS ms, dryrun: $dryRun, disablePropertiesSupport: $disablePropertiesSupport"

    // Create Map(repo, paths) of skiped paths (or others properties supported in future ...)
    def skip = [:]
    if ( ! disablePropertiesSupport && repokey){
        skip = getSkippedPaths(repokey)
    }

    def calendarUntil = Calendar.getInstance()

    calendarUntil.add(mapTimeUnitToCalendar(timeUnit), -timeInterval)

    def calendarUntilFormatted = new SimpleDateFormat("yyyy/MM/dd HH:mm").format(calendarUntil.getTime());
    log.info "Removing all artifacts not downloaded since $calendarUntilFormatted"

    int cntFoundArtifacts = 0
    int cntNoDeletePermissions = 0
    long bytesFound = 0
    long bytesFoundWithNoDeletePermission = 0
    def artifactsCleanedUp = searches.artifactsNotDownloadedSince(calendarUntil, calendarUntil, repokey  )
    artifactsCleanedUp.find {
        try {


            if ( Global.stopCleaning ) {
                log.info "Stopping by request, ending loop"
                return true
            }

            if ( ! disablePropertiesSupport && skip[ repokey ] && StringUtils.startsWithAny(it.path, skip[ repokey ])){
                if (log.isDebugEnabled()){
                    log.debug "Skip $it"
                }
                return false
            }

            bytesFound += repositories.getItemInfo(it)?.getSize()
            cntFoundArtifacts++
            if (!security.canDelete(it)) {
                bytesFoundWithNoDeletePermission += repositories.getItemInfo(it)?.getSize()
                cntNoDeletePermissions++
            }
            if (dryRun) {
                log.info "Found $it, $cntFoundArtifacts/$artifactsCleanedUp.size total $bytesFound bytes"
                log.info "\t==> currentUser: ${security.currentUser().getUsername()}"
                log.info "\t==> canDelete: ${security.canDelete(it)}"

            } else {
                if (security.canDelete(it)) {
                    log.info "Deleting $it, $cntFoundArtifacts/$artifactsCleanedUp.size total $bytesFound bytes"
                    repositories.delete it
                } else {
                    log.info "Can't delete $it (user ${security.currentUser().getUsername()} has no delete permissions), " +
                            "$cntFoundArtifacts/$artifactsCleanedUp.size total $bytesFound bytes"
                }
            }
        } catch (ItemNotFoundRuntimeException ex) {
            log.info "Failed to find $it, skipping"
        }

        if (paceTimeMS > 0) {
            sleep( paceTimeMS )
        }

        return false
    }

    if (dryRun) {
        log.info "Dry run - nothing deleted. Found $cntFoundArtifacts artifacts consuming $bytesFound bytes"
        if (cntNoDeletePermissions > 0) {
            log.info "$cntNoDeletePermissions artifacts cannot be deleted due to lack of permissions ($bytesFoundWithNoDeletePermission bytes)"
        }
    } else {
        log.info "Finished cleanup, deleting $cntFoundArtifacts artifacts that took up $bytesFound bytes"
        if (cntNoDeletePermissions > 0) {
            log.info "$cntNoDeletePermissions artifacts could not be deleted due to lack of permissions ($bytesFoundWithNoDeletePermission bytes)"
        }
    }
}

private def getSkippedPaths(String repoKey) {
    def timeStart = new Date()
    def skip = [:]
   // for (String repoKey : repos){
        def pathsTmp = []
        def aql = "items.find({\"repo\":\"" + repoKey + "\",\"type\": \"any\",\"@cleanup.skip\":\"true\"}).include(\"repo\", \"path\", \"name\", \"type\")"
        searches.aql(aql.toString()) {
            for (item in it) {
                def path = item.path + '/' + item.name
                // Root path case behavior
                if ('.' == item.path){
                    path = item.name
                }
                if ('folder' == item.type){
                    path += '/'
                }
                if (log.isTraceEnabled()){
                    log.trace "skip found for " + repoKey + ":" + path
                }
                pathsTmp.add(path)
            }
        }

        // Simplify list to have only parent paths
        def paths = []
        for (path in pathsTmp.sort{ it }) {
            if (paths.size == 0 || ! path.startsWith(paths[-1])) {
                if (log.isTraceEnabled()){
                    log.trace "skip added for " + repoKey + ":" + path
                }
                paths.add(path)
            }
        }

        if (paths.size > 0){
            skip[repoKey] = paths.toArray(new String[paths.size])
        }
    //}
    def timeStop = new Date()
    TimeDuration duration = TimeCategory.minus(timeStop, timeStart)
    log.info "Elapsed time to retrieve paths to skip: " + duration
    return skip
}

private def mapTimeUnitToCalendar (String timeUnit) {
    switch ( timeUnit ) {
        case "minute":
            return Calendar.MINUTE
        case "hour":
            return Calendar.HOUR
        case "day":
            return Calendar.DAY_OF_YEAR
        case "month":
            return Calendar.MONTH
        case "year":
            return Calendar.YEAR
        default:
            def errorMessage = "$timeUnit is no valid time unit. Please check your request or scheduled policy."
            log.error errorMessage
            throw new CancelException(errorMessage, 400)
    }
}

private String mapTimeUnitToAql(String timeUnit , String timeInterval) {
    assert timeInterval.isInteger()

    switch ( timeUnit ) {

        case "seconds":
        case "s":
            return "${timeInterval}s"

        case "minutes":
        case "minute":
            return "${timeInterval}minutes"
        
        case "hour": // there is no unit for hour in aql. So convert it to minutes
             return "${String.valueOf(Integer.parseInt(timeInterval)*60)}minutes"

        case "days":
        case "d":
        case "day":
            return "${timeInterval}d"

        //case "weeks":
        //case "w":
        //    return "w"

        case "months":
        case "mo":
        case "month":
            return "${timeInterval}mo"

        case "years":
        case "y":
        case "year":
            return "${timeInterval}y"

        default:
            def errorMessage = "$timeUnit is no valid time unit. Please check your request or scheduled policy."
            log.error errorMessage
            throw new CancelException(errorMessage, 400)
    }
}
