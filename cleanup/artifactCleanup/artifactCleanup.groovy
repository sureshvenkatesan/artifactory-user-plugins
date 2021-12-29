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

import org.apache.commons.lang3.StringUtils
import org.artifactory.api.repo.exception.ItemNotFoundRuntimeException
import org.artifactory.exception.CancelException

import groovy.json.JsonSlurper
import groovy.time.TimeCategory
import groovy.time.TimeDuration
import groovy.transform.Field

import java.text.SimpleDateFormat

import org.artifactory.resource.ResourceStreamHandle
import groovy.json.JsonOutput
import groovy.json.JsonBuilder

@Field final String CONFIG_FILE_PATH = "plugins/${this.class.name}.json"
@Field final String PROPERTIES_FILE_PATH = "plugins/${this.class.name}.properties"
@Field final String DEFAULT_TIME_UNIT = "month"
@Field final int DEFAULT_TIME_INTERVAL = 1

@Field final String SCRIPT_FILE_PATH = "plugins/${this.class.name}.groovy"

class Global {
    static Boolean stopCleaning = false
    static Boolean pauseCleaning = false
    static int paceTimeMS = 0
}

// curl command example for running this plugin (Prior to Artifactory 5.x, use pipe '|' and not semi-colons ';' for parameters separation).
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/cleanup?params=timeUnit=day;timeInterval=1;repos=libs-release-local;dryRun=true;paceTimeMS=2000;disablePropertiesSupport=true"
//
// For a HA cluster, the following commands have to be directed at the instance running the script. Therefore it is best to invoke
// the script directly on an instance so the below commands can operate on same instance
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/cleanupCtl?params=command=pause"
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/cleanupCtl?params=command=resume"
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/cleanupCtl?params=command=stop"
// curl -i -uadmin:password -X POST "http://localhost:8081/artifactory/api/plugins/execute/cleanupCtl?params=command=adjustPaceTimeMS;value=-1000"

def pluginGroup = 'cleaners'

// The cleanup config cache. A thread-safe, write-through cache for the Cleanup config
// file.
configMutex = new Object()
configData = null

executions {
    cleanup(groups: [pluginGroup]) { params ->
        def timeUnit = params['timeUnit'] ? params['timeUnit'][0] as String : DEFAULT_TIME_UNIT
        def timeInterval = params['timeInterval'] ? params['timeInterval'][0] as int : DEFAULT_TIME_INTERVAL
        def repos = params['repos'] as String[]
        def dryRun = params['dryRun'] ? new Boolean(params['dryRun'][0]) : false
        def disablePropertiesSupport = params['disablePropertiesSupport'] ? new Boolean(params['disablePropertiesSupport'][0]) : false
        def paceTimeMS = params['paceTimeMS'] ? params['paceTimeMS'][0] as int : 0

        // Enable fallback support for deprecated month parameter
        if ( params['months'] && !params['timeInterval'] ) {
            log.info('Deprecated month parameter is still in use, please use the new timeInterval parameter instead!', properties)
            timeInterval = params['months'][0] as int
        } else if ( params['months'] ) {
            log.warn('Deprecated month parameter and the new timeInterval are used in parallel: month has been ignored.', properties)
        }

        artifactCleanup(timeUnit, timeInterval, repos, log, paceTimeMS, dryRun, disablePropertiesSupport)
    }

    cleanupCtl(groups: [pluginGroup]) { params ->
        def command = params['command'] ? params['command'][0] as String : ''

        switch ( command ) {
            case "stop":
                Global.stopCleaning = true
                log.info "Stop request detected"
                break
            case "adjustPaceTimeMS":
                def adjustPaceTimeMS = params['value'] ? params['value'][0] as int : 0
                def newPaceTimeMS = ((Global.paceTimeMS + adjustPaceTimeMS) > 0) ? (Global.paceTimeMS + adjustPaceTimeMS) : 0
                log.info "Pacing adjustment request detected, adjusting old pace time ($Global.paceTimeMS) by $adjustPaceTimeMS to new value of $newPaceTimeMS"
                Global.paceTimeMS = newPaceTimeMS
                break
            case "pause":
                Global.pauseCleaning = true
                log.info "Pause request detected"
                break
            case "resume":
                Global.pauseCleaning = false
                log.info "Resume request detected"
                break
            case "setCurrentTime":
                Global.stopCleaning = true
                log.info "Will update the script timestamp. Reload the plugin after this"
                SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
                def scriptFile = new File(ctx.artifactoryHome.etcDir, SCRIPT_FILE_PATH)
                log.info "Last Modified Date (before):- " + sdf.format(scriptFile.lastModified())
                
                scriptFile.setLastModified(new Date().getTime());
                log.info "Last Modified Date (after):- " + sdf.format(scriptFile.lastModified())
                break
            default:
                log.info "Missing or invalid command, '$command'"
        }
    }

    addTeamRepoCleanup(version: '1.0', description: 'Add cleanup schedule for Team Repos', httpMethod: 'POST', groups: [pluginGroup]) { params, ResourceStreamHandle body ->
        def command = params['command'] ? params['command'][0] as String : ''

                switch ( command ) {
                    case "TeamRepoCleanup":
                        // ensure that the request body is not empty
                            assert body
                            def newCleanupjson = new JsonSlurper().parse(new InputStreamReader(body.inputStream))
                            log.info "Appending new Team Onboarding Cleanup Policy. Received following Team Policy"
                            log.info JsonOutput.prettyPrint(JsonOutput.toJson(newCleanupjson))

                            if (isConfigFileNotEmpty()) {
                            // If a configuration already exists, don't replace it: The PUT endpoint
                            // should be used for that instead
                                if(CheckRepoCleanupPolicyExists(newCleanupjson)){
                                    status = 409
                                    message = "Team Repos clean policy already exists on this instance. Use 'PUT' to update"
                                    return
                                }
                                else{
                                        log.info "writing/appending the newCleanupjson to the configFile"
                                        synchronized (configMutex) {
                                            newCleanupjson?.policies.each { policy ->
                                                configData.policies << policy
                                            }
                                        }
                                        if(writeToConfigFile()) {
                                           log.info "writing  finalCleanupjson successful"
                                           message = "Successfully Added Team cleanup  Policy " + JsonOutput.prettyPrint(JsonOutput.toJson(newCleanupjson))
                                           status = 200
                                       }   
                                }

                            }
                            else { 
                               // COnfig file is empty. Cleanup Policy defined for the very first  time , hence not using writeToConfigFile() as it resiults in json with \n

                                  def configFile = new File(ctx.artifactoryHome.etcDir, CONFIG_FILE_PATH)
                                  configFile.write(new JsonBuilder(newCleanupjson).toPrettyString())
                                   log.info "writing  newCleanupjson successful"
                                   message = "Successfully Added Team cleanup  Policy " + JsonOutput.prettyPrint(JsonOutput.toJson(newCleanupjson))
                                   status = 200
                               
                            }
                        
                        break
                    default:
                        log.info "Missing or invalid command, '$command'"
                        status = 400
                        return
        }
    }

    updateTeamRepoCleanup(version: '1.0', description:'Update cleanup schedule for Team Repos', httpMethod: 'PUT', groups: [pluginGroup]) { params, ResourceStreamHandle body ->
        def command = params['command'] ? params['command'][0] as String : ''

                switch ( command ) {
                    case "TeamRepoCleanup":
                        // ensure that the request body is not empty
                            assert body
                            def newCleanupjson = new JsonSlurper().parse(new InputStreamReader(body.inputStream))
                            log.info "Updating new Team Onboarding Cleanup Policy. Received following Team Policy"
                            log.info JsonOutput.prettyPrint(JsonOutput.toJson(newCleanupjson))

                            if (isConfigFileNotEmpty()) {
                                // If a configuration already exists, don't replace it: The PUT endpoint
                                // should be used for that instead
                                    if(CheckRepoCleanupPolicyExists(newCleanupjson)){
                                        // Will this result in duplicate cleanup policy for the Team's Repos
                                        def duplicateRepos = UpdateRepoCleanupPolicyAndNoDuplicates(newCleanupjson)
                                        if(duplicateRepos)
                                        {
                                            status = 409
                                            message = "Will result in duplicate cleanup policy for these Team Repos : ${duplicateRepos}"
                                            return
                                        }
                                        else {
                                            // Will not resut in duplicate cleanup policy for these Team Repos. Ok to Update
                                                if(writeToConfigFile()) {
                                                log.info "writing  finalCleanupjson successful"
                                                message = "Successfully Updated Team cleanup  Policy " + JsonOutput.prettyPrint(JsonOutput.toJson(newCleanupjson))
                                                status = 200
                                            }   
                                        }
                                }
                                else {
                                    // No team repo cleanup policies exist
                                    status = 409
                                    message = "Team Repos clean policy does not exists on this instance. Use 'POST' to add new policy for repos used by this team"
                                    return 
                                }
                            }
                            else { 
                               // cleanup Policy defined for the very first  time
                                    status = 409
                                    message = "Team Repos clean policy does not exists on this instance. Use 'POST' to add new policy for repos used by this team"
                                    return                               
                            }                                    
                        break
                    default:
                        log.info "Missing or invalid command, '$command'"
                        status = 400
        }
    }

    // Using POST instaed of DELETE to delete  Team repo cleanup policies because , 
    //DELETE fails with error similar to https://stackoverflow.com/questions/25704985/groovy-httpbuilder-producing-error-when-parsing-valid-json
    deleteTeamRepoCleanup(version:'1.0', description:'Delete cleanup schedule for Team Repos', httpMethod: 'POST', groups: [pluginGroup]) { params, ResourceStreamHandle body ->
        def command = params['command'] ? params['command'][0] as String : ''

                switch ( command ) {
                    case "TeamRepoCleanup":
                        // ensure that the request body is not empty
                            assert body
                            def newCleanupjson = new JsonSlurper().parse(new InputStreamReader(body.inputStream))
                            log.info "Deleting Team Onboarding Cleanup Policy. Received following Team Policy to delete"
                            log.info JsonOutput.prettyPrint(JsonOutput.toJson(newCleanupjson))

                            if (isConfigFileNotEmpty()) {
                                // If a configuration already exists, only then you can delete it
                                def deletedRepos = DeleteRepoCleanupPolicy(newCleanupjson)
                                    if(deletedRepos){

                                            // Save the repo policy deletes to config file 
                                            if(writeToConfigFile()) {
                                                log.info "Deletion of cleanup policies and writing config successful"
                                                message = "Successfully Deleted Team cleanup  Policy " + JsonOutput.prettyPrint(JsonOutput.toJson(newCleanupjson))
                                                status = 200
                                            }   
                                    }
                                    else {
                                        // No team repo cleanup policies exist
                                        status = 409
                                        message = "Cannot Delete as Team Repos clean policy does not exists on this instance."
                                        return 
                                    }
                            }
                            else { 
                               // No team repo cleanup policies exist
                                    status = 409
                                    message = "Cannot Delete as config is empty and so Team Repos clean policy does not exists on this instance."
                                    return                               
                            }                                    
                        break
                    default:
                        log.info "Missing or invalid command, '$command'"
                        status = 400
        }
    }


}

// returns list of repos for which the cleanup policies have to be DELETED 
private def DeleteRepoCleanupPolicy(newCleanupjson){
        def finalPolicies = [] 
        def final_repos_to_clean_up = []

        configData?.policies.each { oldpolicy ->
            def delete_oldpolicy = false
            newCleanupjson?.policies.each { newpolicy ->
                if(!(newpolicy.repos).disjoint(oldpolicy.repos) ) {
                    //disjoint() method returns true if there's no item that is common to both lists
                    // Since now there is a  repo thta is common in old and new policy replace old policy with new
                    delete_oldpolicy = true
                    final_repos_to_clean_up << (oldpolicy.containsKey("repos") ? oldpolicy.repos  : ["__none__"])
                    //log.info "if-> finalPolicies ${finalPolicies} "
                    log.info "In DeleteRepoCleanupPolicy -> final_repos_to_clean_up ${final_repos_to_clean_up} "
                }
            }
                    if(!delete_oldpolicy){
                        // the old policy will not be deleted . So keep the old policy.
                        finalPolicies << oldpolicy
                    }

        }


        final_repos_to_clean_up.collectMany{ it } // same as flatten the list

        if (final_repos_to_clean_up?.size() > 0) {
                synchronized (configMutex) {
                    configData?.policies = finalPolicies
                }
            
        }
        else {
            log.warn "Cannot update config as there are no matching team  policies to delete"
        }

        return final_repos_to_clean_up


}


// returns list of repos with could have duplicate cleanup policies. If no duplicates then updates configData with the new Policies 
private def UpdateRepoCleanupPolicyAndNoDuplicates(newCleanupjson){
        def finalPolicies = [] 
        def final_repos_to_clean_up = []

        configData?.policies.each { oldpolicy ->
            def replace_oldpolicy = false
            newCleanupjson?.policies.each { newpolicy ->
                if(!(newpolicy.repos).disjoint(oldpolicy.repos) ) {
                    //disjoint() method returns true if there's no item that is common to both lists
                    // Since now there is a  repo thta is common in old and new policy replace old policy with new
                    replace_oldpolicy = true
                    finalPolicies << newpolicy
                    final_repos_to_clean_up << (newpolicy.containsKey("repos") ? newpolicy.repos  : ["__none__"])
                    log.info "if-> finalPolicies ${finalPolicies} "
                    log.info "if-> final_repos_to_clean_up ${final_repos_to_clean_up} "
                }
            }
                    if(!replace_oldpolicy){
                        // the old policy was not replaced . So keep the old policy.
                        finalPolicies << oldpolicy
                        final_repos_to_clean_up << (oldpolicy.containsKey("repos") ? oldpolicy.repos  : ["__none__"])
                        log.info "else -> finalPolicies ${finalPolicies}"
                        log.info "else -> final_repos_to_clean_up ${final_repos_to_clean_up} "

                    }

        }


        final_repos_to_clean_up.collectMany{ it } // same as flatten the list
        def unique_repos = []
        def duplicate_repos = []

        final_repos_to_clean_up.each { unique_repos.contains(it) ? duplicate_repos << it : unique_repos << it }

        if (duplicate_repos?.size() > 0) {
            log.warn "Cannot update config as there are duplicate cleanup policies for repos ${duplicate_repos}"
        }
        else {

                synchronized (configMutex) {
                    configData?.policies = finalPolicies
                }
        }

        return duplicate_repos


}

// returns true if the cleanup policy for any of the team repos already exists
private boolean CheckRepoCleanupPolicyExists(json) {

    //def config = new JsonSlurper().parse(configFile.toURL())
    def currentRepos = []
     log.info "In isValidTeamOnBoardCleanupPolicy"
    configData.policies.each { policySettings1 ->
       currentRepos << policySettings1.repos
    }
    //currentRepos.flatten()
    currentRepos.collectMany{ it }

    def newrepos = []
    json?.policies.each { policySettings2 ->
       log.info "policySettings2: ${policySettings2.repos}"
       newrepos << (policySettings2.containsKey("repos") ? policySettings2.repos  : ["__none__"])
    }
      //newrepos.flatten()
      newrepos.collectMany{ it }

       log.info "currentRepos: ${currentRepos} "
       log.info "newrepos: ${newrepos}"
       //disjoint() method returns true if there's no item that is common to both lists. 
       if (currentRepos.disjoint(newrepos) == true)
       {
           log.info "the new config contains repos for which clean has not been configured yet"  
           return false
       }
       else {
           def duplicate_repos = currentRepos.intersect(newrepos)
           log.warn "Clean for repos '${duplicate_repos}' are already defined. Remove them from the newConfig and try again"
           return true
       }

}

// USed for all writes to the config file
private def writeToConfigFile(){

    log.info "writing newCleanupjson"
    synchronized (configMutex) {
       def configFile = new File(ctx.artifactoryHome.etcDir, CONFIG_FILE_PATH) 
       //configFile.write(new JsonBuilder(CleanupPoliciesJson).toPrettyString())
       //configData?.policies = newCleanupjson?.policies
       if (configData)
       {
         configFile.write (new  JsonBuilder(configData).toPrettyString())
        log.info "writing ${configData}"
         return true;
       }
    }
    return false;
}

// Return the cached config file from the 'configData' global. If the
// configuration has not yet been cached, read it from the config file first.

private def isConfigFileNotEmpty() {
    synchronized (configMutex) {
       // if (!configData ) {
            // Not cached yet. So cache it now.
            def configFile = new File(ctx.artifactoryHome.etcDir, CONFIG_FILE_PATH)
            if ( configFile.exists()) {
                configData = new JsonSlurper().parse(configFile.toURL())
                log.debug "Config file loaded successfully from disk."
            }

        //}
        return configData
    }
}

def deprecatedConfigFile = new File(ctx.artifactoryHome.etcDir, PROPERTIES_FILE_PATH)
def configFile = new File(ctx.artifactoryHome.etcDir, CONFIG_FILE_PATH)

if ( deprecatedConfigFile.exists() ) {

    if ( !configFile.exists() ) {
        def config = new ConfigSlurper().parse(deprecatedConfigFile.toURL())
        log.info "Schedule job policy list: $config.policies"

        def count=1
        config.policies.each{ policySettings ->
            def cron = policySettings[ 0 ] ? policySettings[ 0 ] as String : ["0 0 5 ? * 1"]
            def repos = policySettings[ 1 ] ? policySettings[ 1 ] as String[] : ["__none__"]
            def months = policySettings[ 2 ] ? policySettings[ 2 ] as int : 6
            def paceTimeMS = policySettings[ 3 ] ? policySettings[ 3 ] as int : 0
            def dryRun = policySettings[ 4 ] ? policySettings[ 4 ] as Boolean : false
            def disablePropertiesSupport = policySettings[ 5 ] ? policySettings[ 5 ] as Boolean : false

            jobs {
                "scheduledCleanup_$count"(cron: cron) {
                    log.info "Policy settings for scheduled run at($cron): repo list($repos), timeUnit(month), timeInterval($months), paceTimeMS($paceTimeMS) dryrun($dryRun) disablePropertiesSupport($disablePropertiesSupport)"
                    artifactCleanup( "month", months, repos, log, paceTimeMS, dryRun, disablePropertiesSupport )
                }
            }
            count++
        }
    } else  {
        log.warn "Deprecated 'artifactCleanup.properties' file is still present, but ignored. You should remove the file."
    }
}

if ( configFile.exists() ) {
  
    def config = new JsonSlurper().parse(configFile.toURL())
    log.info "Schedule job policy list: $config.policies"

    def count=1
    config.policies.each{ policySettings ->
        def cron = policySettings.containsKey("cron") ? policySettings.cron as String : ["0 0 5 ? * 1"]
        def repos = policySettings.containsKey("repos") ? policySettings.repos as String[] : ["__none__"]
        def timeUnit = policySettings.containsKey("timeUnit") ? policySettings.timeUnit as String : DEFAULT_TIME_UNIT
        def timeInterval = policySettings.containsKey("timeInterval") ? policySettings.timeInterval as int : DEFAULT_TIME_INTERVAL
        def paceTimeMS = policySettings.containsKey("paceTimeMS") ? policySettings.paceTimeMS as int : 0
        def dryRun = policySettings.containsKey("dryRun") ? new Boolean(policySettings.dryRun) : false
        def disablePropertiesSupport = policySettings.containsKey("disablePropertiesSupport") ? new Boolean(policySettings.disablePropertiesSupport) : false

        jobs {
            "scheduledCleanup_$count"(cron: cron) {
                log.info "Policy settings for scheduled run at($cron): repo list($repos), timeUnit($timeUnit), timeInterval($timeInterval), paceTimeMS($paceTimeMS) dryrun($dryRun) disablePropertiesSupport($disablePropertiesSupport)"
                artifactCleanup( timeUnit, timeInterval, repos, log, paceTimeMS, dryRun, disablePropertiesSupport )
            }
        }
        count++
    }  
}

if ( deprecatedConfigFile.exists() && configFile.exists() ) {
    log.warn "The deprecated artifactCleanup.properties and the new artifactCleanup.json are defined in parallel. You should migrate the old file and remove it."
}

private def artifactCleanup(String timeUnit, int timeInterval, String[] repos, log, paceTimeMS, dryRun = false, disablePropertiesSupport = false) {
    log.info "Starting artifact cleanup for repositories $repos, until $timeInterval ${timeUnit}s ago with pacing interval $paceTimeMS ms, dryrun: $dryRun, disablePropertiesSupport: $disablePropertiesSupport"

    // Create Map(repo, paths) of skiped paths (or others properties supported in future ...)
    def skip = [:]
    if ( ! disablePropertiesSupport && repos){
        skip = getSkippedPaths(repos)
    }

    def calendarUntil = Calendar.getInstance()

    calendarUntil.add(mapTimeUnitToCalendar(timeUnit), -timeInterval)

    def calendarUntilFormatted = new SimpleDateFormat("yyyy/MM/dd HH:mm").format(calendarUntil.getTime());
    log.info "Removing all artifacts not downloaded since $calendarUntilFormatted"

    //Global.stopCleaning = false
    int cntFoundArtifacts = 0
    int cntNoDeletePermissions = 0
    long bytesFound = 0
    long bytesFoundWithNoDeletePermission = 0
    def artifactsCleanedUp = searches.artifactsNotDownloadedSince(calendarUntil, calendarUntil, repos)
    artifactsCleanedUp.find {
        try {
            while ( Global.pauseCleaning ) {
                log.info "Pausing by request"
                sleep( 60000 )
            }

            if ( Global.stopCleaning ) {
                log.info "Stopping by request, ending loop"
                return true
            }

            if ( ! disablePropertiesSupport && skip[ it.repoKey ] && StringUtils.startsWithAny(it.path, skip[ it.repoKey ])){
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

        def sleepTime = (Global.paceTimeMS > 0) ? Global.paceTimeMS : paceTimeMS
        if (sleepTime > 0) {
            sleep( sleepTime )
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

private def getSkippedPaths(String[] repos) {
    def timeStart = new Date()
    def skip = [:]
    for (String repoKey : repos){
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
    }
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
