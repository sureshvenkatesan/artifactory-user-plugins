Artifactory Clean Docker Images User Plugin
===========================================

This plugin is used to clean Docker repositories based on configured cleanup
policies.

Configuration
-------------

The `cleanDockerImages.properties` file has the following field:

- `dockerRepos`: A list of Docker repositories to clean. If a repo is not in
  this list, it will not be cleaned.

For example:

``` json
dockerRepos = ["example-docker-local", "example-docker-local-2"]
```

Usage
-----

Cleanup policies are specified as labels on the Docker image. Currently, this
plugin supports the following policies:

- `maxDays`: The maximum number of days a Docker image can exist in an
  Artifactory repository. Any images older than this will be deleted.
- `maxCount`: The maximum number of versions of a particular image which should
  exist. For example, if there are 10 versions of a Docker image and `maxCount`
  is set to 6, the oldest 4 versions of the image will be deleted.

To set these labels for an image, add them to the Dockerfile before building:

``` dockerfile
LABEL com.jfrog.artifactory.retention.maxCount="10"
LABEL com.jfrog.artifactory.retention.maxDays="7"
```

When a Docker image is deployed, Artifactory will automatically create
properties reflecting each of its labels. These properties are read by the
plugin in order to decide on the cleanup policy for the image.

Cleanup can be triggered via a REST endpoint. For example:

``` shell
curl -XPOST -uadmin:password http://localhost:8081/artifactory/api/plugins/execute/cleanDockerImages
```

A dry run can also be triggered:

``` shell
curl -XPOST -uadmin:password "http://localhost:8081/artifactory/api/plugins/execute/cleanDockerImages?params=dryRun=true"
```

- To add a new list of docker repositories to cleanup
``` shell
curl -X POST -v -u admin:password -H 'Content-Type: application/json'  "http://localhost:8081/artifactory/api/plugins/execute/updateDockerReposToClean?params=command=AddReposToClean" -T docker1.json
```
Here docker1.json contains the new list of docker repositories that needs to be added to the dockerRepos in the CleanDockerImagesTest.properties
```json
{
    "dockerRepos": ["example-docker-local-3","example-docker-local-4"]
}
```

- To remove a list of docker repositories from dockerRepos in the CleanDockerImagesTest.properties , that do not need cleanup
``` shell
curl -X POST -v -u admin:password -H 'Content-Type: application/json'  "http://localhost:8081/artifactory/api/plugins/execute/updateDockerReposToClean?params=command=DeleteReposToClean" -T docker2.json
```
Here docker2.json contains a list of docker repositories to remove from dockerRepos in the CleanDockerImagesTest.properties 
```json
{
    "dockerRepos": ["example-docker-local-2","example-docker-local-3"]
}
```