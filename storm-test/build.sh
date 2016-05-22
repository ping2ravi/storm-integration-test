cwd=$(pwd)

docker stop StormIntegrationBuild
docker rm StormIntegrationBuild


BUILD_OUTPUT_DIRECTORY=/var/build-output

if [ -z "${HOST_JENKINS_HOME+xxx}" ];
then
HOST_JENKINS_HOME=${cwd}
fi

MVN_COMMAND=$1
if [ -z "${profile+xxx}" ];
then
MVN_COMMAND=clean install
fi

echo docker run --name=StormIntegrationBuild -e MVN_COMMAND="$MVN_COMMAND" -e BUILD_OUTPUT=$BUILD_OUTPUT_DIRECTORY -v $HOST_JENKINS_HOME/workspace/${JOB_NAME}/build-output:$BUILD_OUTPUT_DIRECTORY -v ~/.m2:/root/.m2/ -e GIT_REPO=https://github.com/ping2ravi/storm-integration-test.git --net=host ping2ravi/maven-build-docker:latest

docker run --name=StormIntegrationBuild -e MVN_COMMAND="$MVN_COMMAND" -e BUILD_OUTPUT=$BUILD_OUTPUT_DIRECTORY -v $HOST_JENKINS_HOME/workspace/${JOB_NAME}/build-output:$BUILD_OUTPUT_DIRECTORY -v ~/.m2:/root/.m2/ -e GIT_REPO=https://github.com/ping2ravi/storm-integration-test.git --net=host ping2ravi/maven-build-docker:latest
 
docker rm StormIntegrationBuild
