#####################################################################################
service=$(docker_user)/boost-gui
version=dev
########### DOCKER ##################################################################
tag=$(service):$(version)
rootdir=$(realpath .)

dbuild: 
	docker build -t $(tag) -f Dockerfile.source --build-arg BUILD_VERSION=$(version) $(rootdir)/../../../

dpush: dbuild
	docker push $(tag)

dscan: dbuild
	docker scan --accept-license $(tag)		
#####################################################################################
.PHONY:
	dbuild dpush dscan
