#####################################################################################
service=$(docker_user)/boost-dev
version=dev
########### DOCKER ##################################################################
tag=$(service):$(version)
rootdir=$(realpath .)

dbuild: 
	DOCKER_BUILDKIT=1 docker build --build-arg LOTUS_TEST_IMAGE=$(lotus_test_image) \
		-t $(tag) $(docker_args) -f Dockerfile.source $(rootdir)/../../../

dpush: dbuild
	docker push $(tag)

dscan: dbuild
	docker scan --accept-license $(tag)		
#####################################################################################
.PHONY:
	dbuild dpush dscan
