#####################################################################################
service=$(docker_user)/booster-bitswap-dev
version=dev
########### DOCKER ##################################################################
tag=$(service):$(version)
rootdir=$(realpath .)

dbuild: 
	docker build --build-arg LOTUS_TEST_IMAGE=$(lotus_test_image) \
		-t $(tag) -f Dockerfile.source $(rootdir)/../../../

dpush: dbuild
	docker push $(tag)

dscan: dbuild
	docker scan --accept-license $(tag)		
#####################################################################################
.PHONY:
	dbuild dpush dscan
