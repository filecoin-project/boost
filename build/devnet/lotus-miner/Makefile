#####################################################################################
service=$(docker_user)/lotus-miner-dev
version?=$(lotus_version)
########### DOCKER ##################################################################
tag=$(service):$(version)
dbuild: 
	docker build -t $(tag) --build-arg LOTUS_TEST_IMAGE=$(lotus_test_image) --build-arg BUILD_VERSION=$(version) .

dpush: dbuild
	docker push $(tag)

dscan: dbuild
	docker scan --accept-license $(tag)		
#####################################################################################
.PHONY:
	dbuild dpush dscan
