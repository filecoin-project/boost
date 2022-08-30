#####################################################################################
service=$(docker_user)/boost-dev
version=$(boost_version)
########### DOCKER ##################################################################
tag=$(service):$(version)

dbuild: 
	docker build --build-arg LOTUS_TEST_IMAGE=$(lotus_test_image)  --build-arg BUILD_VERSION=$(version) \
		-t $(tag) .

dpush: dbuild
	docker push $(tag)

dscan: dbuild
	docker scan --accept-license $(tag)		
#####################################################################################
.PHONY:
	dbuild dpush dscan
