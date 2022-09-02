#####################################################################################
service=$(docker_user)/boost-dev
version=$(boost_version)
docker_dir=docker/devnet/boost
########### DOCKER ##################################################################
tag=$(service):$(version)

dbuild: 
	cd ../../../ && \
		docker build --build-arg LOTUS_TEST_IMAGE=$(lotus_test_image)  --build-arg BUILD_VERSION=$(version) \
			-t $(tag) -f $(docker_dir)/Dockerfile .

dpush: dbuild
	docker push $(tag)

dscan: dbuild
	docker scan --accept-license $(tag)		
#####################################################################################
.PHONY:
	dbuild dpush dscan
