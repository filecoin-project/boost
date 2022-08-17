#####################################################################################
service=$(docker_user)/boost-gui
version=$(boost_version)
docker_dir=build/devnet/boost-gui
########### DOCKER ##################################################################
tag=$(service):$(version)

dbuild: 
	cd ../../../ && docker build -t $(tag) --build-arg BUILD_VERSION=$(version) -f $(docker_dir)/Dockerfile .

dpush: dbuild
	docker push $(tag)

dscan: dbuild
	docker scan --accept-license $(tag)		
#####################################################################################
.PHONY:
	dbuild dpush dscan
