#####################################################################################
service=$(docker_user)/boost-gui
version=$(boost_version)
########### DOCKER ##################################################################
tag=$(service):$(version)

dbuild: 
	docker build -t $(tag) --build-arg BUILD_VERSION=$(version) .

dpush: dbuild
	docker push $(tag)

dscan: dbuild
	docker scan --accept-license $(tag)		
#####################################################################################
.PHONY:
	dbuild dpush dscan
