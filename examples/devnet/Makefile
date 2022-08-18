##################################################################################
start: 
	docker-compose up -d
	docker-compose logs -f
.PHONY: start
##################################################################################
ssh/boost: 
	docker-compose exec boost /bin/bash
.PHONY: ssh/boost
##################################################################################
clean: clean/docker
	rm -rf data
.PHONY: clean
clean/docker:
	docker-compose stop
	docker-compose rm -f
.PHONY: clean/docker
##################################################################################