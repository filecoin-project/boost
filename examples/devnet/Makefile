##################################################################################
start: 
	docker compose up -d
	docker compose logs -f
.PHONY: start
##################################################################################
ssh/boost: 
	docker compose exec boost /bin/bash
.PHONY: ssh/boost
##################################################################################
clean: clean/docker
	rm -rf data
.PHONY: clean
clean/all: clean
	rm -rf /var/tmp/filecoin-proof-parameters
.PHONY: clean/all
clean/docker:
	docker compose down
.PHONY: clean/docker
##################################################################################
