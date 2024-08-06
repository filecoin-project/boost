## How to set up Github Action runner
1. Clone this repo and copy out the files in this directory to one level above
```shell
mkdir github-runner

cd github-runner

git clone https://github.com/filecoin-project/curio.git
git clone https://github.com/filecoin-project/boost.git

cp -r boost/.github/utils/* .
```
2. Copy the Dockerfile

```shell
copy boost/.github/image/Dockerfile .
```

3. Create new image

```shell
docker buildx build -t curio/github-runner:latest .
```

4. Create systemd file. Please ensure to equal number files for Boost and Curio. If server can host 10 runner then 5 should be for Boost and 5 for Curio.

```shell
for i in {1..5}; do cat github-actions-runner.service | sed "s/NUM/$i/g" > github-actions-runner$i.service; done
for i in {6..10}; do cat github-actions-runner.service | sed 's/curio-/boost-/g' | sed "s/NUM/$i/g" > github-actions-runner$i.service; done
for i in {1..10}; do install -m 644 github-actions-runner$i.service /etc/systemd/system/ ; done
systemctl daemon-reload
```
5. Add the token to ENV files
6. Copy the ENV files to /etc

```shell
cp boost-github-actions-runner.env
cp curio-github-actions-runner.env
```

7. Start and Enable the services
```shell
for i in {1..10}; do systemctl start github-actions-runner$i.service; done
for i in {1..10}; do systemctl status github-actions-runner$i.service; done
for i in {1..10}; do systemctl enable github-actions-runner$i.service; done
```

8. Verify that new runners are visible in the repo.


## Github Token
Creating GitHub personal access token (PAT) for using by self-hosted runner make sure the following scopes are selected:

```text
repo (all)
admin:public_key - read:public_key
admin:repo_hook - read:repo_hook
admin:org_hook
notifications
workflow
```

## This setup is based in the https://github.com/myoung34/docker-github-actions-runner