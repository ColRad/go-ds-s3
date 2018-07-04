IPFS_PATH ?= ${HOME}/.ipfs
build:
	go build -buildmode=plugin -o=s3plugin.so

install: build
	mkdir -p ${IPFS_PATH}/plugins
	chmod +x s3plugin.so
	rm -f ${IPFS_PATH}/plugins/s3plugin.so
	cp s3plugin.so ${IPFS_PATH}/plugins/

