.PHONY: build run setup-localstack

IMAGE_NAME := imdb-import

build:
	@docker build -t ${IMAGE_NAME} .

run:
	@$(MAKE) build
	@docker run --rm -it --net="host" \
		-e RDS_SERVER="${RDS_SERVER}" \
		-e RDS_DATABASE="${RDS_DATABASE}" \
		-e RDS_USER="${RDS_USER}" \
		-e RDS_PASSWORD="${RDS_PASSWORD}" \
		${IMAGE_NAME}

