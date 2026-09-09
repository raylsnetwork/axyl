.PHONY: help attest udeps check test test-faucet fmt clippy docker-login docker-testnet docker-push docker-builder docker-builder-init up down validators pr init-submodules update-rayls-contracts revert-submodule

# full path for the Makefile
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BASE_DIR:=$(shell basename $(ROOT_DIR))

.DEFAULT: help

# Default tag is latest if not specified
TAG ?= latest

# Date-pinned nightly for rustfmt — unstable-option behavior drifts between
# nightlies, so fmt must match CI (.github/workflows/pr.yaml fmt job).
# Install with: rustup toolchain install $(FMT_NIGHTLY) --profile minimal --component rustfmt
FMT_NIGHTLY := nightly-2026-06-24

help:
	@echo ;
	@echo "make attest" ;
	@echo "    :::> Run CI locally and submit signed attestation to testnet." ;
	@echo ;
	@echo "make udeps" ;
	@echo "    :::> Check unused dependencies in the entire project by package." ;
	@echo "    :::> Dev needs 'cargo-udeps' installed." ;
	@echo "    :::> Dev also needs rust nightly and protobuf (on mac). ";
	@echo "    :::> To install run: 'cargo install cargo-udeps --locked'." ;
	@echo ;
	@echo "make check" ;
	@echo "    :::> Cargo check workspace with all features activated." ;
	@echo ;
	@echo "make test" ;
	@echo "    :::> Run all tests in workspace with all features using 4 threads." ;
	@echo ;
	@echo "make test-faucet" ;
	@echo "    :::> Test faucet integration test in main binary." ;
	@echo ;
	@echo "make test-restarts" ;
	@echo "    :::> Test restart integration tests." ;
	@echo ;
	@echo "make fmt" ;
	@echo "    :::> cargo +$(FMT_NIGHTLY) fmt (date-pinned to match the CI fmt check)" ;
	@echo ;
	@echo "make clippy" ;
	@echo "    :::> Cargo +nightly clippy for all features with fix enabled." ;
	@echo ;
	@echo "make docker-login" ;
	@echo "    :::> Setup docker registry using gcloud artifacts." ;
	@echo ;
	@echo "make docker-testnet" ;
	@echo "    :::> Build rayls-network binary and push to gcloud artifact registry with latest image tag." ;
	@echo ;
	@echo "make docker-push" ;
	@echo "    :::> Push testnet:latest image to gcloud artifact registry." ;
	@echo ;
	@echo "make docker-builder" ;
	@echo "    :::> Create docker builder for building rayls-network binary container images." ;
	@echo ;
	@echo "make docker-builder-init" ;
	@echo "    :::> Bootstrap the docker builder for building rayls-network binary container images." ;
	@echo ;
	@echo "make up" ;
	@echo "    :::> Launch docker compose file with 4 local validators in detached state." ;
	@echo ;
	@echo "make down" ;
	@echo "    :::> Bring the docker compose containers down and remove orphans and volumes." ;
	@echo ;
	@echo "make validators" ;
	@echo "    :::> Run 4 validators locally (outside of docker)." ;
	@echo ;

# run CI locally and submit attestation githash to on-chain program
attest:
	./etc/test/test-and-attest.sh ;

# check for unused dependencies
udeps:
	find . -type f -name Cargo.toml -exec sed -rne 's/^name = "(.*)"/\1/p' {} + | xargs -I {} sh -c "echo '\n\n{}:' && cargo +nightly udeps --package {}" ;

check:
	cargo check --workspace --all-features --all-targets ;

# run workspace unit tests
test:
	cargo test --workspace --no-fail-fast -- --show-output ;

# run faucet integration test
test-faucet:
	cargo test --package rayls-network --features faucet --test it ;

# run restart integration tests
test-restarts:
	cargo test test_restarts -- --ignored ;

# format using the pinned nightly toolchain (same as the CI fmt check)
fmt:
	cargo +$(FMT_NIGHTLY) fmt ;

# clippy formatter + try to fix problems
clippy:
	cargo +nightly clippy --workspace --all-features --fix ;

# login to gcloud artifact registry for managing docker images
docker-login:
	gcloud auth application-default login ;
	gcloud auth configure-docker us-docker.pkg.dev ;

# build and push latest testnet image for amd64 and arm64
docker-testnet:
	docker buildx build -f ./etc/Dockerfile --platform linux/amd64,linux/arm64 -t us-docker.pkg.dev/rayls-network/rayls-public/testnet:$(TAG) . --push ;

# push local testnet:latest to the gcloud artifact registry
docker-push:
	docker push us-docker.pkg.dev/rayls-network/rayls-public/testnet:$(TAG) ;

# docker buildx used for multiple processor image building
docker-builder:
	docker buildx create --name rayls-builder --use ;

# inpect and bootstrap docker buildx for multiple processor image building
docker-builder-init:
	docker buildx inspect --bootstrap ;

# bring docker compose up
up:
	docker compose -f ./etc/docker-network/compose.yaml up --build --remove-orphans --detach ;

# bring docker compose down
down:
	docker compose -f ./etc/docker-network/compose.yaml down --remove-orphans -v ;

# alternative approach to run 4 local validator nodes outside of docker on local machine
validators:
	./etc/test-network/local-testnet.sh ;

# workspace tests that don't require faucet credentials
public-tests:
	cargo test --workspace --exclude rayls-execution-faucet --no-fail-fast -- --show-output ;
	cargo test -p rayls-network --test it test_epoch_boundary -- --ignored ;
	cargo test test_restarts -- --ignored ;

# local checks to ensure PR is ready
pr:
	make fmt && \
	make clippy && \
	make public-tests
