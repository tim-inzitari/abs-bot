#!/usr/bin/env bash

set -euo pipefail

IMAGE_NAME="tsinzitari/abs-bot"
DEFAULT_TAG="latest"

TAG="${1:-$DEFAULT_TAG}"
PLATFORM="${PLATFORM:-linux/amd64}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found in PATH." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not available." >&2
  exit 1
fi

echo "Building Python image ${IMAGE_NAME}:${TAG} for platform ${PLATFORM}..."
docker build --platform "${PLATFORM}" -t "${IMAGE_NAME}:${TAG}" .

echo "Tagging ${IMAGE_NAME}:${TAG} as ${IMAGE_NAME}:latest..."
docker tag "${IMAGE_NAME}:${TAG}" "${IMAGE_NAME}:latest"

echo "Pushing ${IMAGE_NAME}:${TAG}..."
docker push "${IMAGE_NAME}:${TAG}"

if [[ "${TAG}" != "latest" ]]; then
  echo "Pushing ${IMAGE_NAME}:latest..."
  docker push "${IMAGE_NAME}:latest"
fi

echo "Done."
