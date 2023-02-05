from prefect.deployments import Deployment
from flows.parameterized_flow import etl_parent_flow
from prefect.infrastructure.docker import DockerContainer
from prefect.filesystems import GCS

storage = GCS.load("prefect-gcs")
docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow",
    infrastructure=docker_block,
    storage=storage
)

if __name__ == "__main__":
    docker_dep.apply()