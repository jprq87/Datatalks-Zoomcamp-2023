from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer.load("jprq-zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()