from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.filesystems import GitHub

github_block = GitHub.load("github-block-jprq")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow",
    infrastructure=github_block,
)


if __name__ == "__main__":
    docker_dep.apply()