import asyncio
import os
import subprocess
import time
from dataclasses import dataclass
from typing import Callable, List, TypeVar

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client import AppsV1Api, CoreV1Api, NetworkingV1Api
from kubernetes.client.models import (
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1DeploymentStrategy,
    V1EnvVar,
    V1HTTPIngressPath,
    V1HTTPIngressRuleValue,
    V1Ingress,
    V1IngressBackend,
    V1IngressRule,
    V1IngressServiceBackend,
    V1IngressSpec,
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1ResourceRequirements,
    V1RollingUpdateDeployment,
    V1Service,
    V1ServiceAccount,
    V1ServiceBackendPort,
    V1ServicePort,
    V1ServiceSpec,
)
from returns.context import RequiresContextIOResultE
from returns.curry import partial
from returns.functions import raise_exception
from returns.io import IOFailure, IOResultE, IOSuccess
from returns.pipeline import flow, is_successful
from returns.pointfree import alt, bind_context_ioresult, bind_ioresult, lash, map_
from returns.result import Failure, ResultE, Success
from toolz.functoolz import memoize

T = TypeVar("T")


@dataclass(frozen=True)
class Config:
    data_bucket: str
    data_s3_prefix: str
    cluster_name: str
    cluster_role_arn: str
    bucket_k8s_var: str
    prefix_k8s_var: str
    aws_region: str
    k8s_namespace: str
    sa_role_arn: str
    app_name: str
    app_port: int
    subnet_a: str
    subnet_b: str
    cert_arn: str
    site_node_port: str
    site_app_port: int
    n_pods: int
    app_image: str
    container_cpu: str
    container_memory: str
    load_balancer_port: int
    service_account_name: str
    ingress_name: str
    user_pool_arn: str
    user_pool_client_id: str
    user_pool_domain: str
    ingress_group_name: str


def add_v1_resource(
    init_fn: Callable[[], T],
    read_fn: Callable[[], T],
    create_fn: Callable[[T], T],
    change_check_fn: Callable[[T], bool],
    update_fn: Callable[[T], T],
) -> IOResultE[T]:
    creation_fn: Callable[[Exception], IOResultE[T]] = lambda _: create_v1_resource(
        init_fn=init_fn, create_fn=create_fn
    )
    return flow(
        read_v1_resource(read_fn=read_fn),
        lash(creation_fn),
        bind_ioresult(
            partial(
                update_v1_resource,
                change_check_fn=change_check_fn,
                init_fn=init_fn,
                update_fn=update_fn,
            )
        ),
    )


def read_v1_resource(read_fn: Callable[[], T]) -> IOResultE[T]:
    output: IOResultE[T]
    try:
        response = read_fn()
    except Exception as e:
        output = IOFailure(e)
    else:
        output = IOSuccess(response)
    return output


def update_v1_resource(
    original: T,
    change_check_fn: Callable[[T], bool],
    init_fn: Callable[[], T],
    update_fn: Callable[[T], T],
) -> IOResultE[T]:
    output: IOResultE[T]
    if change_check_fn(original):
        output = IOSuccess(original)
    else:
        try:
            updated = init_fn()
            response = update_fn(updated)
        except Exception as e:
            output = IOFailure(e)
        else:
            output = IOSuccess(response)
    return output


def create_v1_resource(
    init_fn: Callable[[], T], create_fn: Callable[[T], T]
) -> IOResultE[T]:
    output: IOResultE[T]
    initialized = init_fn()
    try:
        response = create_fn(initialized)
    except Exception as e:
        output = IOFailure(e)
    else:
        output = IOSuccess(response)
    return output


def configure_k8s_client() -> RequiresContextIOResultE[None, Config]:
    return RequiresContextIOResultE(
        lambda config: _configure_k8s_client(
            role_to_assume=config.cluster_role_arn,
            cluster_name=config.cluster_name,
            region=config.aws_region,
        )
    )


def _configure_k8s_client(
    role_to_assume: str, cluster_name: str, region: str
) -> IOResultE[None]:
    output: IOResultE[None]
    cmd = [
        "aws",
        "eks",
        "update-kubeconfig",
        "--region",
        region,
        "--role-arn",
        role_to_assume,
        "--name",
        cluster_name,
    ]
    try:
        subprocess.run(args=cmd, check=True)
    except Exception as e:
        output = IOFailure(e)
    else:
        output = IOSuccess(None)
    return output


def deploy_service_account(
    client: CoreV1Api,
) -> RequiresContextIOResultE[V1ServiceAccount, Config]:
    return RequiresContextIOResultE(
        lambda config: _deploy_service_account(
            client=client,
            name=config.service_account_name,
            namespace=config.k8s_namespace,
            role_arn=config.sa_role_arn,
        )
    )


def _deploy_service_account(
    client: CoreV1Api, name: str, namespace: str, role_arn: str
) -> IOResultE[V1ServiceAccount]:
    init_fn = partial(
        init_service_acct, name=name, namespace=namespace, role_arn=role_arn
    )
    read_fn = partial(
        client.read_namespaced_service_account, name=name, namespace=namespace
    )
    create_fn: Callable[
        [V1ServiceAccount], V1ServiceAccount
    ] = lambda resource: client.create_namespaced_service_account(
        namespace=namespace, body=resource
    )
    change_check_fn = partial(
        is_service_acct_unchanged, namespace=namespace, name=name, role_arn=role_arn
    )
    update_fn: Callable[
        [V1ServiceAccount], V1ServiceAccount
    ] = lambda resource: client.patch_namespaced_service_account(
        name=name, namespace=namespace, body=resource
    )
    return add_v1_resource(
        init_fn=init_fn,
        read_fn=read_fn,
        create_fn=create_fn,
        change_check_fn=change_check_fn,
        update_fn=update_fn,
    )


@memoize
def init_service_acct(name: str, namespace: str, role_arn: str) -> V1ServiceAccount:
    return V1ServiceAccount(
        api_version="v1",
        kind="ServiceAccount",
        metadata=V1ObjectMeta(
            name=name,
            namespace=namespace,
            annotations={"eks.amazonaws.com/role-arn": role_arn},
        ),
    )


def is_service_acct_unchanged(
    service_acct: V1ServiceAccount, namespace: str, role_arn: str, name: str
) -> bool:
    output: bool
    if service_acct.metadata is not None:
        if service_acct.metadata.annotations is not None:
            output = (
                service_acct.metadata.name == name
                and service_acct.metadata.namespace == namespace
                and service_acct.metadata.annotations.get("eks.amazonaws.com/role-arn")
                == role_arn
            )
        else:
            output = False
    else:
        output = False
    return output


def deploy_node_port(client: CoreV1Api) -> RequiresContextIOResultE[V1Service, Config]:
    return RequiresContextIOResultE(
        lambda config: _deploy_node_port(
            client=client,
            name=f"{config.app_name}-np-service",
            namespace=config.k8s_namespace,
            app_name=config.app_name,
            app_port=config.app_port,
        )
    )


def _deploy_node_port(
    client: CoreV1Api, name: str, namespace: str, app_name: str, app_port: int
) -> IOResultE[V1Service]:
    init_fn = partial(
        init_node_port,
        service_name=name,
        namespace=namespace,
        app_name=app_name,
        app_port=app_port,
    )
    read_fn = partial(client.read_namespaced_service, name=name, namespace=namespace)
    create_fn: Callable[
        [V1Service], V1Service
    ] = lambda resource: client.create_namespaced_service(
        namespace=namespace, body=resource
    )
    change_check_fn = partial(
        is_node_port_unchanged,
        namespace=namespace,
        name=name,
        app_name=app_name,
        app_port=app_port,
    )
    update_fn: Callable[
        [V1Service], V1Service
    ] = lambda resource: client.patch_namespaced_service(
        name=name, namespace=namespace, body=resource
    )
    return add_v1_resource(
        init_fn=init_fn,
        read_fn=read_fn,
        create_fn=create_fn,
        change_check_fn=change_check_fn,
        update_fn=update_fn,
    )


@memoize
def init_node_port(service_name: str, namespace: str, app_name: str, app_port: int):
    return V1Service(
        api_version="v1",
        kind="Service",
        metadata=V1ObjectMeta(name=service_name, namespace=namespace),
        spec=V1ServiceSpec(
            type="NodePort",
            selector={"app": app_name},
            ports=[V1ServicePort(port=app_port, target_port=app_port, protocol="TCP")],
        ),
    )


def is_node_port_unchanged(
    service: V1Service,
    name: str,
    namespace: str,
    app_name: str,
    app_port: int,
) -> bool:
    output: bool
    if service.metadata is not None and service.spec is not None:
        if (
            service.spec.selector is not None
            and service.spec.type is not None
            and service.spec.ports is not None
        ):
            if len(service.spec.ports) == 1:
                output = (
                    service.metadata.name == name
                    and service.metadata.namespace == namespace
                    and service.spec.type == "NodePort"
                    and service.spec.selector.get("app") == app_name
                    and service.spec.ports[0].port == app_port
                    and str(service.spec.ports[0].target_port) == str(app_port)
                    and service.spec.ports[0].protocol == "TCP"
                )
            else:
                output = False
        else:
            output = False
    else:
        output = False
    return output


def deploy_ingress(
    api_node_port: V1Service,
    client: NetworkingV1Api,
) -> RequiresContextIOResultE[V1Ingress, Config]:
    output: RequiresContextIOResultE[V1Ingress, Config]
    if api_node_port.metadata is not None:
        if api_node_port.metadata.name is not None:
            np_name = api_node_port.metadata.name
            output = RequiresContextIOResultE(
                lambda config: _deploy_ingress(
                    client=client,
                    name=config.ingress_name,
                    namespace=config.k8s_namespace,
                    subnets=[config.subnet_a, config.subnet_b],
                    certificate_arn=config.cert_arn,
                    api_node_port_name=np_name,  # potential failure here
                    api_app_port=config.app_port,
                    user_pool_arn=config.user_pool_arn,
                    user_pool_client_id=config.user_pool_client_id,
                    user_pool_domain=config.user_pool_domain,
                    ingress_group_name=config.ingress_group_name,
                )
            )
        else:
            output = RequiresContextIOResultE(
                lambda _: IOFailure(Exception("bad node port name supplied"))
            )
    else:
        output = RequiresContextIOResultE(
            lambda _: IOFailure(Exception("bad node port meta obj supplied"))
        )
    return output


def _deploy_ingress(
    client: NetworkingV1Api,
    name: str,
    namespace: str,
    subnets: List[str],
    certificate_arn: str,
    api_node_port_name: str,
    api_app_port: int,
    user_pool_arn: str,
    user_pool_client_id: str,
    user_pool_domain: str,
    ingress_group_name: str,
) -> IOResultE[V1Ingress]:
    init_fn = partial(
        init_ingress,
        name=name,
        namespace=namespace,
        subnets=subnets,
        certificate_arn=certificate_arn,
        api_node_port_name=api_node_port_name,
        api_app_port=api_app_port,
        user_pool_arn=user_pool_arn,
        user_pool_client_id=user_pool_client_id,
        user_pool_domain=user_pool_domain,
        ingress_group_name=ingress_group_name,
    )
    read_fn = partial(client.read_namespaced_ingress, name=name, namespace=namespace)
    create_fn: Callable[
        [V1Ingress], V1Ingress
    ] = lambda resource: client.create_namespaced_ingress(
        namespace=namespace, body=resource
    )
    change_check_fn = partial(
        is_ingress_unchanged,
        name=name,
        namespace=namespace,
        subnets=subnets,
        certificate_arn=certificate_arn,
        api_node_port_name=api_node_port_name,
        api_app_port=api_app_port,
        user_pool_arn=user_pool_arn,
        user_pool_client_id=user_pool_client_id,
        user_pool_domain=user_pool_domain,
        ingress_group_name=ingress_group_name,
    )
    update_fn: Callable[
        [V1Ingress], V1Ingress
    ] = lambda resource: client.patch_namespaced_ingress(
        name=name, namespace=namespace, body=resource
    )
    return add_v1_resource(
        init_fn=init_fn,
        read_fn=read_fn,
        create_fn=create_fn,
        change_check_fn=change_check_fn,
        update_fn=update_fn,
    )


@memoize
def init_ingress(
    name: str,
    namespace: str,
    subnets: List[str],
    certificate_arn: str,
    api_node_port_name: str,
    api_app_port: int,
    user_pool_arn: str,
    user_pool_client_id: str,
    user_pool_domain: str,
    ingress_group_name: str,
) -> V1Ingress:
    return V1Ingress(
        api_version="networking.k8s.io/v1",
        kind="Ingress",
        metadata=V1ObjectMeta(
            name=name,
            namespace=namespace,
            annotations={
                "alb.ingress.kubernetes.io/target-type": "ip",
                "alb.ingress.kubernetes.io/scheme": "internal",
                "alb.ingress.kubernetes.io/subnets": ",".join(sorted(subnets)),
                "alb.ingress.kubernetes.io/certificate-arn": certificate_arn,
                "alb.ingress.kubernetes.io/healthcheck-path": "/site",
                "alb.ingress.kubernetes.io/auth-type": "cognito",
                "alb.ingress.kubernetes.io/auth-idp-cognito": (
                    f'{{"userPoolARN": "{user_pool_arn}"'
                    + f',"userPoolClientID": "{user_pool_client_id}"'
                    + f', "userPoolDomain": "{user_pool_domain}"}}'
                ),
                "alb.ingress.kubernetes.io/group.name": ingress_group_name,
            },
        ),
        spec=V1IngressSpec(
            ingress_class_name="alb",
            rules=[
                V1IngressRule(
                    http=V1HTTPIngressRuleValue(
                        paths=[
                            V1HTTPIngressPath(
                                backend=V1IngressBackend(
                                    service=V1IngressServiceBackend(
                                        name=api_node_port_name,
                                        port=V1ServiceBackendPort(number=api_app_port),
                                    )
                                ),
                                path="/api",
                                path_type="Prefix",
                            )
                        ]
                    )
                ),
            ],
        ),
    )


# function written solely to appease mypy
def _pick(ingress_rule: V1IngressRule) -> str:
    if ingress_rule.http is not None:
        if len(ingress_rule.http.paths) > 0:
            if ingress_rule.http.paths[0].path is not None:
                output = ingress_rule.http.paths[0].path
            else:
                output = "0"  # ascii 2F
        else:
            output = "0"
    else:
        output = "0"
    return output


def is_ingress_unchanged(
    ingress: V1Ingress,
    name: str,
    namespace: str,
    subnets: List[str],
    certificate_arn: str,
    api_node_port_name: str,
    api_app_port: int,
    user_pool_arn: str,
    user_pool_client_id: str,
    user_pool_domain: str,
    ingress_group_name: str,
) -> bool:
    output: bool
    # excessive checks on optionals is to appease mypy
    if ingress.metadata is not None and ingress.spec is not None:
        if ingress.metadata.annotations is not None and ingress.spec.rules is not None:
            if len(ingress.spec.rules) == 1:
                if ingress.spec.rules[0].http is not None:
                    if len(ingress.spec.rules[0].http.paths) == 1:
                        if (
                            ingress.spec.rules[0].http.paths[0].backend.service
                            is not None
                            and ingress.spec.rules[0].http.paths[0].path is not None
                        ):
                            if (
                                ingress.spec.rules[0].http.paths[0].backend.service.port
                                is not None
                            ):
                                ingress.spec.rules.sort(key=lambda item: _pick(item))
                                output = (
                                    ingress.metadata.name == name
                                    and ingress.metadata.namespace == namespace
                                    and ingress.metadata.annotations.get(
                                        "alb.ingress.kubernetes.io/target-type"
                                    )
                                    == "ip"
                                    and ingress.metadata.annotations.get(
                                        "alb.ingress.kubernetes.io/scheme"
                                    )
                                    == "internal"
                                    and ingress.metadata.annotations.get(
                                        "alb.ingress.kubernetes.io/subnets"
                                    )
                                    == ",".join(sorted(subnets))
                                    and ingress.metadata.annotations.get(
                                        "alb.ingress.kubernetes.io/certificate-arn"
                                    )
                                    == certificate_arn
                                    and ingress.spec.ingress_class_name == "alb"
                                    and ingress.spec.rules[0].http.paths[0].path
                                    == "/api"
                                    and ingress.spec.rules[0].http.paths[0].path_type
                                    == "Prefix"
                                    and ingress.spec.rules[0]
                                    .http.paths[0]
                                    .backend.service.name
                                    == api_node_port_name
                                    and ingress.spec.rules[0]
                                    .http.paths[0]
                                    .backend.service.port.number
                                    == api_app_port
                                    and ingress.metadata.annotations.get(
                                        "alb.ingress.kubernetes.io/auth-type"
                                    )
                                    == "cognito"
                                    and ingress.metadata.annotations.get(
                                        "alb.ingress.kubernetes.io/auth-idp-cognito"
                                    )
                                    == (
                                        f'{{"userPoolARN": "{user_pool_arn}"'
                                        + f',"userPoolClientID": "{user_pool_client_id}"'
                                        + f', "userPoolDomain": "{user_pool_domain}"}}'
                                    )
                                    and ingress.metadata.annotations.get(
                                        "alb.ingress.kubernetes.io/group.name"
                                    )
                                    == ingress_group_name
                                )
                            else:
                                output = False

                        else:
                            output = False
                    else:
                        output = False
                else:
                    output = False
            else:
                output = False
        else:
            output = False
    else:
        output = False
    return output


def wait_for_host_name(
    client: NetworkingV1Api,
) -> RequiresContextIOResultE[V1Ingress, Config]:
    return RequiresContextIOResultE(
        lambda config: _wait_for_host_name(
            client=client,
            ingress_name=config.ingress_name,
            namespace=config.k8s_namespace,
        )
    )


def _wait_for_host_name(
    client: NetworkingV1Api, ingress_name: str, namespace: str
) -> IOResultE[V1Ingress]:
    _get_host_again: Callable[[bool], IOResultE[V1Ingress]] = (
        lambda is_defined: read_v1_resource(
            partial(
                client.read_namespaced_ingress,
                name=ingress_name,
                namespace=namespace,
            )
        )
        if is_defined
        else IOFailure(
            Exception("Ingress hostname still undefined after waiting period.")
        )
    )

    return flow(
        _is_hostname_defined(
            client=client,
            ingress_name=ingress_name,
            namespace=namespace,
        ),
        _get_host_again,
    )


def _is_hostname_defined(
    client: NetworkingV1Api, ingress_name: str, namespace: str
) -> bool:
    def _read_fn() -> V1Ingress:
        return client.read_namespaced_ingress(ingress_name, namespace)

    def _check_fn(ingress: V1Ingress) -> bool:
        return is_successful(get_host(ingress))

    return is_successful(status_check(_read_fn, _check_fn, 180, 5, "Ingress hostname"))


def status_check(
    read_fn: Callable[[], T],
    check_fn: Callable[[T], bool],
    duration: int,
    delay: int,
    resource_name: str = "Resource",
) -> IOResultE[bool]:
    output: IOResultE[bool]
    time.sleep(delay)  # breathing room before status checks
    tmax = time.time() + duration
    while time.time() < tmax:
        try:
            resource = read_fn()
        except Exception as e:
            output = IOFailure(e)
            break
        else:
            if check_fn(resource):
                output = IOSuccess(True)
                break
            else:
                time.sleep(delay)
    else:
        output = IOFailure(
            Exception(f"{resource_name} not available within {duration} seconds")
        )
    return output


def ping_ingress(ingress: V1Ingress) -> RequiresContextIOResultE[bool, Config]:
    return RequiresContextIOResultE(
        lambda config: _ping_ingress(ingress=ingress, lb_port=config.load_balancer_port)
    )


def _ping_ingress(ingress: V1Ingress, lb_port: int) -> IOResultE[bool]:
    is_success: Callable[[bool], IOResultE[bool]] = (
        lambda flag: IOSuccess(flag)
        if flag
        else IOFailure(Exception("Unable to ping ingress load balancer."))
    )
    return flow(
        get_host(ingress=ingress),
        map_(
            partial(
                wait_host_port, port=lb_port, duration=315, delay=5, timeout=10
            )  # time durations in seconds
        ),
        IOResultE.from_result,
        bind_ioresult(is_success),
    )


def get_host(ingress: V1Ingress) -> ResultE[str]:
    output: ResultE[str]
    # heavy None checking here to appease mypy
    if ingress.status is not None:
        if ingress.status.load_balancer is not None:
            if ingress.status.load_balancer.ingress is not None:
                if len(ingress.status.load_balancer.ingress) == 1:
                    if isinstance(
                        ingress.status.load_balancer.ingress[0].hostname, str
                    ):
                        output = Success(
                            ingress.status.load_balancer.ingress[0].hostname
                        )
                    else:
                        output = Failure(
                            Exception("Ingress load balancer hostname is not str.")
                        )
                else:
                    output = Failure(
                        Exception("Unexpected number of ingress load balancers.")
                    )
            else:
                output = Failure(Exception("Ingress load balancer list is None."))
        else:
            output = Failure(Exception("Igress load balancer status is None."))
    else:
        output = Failure(Exception("Ingress status was None."))
    return output


def wait_host_port(
    host: str, port: int, duration: int, delay: int, timeout: int
) -> bool:
    return asyncio.run(
        _wait_host_port(
            host=host, port=port, duration=duration, delay=delay, timeout=timeout
        )
    )


# taken from:
# https://gist.github.com/betrcode/0248f0fda894013382d7?permalink_comment_id=3161499#gistcomment-3161499
async def _wait_host_port(
    host: str, port: int, duration: int, delay: int, timeout: int
) -> bool:
    tmax = time.time() + duration
    while time.time() < tmax:
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=timeout
            )
            writer.close()
            await writer.wait_closed()
            return True
        except:
            await asyncio.sleep(delay)
    return False


def deploy_k8s_deployment(
    service_account: V1ServiceAccount, client: AppsV1Api
) -> RequiresContextIOResultE[V1Deployment, Config]:
    return RequiresContextIOResultE(
        lambda config: flow(
            service_account,
            get_service_account_name,
            IOResultE.from_result,
            bind_ioresult(
                partial(
                    _deploy_k8s_deployment,
                    client=client,
                    name=f"{config.app_name}-deployment",
                    namespace=config.k8s_namespace,
                    n_pods=config.n_pods,
                    app_name=config.app_name,
                    app_image=config.app_image,
                    app_port=config.app_port,
                    bucket_var_name=config.bucket_k8s_var,
                    bucket=config.data_bucket,
                    s3_prefix_var_name=config.prefix_k8s_var,
                    s3_prefix=config.data_s3_prefix,
                    container_cpu=config.container_cpu,
                    container_memory=config.container_memory,
                )
            ),
        )
    )


def _deploy_k8s_deployment(
    service_account: str,
    client: AppsV1Api,
    name: str,
    namespace: str,
    n_pods: int,
    app_name: str,
    app_image: str,
    app_port: int,
    bucket_var_name: str,
    bucket: str,
    s3_prefix_var_name: str,
    s3_prefix: str,
    container_cpu: str,
    container_memory: str,
) -> IOResultE[V1Deployment]:
    init_fn = partial(
        _init_deployment,
        name=name,
        namespace=namespace,
        n_pods=n_pods,
        app_name=app_name,
        service_account=service_account,
        app_image=app_image,
        app_port=app_port,
        bucket_var_name=bucket_var_name,
        bucket=bucket,
        s3_prefix_var_name=s3_prefix_var_name,
        s3_prefix=s3_prefix,
        container_cpu=container_cpu,
        container_memory=container_memory,
    )
    dummy_check_flag = [True]

    def _read_fn() -> V1Deployment:
        try:
            deployment = client.read_namespaced_deployment(
                name=name, namespace=namespace
            )
        except:
            raise
        else:
            dummy_check_flag[0] = False
        return deployment

    _create_fn: Callable[
        [V1Deployment], V1Deployment
    ] = lambda resource: client.create_namespaced_deployment(
        namespace=namespace, body=resource
    )
    _check_fn: Callable[[V1Deployment], bool] = lambda _: dummy_check_flag[0]
    _update_fn: Callable[
        [V1Deployment], V1Deployment
    ] = lambda resource: client.replace_namespaced_deployment(
        name=name, namespace=namespace, body=resource
    )
    return add_v1_resource(
        init_fn=init_fn,
        read_fn=_read_fn,
        create_fn=_create_fn,
        change_check_fn=_check_fn,
        update_fn=_update_fn,
    )


def _init_deployment(
    name: str,
    namespace: str,
    n_pods: int,
    app_name: str,
    service_account: str,
    app_image: str,
    app_port: int,
    bucket_var_name: str,
    bucket: str,
    s3_prefix_var_name: str,
    s3_prefix: str,
    container_cpu: str,
    container_memory: str,
):
    return V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=V1ObjectMeta(name=name, namespace=namespace),
        spec=V1DeploymentSpec(
            replicas=n_pods,
            selector=V1LabelSelector(match_labels={"app": app_name}),
            strategy=V1DeploymentStrategy(
                rolling_update=V1RollingUpdateDeployment(
                    max_unavailable=0, max_surge=n_pods
                ),  # doubles number of pods during rollout
                type="RollingUpdate",
            ),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels={"app": app_name}),
                spec=V1PodSpec(
                    service_account_name=service_account,
                    containers=[
                        V1Container(
                            name=app_name,
                            env=[
                                V1EnvVar(
                                    name="ROCKET_ADDRESS", value="0.0.0.0"
                                ),  # override rocket default of 127.0.0.1
                                V1EnvVar(name=bucket_var_name, value=bucket),
                                V1EnvVar(name=s3_prefix_var_name, value=s3_prefix),
                            ],
                            image=app_image,
                            ports=[V1ContainerPort(container_port=app_port)],
                            resources=V1ResourceRequirements(
                                requests={
                                    "cpu": container_cpu,
                                    "memory": container_memory,
                                }
                            ),
                        )
                    ],
                ),
            ),
        ),
    )


def get_service_account_name(sa: V1ServiceAccount) -> ResultE[str]:
    output: ResultE[str]
    if sa.metadata is not None:
        if sa.metadata.name is not None:
            output = Success(sa.metadata.name)
        else:
            output = Failure(Exception("Service Account name is None."))
    else:
        output = Failure(Exception("Service Account metadata is None."))
    return output


def track_k8s_deployment(
    deployment: V1Deployment,
    client: AppsV1Api,
) -> RequiresContextIOResultE[bool, Config]:
    return RequiresContextIOResultE(
        lambda config: flow(
            deployment,
            get_deployment_name,
            IOResultE.from_result,
            bind_ioresult(
                partial(
                    _track_k8s_deployment,
                    client=client,
                    namespace=config.k8s_namespace,
                )
            ),
        )
    )


def _track_k8s_deployment(
    name: str, client: AppsV1Api, namespace: str
) -> IOResultE[bool]:
    def _read_fn() -> V1Deployment:
        return client.read_namespaced_deployment(name=name, namespace=namespace)

    return status_check(
        read_fn=_read_fn,
        check_fn=_all_pods_ready,
        duration=300,
        delay=5,
        resource_name="Deployment pods",
    )


def get_deployment_name(deployment: V1Deployment) -> ResultE[str]:
    output: ResultE[str]
    if deployment.metadata is not None:
        if deployment.metadata.name is not None:
            output = Success(deployment.metadata.name)
        else:
            output = Failure(Exception("Empty deployment name."))
    else:
        output = Failure(Exception("Empty deployment metadata."))
    return output


def _all_pods_ready(deployment: V1Deployment) -> bool:
    output: bool
    if deployment.status is not None and deployment.spec is not None:
        if (
            deployment.status.replicas is not None
            and deployment.spec.replicas is not None
        ):
            if deployment.status.unavailable_replicas is not None:
                output = (
                    deployment.status.unavailable_replicas == 0
                    and deployment.status.replicas == deployment.spec.replicas
                )
            else:
                output = deployment.status.replicas == deployment.spec.replicas
        else:
            output = False
    else:
        output = False
    return output


def main() -> None:
    # environment variables can be set by codebuild for CD
    config = Config(
        data_bucket=os.environ["ENV_BUCKET_WITH_JSON"],
        data_s3_prefix=os.environ["ENV_JSON_S3_KEY_PREFIX"],
        cluster_name=os.environ["ENV_K8S_CLUSTER_NAME"],
        cluster_role_arn=os.environ["ENV_K8S_ROLE_ARN"],
        bucket_k8s_var="ENV_BUCKET_NAME",
        prefix_k8s_var="ENV_S3_KEY_PREFIX",
        aws_region=os.environ["ENV_AWS_REGION"],
        k8s_namespace=os.environ["ENV_K8S_NAMESPACE"],
        sa_role_arn=os.environ["ENV_POD_SA_ROLE_ARN"],
        app_name=os.environ["ENV_API_APP_NAME"],
        app_port=8000,  # rocket default
        subnet_a=os.environ["ENV_PRIVATE_SUBNET_A"],
        subnet_b=os.environ["ENV_PRIVATE_SUBNET_B"],
        cert_arn=os.environ["ENV_CERT_ARN"],
        site_node_port=os.environ["ENV_YEW_NODE_PORT_NAME"],  # not used, TODO: remove
        site_app_port=int(
            os.environ["ENV_YEW_NODE_PORT_PORT"]
        ),  # not used, TODO: remove
        n_pods=os.environ["ENV_NUM_PODS"],
        app_image=os.environ["ENV_ECR_APP_IMAGE"],
        container_cpu="500m",  # in millicores
        container_memory="950Mi",  # in Mebibytes
        load_balancer_port=443,  # lb should be over TLS
        service_account_name=os.environ["ENV_K8S_SERVICE_ACCOUNT_NAME"],
        ingress_name="styeward-api-ingress",
        user_pool_arn=os.environ["ENV_USERPOOL_ARN"],
        user_pool_client_id=os.environ["ENV_USERPOOL_CLIENT"],
        user_pool_domain=os.environ["ENV_USERPOOL_DOMAIN"],
        ingress_group_name=os.environ["ENV_INGRESS_GROUP_NAME"],
    )
    # below lines will raise exceptions despite FP pattern to fail CD
    flow(configure_k8s_client(), alt(raise_exception))(config)
    k8s_config.load_kube_config()  # type: ignore
    corev1_client = k8s_client.CoreV1Api()
    networkv1_client = k8s_client.NetworkingV1Api()
    appsv1_client = k8s_client.AppsV1Api()
    flow(
        deploy_service_account(client=corev1_client),
        bind_context_ioresult(partial(deploy_k8s_deployment, client=appsv1_client)),
        bind_context_ioresult(partial(track_k8s_deployment, client=appsv1_client)),
        alt(raise_exception),
    )(config)
    hostname_waiter: Callable[
        [V1Ingress], RequiresContextIOResultE[V1Ingress, Config]
    ] = lambda _: wait_for_host_name(networkv1_client)
    flow(
        deploy_node_port(client=corev1_client),
        bind_context_ioresult(partial(deploy_ingress, client=networkv1_client)),
        bind_context_ioresult(hostname_waiter),
        bind_context_ioresult(ping_ingress),
        alt(raise_exception),
    )(config)


if __name__ == "__main__":
    main()
