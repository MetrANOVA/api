import asyncio
import os
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

_core_v1_api: client.CoreV1Api | None = None


async def get_core_v1_api() -> client.CoreV1Api:
    """Lazily build and cache a CoreV1Api client for the process lifetime.

    Tries in-cluster credentials first (the pod's mounted ServiceAccount),
    falling back to the local kubeconfig for dev/test environments.
    """

    METRANOVA_KUBE_NAMESPACE = os.getenv("METRANOVA_KUBE_NAMESPACE", "metranova-test")
    METRANOVA_KUBE_CLUSTER_ID = os.getenv("METRANOVA_KUBE_CLUSTER_ID", "local")
    METRANOVA_KUBE_URL = os.getenv("METRANOVA_KUBE_URL", "http://localhost")
    METRANOVA_KUBE_BEARER_TOKEN = os.getenv("METRANOVA_KUBE_BEARER_TOKEN", None)

    global _core_v1_api
    if _core_v1_api is None:
        try:
            config.load_incluster_config()
            _core_v1_api = client.CoreV1Api()
        except config.ConfigException:
            kubeconfig = {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {
                        "name": "rancher",
                        "cluster": {
                            "server": f"{METRANOVA_KUBE_URL}/k8s/clusters/{METRANOVA_KUBE_CLUSTER_ID}",
                            "insecure-skip-tls-verify": True,  # swap for certificate-authority-data if you have the CA
                        },
                    }
                ],
                "users": [
                    {
                        "name": "rancher-user",
                        "user": {"token": METRANOVA_KUBE_BEARER_TOKEN},
                    }
                ],
                "contexts": [
                    {
                        "name": "rancher-context",
                        "context": {"cluster": "rancher", "user": "rancher-user"},
                    }
                ],
                "current-context": "rancher-context",
            }

            await config.load_kube_config_from_dict(kubeconfig)

            api_client = client.ApiClient()
            _core_v1_api = client.CoreV1Api(api_client)
    return _core_v1_api


async def test_kube_client():
    """Test the Kubernetes client by listing namespaces."""
    await get_core_v1_api()


# asyncio.run(test_kube_client())

__all__ = ["get_core_v1_api", "ApiException"]
