import logging
from datetime import datetime, timezone
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


def get_current_namespace():
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "default"


def restart_deployment(deployment_name):
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    except Exception as e:
        logger.exception("Unable to find config for kubernetes cluster: %s", e)
        return False
    apps_v1_api = client.AppsV1Api()
    namespace = get_current_namespace()
    now = datetime.now(timezone.utc).isoformat()
    patch_body = {
        "spec": {
            "template": {
                "metadata": {"annotations": {"kubectl.kubernetes.io/restartedAt": now}}
            }
        }
    }

    try:
        response = apps_v1_api.patch_namespaced_deployment(
            name=deployment_name, namespace=namespace, body=patch_body
        )
        logger.info(f"Rollout restart triggered for deployment '{deployment_name}'.")
        return True
    except ApiException as ex:
        logger.exception("Exception while restarting deployment: {e}")

    return False
