"""
updating cache refresh in gluu server
Author : Mohammad Abudayyeh
"""
import logging
import logging.config
import os
import time

from ldap3 import Server, Connection, MODIFY_REPLACE

from pygluu.containerlib import get_manager
from pygluu.containerlib.utils import decode_text
from pygluu.containerlib.persistence.couchbase import get_couchbase_user
from pygluu.containerlib.persistence.couchbase import get_couchbase_password
from pygluu.containerlib.persistence.couchbase import CouchbaseClient
from pygluu.containerlib.meta import DockerMeta
from pygluu.containerlib.meta import KubernetesMeta

from settings import LOGGING_CONFIG

SIGNAL_IP = '999.888.999.777'
DEFAULT_IP = '255.255.255.255'

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("entrypoint")


def clean_snapshot(metaclient, container):
    metaclient.exec_cmd(container, "rm -rf /var/ox/identity/cr-snapshots")
    metaclient.exec_cmd(container, "mkdir -p /var/ox/identity/cr-snapshots")


class BaseBackend(object):
    def get_configuration(self):
        raise NotImplementedError

    def update_configuration(self):
        raise NotImplementedError


class LDAPBackend(BaseBackend):
    def __init__(self, host, user, password):
        ldap_server = Server(host, port=1636, use_ssl=True)
        self.backend = Connection(ldap_server, user, password)

    def get_configuration(self):
        with self.backend as conn:
            conn.search(
                "ou=configuration,o=gluu",
                '(objectclass=gluuConfiguration)',
                attributes=['oxTrustCacheRefreshServerIpAddress',
                            'gluuVdsCacheRefreshEnabled'],
            )

            if not conn.entries:
                return {}

            entry = conn.entries[0]
            config = {
                "id": entry.entry_dn,
                "oxTrustCacheRefreshServerIpAddress": entry["oxTrustCacheRefreshServerIpAddress"][0],
                "gluuVdsCacheRefreshEnabled": entry["gluuVdsCacheRefreshEnabled"][0],
            }
            return config

    def update_configuration(self, id_, ip):
        with self.backend as conn:
            conn.modify(
                id_,
                {'oxTrustCacheRefreshServerIpAddress': [(MODIFY_REPLACE, [ip])]}
            )
            result = {
                "success": conn.result["description"] == "success",
                "message": conn.result["message"],
            }
            return result


class CouchbaseBackend(BaseBackend):
    def __init__(self, host, user, password):
        self.backend = CouchbaseClient(host, user, password)

    def get_configuration(self):
        bucket_prefix = os.environ.get("GLUU_COUCHBASE_BUCKET_PREFIX", "gluu")
        req = self.backend.exec_query(
            "SELECT oxTrustCacheRefreshServerIpAddress, gluuVdsCacheRefreshEnabled "
            f"FROM `{bucket_prefix}` "
            "USE KEYS 'configuration'"
        )

        if not req.ok:
            return {}

        config = req.json()["results"][0]

        if not config:
            return {}

        config.update({"id": "configuration"})
        return config

    def update_configuration(self, id_, ip):
        bucket_prefix = os.environ.get("GLUU_COUCHBASE_BUCKET_PREFIX", "gluu")
        req = self.backend.exec_query(
            f"UPDATE `{bucket_prefix}` "
            "USE KEYS '{0}' "
            "SET oxTrustCacheRefreshServerIpAddress='{1}' "
            "RETURNING oxTrustCacheRefreshServerIpAddress".format(id_, ip)
        )

        result = {
            "success": req.ok,
            "message": req.text,
        }
        return result


class CacheRefreshRotator(object):
    def __init__(self, manager, persistence_type, ldap_mapping="default"):
        if persistence_type in ("ldap", "couchbase"):
            backend_type = persistence_type
        else:
            # persistence_type is hybrid
            if ldap_mapping == "default":
                backend_type = "ldap"
            else:
                backend_type = "couchbase"

        # resolve backend
        if backend_type == "ldap":
            host = os.environ.get("GLUU_LDAP_URL", "localhost:1636")
            user = manager.config.get("ldap_binddn")
            password = decode_text(
                manager.secret.get("encoded_ox_ldap_pw"),
                manager.secret.get("encoded_salt"),
            )
            backend_cls = LDAPBackend
        else:
            host = os.environ.get("GLUU_COUCHBASE_URL", "localhost")
            user = get_couchbase_user(manager)
            password = get_couchbase_password(manager)
            backend_cls = CouchbaseBackend

        self.backend = backend_cls(host, user, password)
        self.manager = manager

    def send_signal(self):
        try:
            config = self.backend.get_configuration()

            logger.info("No oxtrust containers found on this node. Provisioning other oxtrust containers at other nodes...")
            req = self.backend.update_configuration(config["id"], SIGNAL_IP)

            if req["success"]:
                check_ip = config.get("oxTrustCacheRefreshServerIpAddress", DEFAULT_IP)
                logger.info("Signal has been sent")
                logger.info("Waiting for response...It may take up to 5 mins")
                process_time = 0
                check = False
                starttime = time.time()

                while not check:
                    config = self.backend.get_configuration()
                    check_ip = config.get("oxTrustCacheRefreshServerIpAddress", DEFAULT_IP)
                    endtime = time.time()
                    process_time = endtime - starttime

                    if check_ip != SIGNAL_IP or round(process_time) > 300.0:
                        check = True

                    time.sleep(5)

                if check_ip == SIGNAL_IP:
                    # No nodes found . Reset to default
                    req = self.backend.update_configuration(config["id"], DEFAULT_IP)
                    if req["success"]:
                        logger.info("No nodes found.Cache Refresh updated ip to default. Please add oxtrust containers")
                    else:
                        logger.warning("Unable to update CacheRefresh to defaults; reason={}".format(req["message"]))
                else:
                    logger.info("Oxtrust containers found at other nodes. Cache Refresh has been updated")
            else:
                logger.warning("Unable to send signal; reason={}".format(req["message"]))
        except Exception as e:
            logger.warning("Unable to update CacheRefresh config; reason={}".format(e))


def write_master_ip(ip):
    with open('/cr/ip_file.txt', 'w+') as ip_file:
        ip_file.write(str(ip))


def check_master_ip(ip):
    with open('/cr/ip_file.txt', 'a+') as ip_file:
        ip_master = ip_file.read().strip()
    if str(ip) in ip_master:
        return True
    return False


def main():
    GLUU_CONTAINER_METADATA = os.environ.get("GLUU_CONTAINER_METADATA", "docker")  # noqa: N806

    # check interval (by default per 5 mins)
    GLUU_CR_ROTATION_CHECK = os.environ.get("GLUU_CR_ROTATION_CHECK", 60 * 5)  # noqa: N806

    try:
        check_interval = int(GLUU_CR_ROTATION_CHECK)
    except ValueError:
        check_interval = 60 * 5

    manager = get_manager()

    persistence_type = os.environ.get("GLUU_PERSISTENCE_TYPE", "ldap")
    ldap_mapping = os.environ.get("GLUU_PERSISTENCE_LDAP_MAPPING", "default")

    rotator = CacheRefreshRotator(manager, persistence_type, ldap_mapping)

    if GLUU_CONTAINER_METADATA == "kubernetes":
        client = KubernetesMeta()
    else:
        client = DockerMeta()

    try:
        while True:
            oxtrust_containers = client.get_containers("APP_NAME=oxtrust")
            oxtrust_ip_pool = [client.get_container_ip(container) for container in oxtrust_containers]
            signalon = False

            config = rotator.backend.get_configuration()
            current_ip_in_ldap = config.get("oxTrustCacheRefreshServerIpAddress", DEFAULT_IP)
            is_cr_enabled = config["gluuVdsCacheRefreshEnabled"] in ("enabled", True)
            # is_cr_enabled = True

            if current_ip_in_ldap in oxtrust_ip_pool and is_cr_enabled:
                write_master_ip(current_ip_in_ldap)
            else:
                signalon = True

            if check_master_ip(current_ip_in_ldap) and oxtrust_containers:
                signalon = True

            if current_ip_in_ldap == SIGNAL_IP:
                logger.info("Signal received. Setting new oxtrust container at this node to CacheRefresh ")
                signalon = True

            if not oxtrust_containers and is_cr_enabled:
                rotator.send_signal()

            # If no oxtrust was found the previous would set ip to default. If later oxtrust was found
            if current_ip_in_ldap == DEFAULT_IP and is_cr_enabled and oxtrust_containers:
                logger.info("Oxtrust containers found after resetting to defaults.")
                signalon = True

            for container in oxtrust_containers:
                ip = client.get_container_ip(container)

                # The user has disabled the CR or CR is not active
                if not is_cr_enabled:
                    logger.warning('Cache refresh is found to be disabled.')

                config = rotator.backend.get_configuration()
                current_ip_in_ldap = config.get("oxTrustCacheRefreshServerIpAddress", DEFAULT_IP)
                is_cr_enabled = config["gluuVdsCacheRefreshEnabled"] in ("enabled", True)
                # is_cr_enabled = True

                # Check  the container has not been setup previously, the CR is enabled
                if ip != current_ip_in_ldap and is_cr_enabled and current_ip_in_ldap not in oxtrust_ip_pool \
                        and signalon:
                    logger.info("Current oxTrustCacheRefreshServerIpAddress: {}".format(current_ip_in_ldap))

                    # Clean cache folder at oxtrust container
                    logger.info(
                        "Cleaning cache folders for {} with IP {}".format(
                            client.get_container_name(container), client.get_container_ip(container)
                        )
                    )
                    clean_snapshot(client, container)

                    logger.info("Updating oxTrustCacheRefreshServerIpAddress to IP address {}".format(ip))
                    req = rotator.backend.update_configuration(config["id"], ip)
                    if req["success"]:
                        logger.info("CacheRefresh config has been updated")
                    else:
                        logger.warning("Unable to update CacheRefresh config; reason={}".format(req["message"]))

            # delay
            time.sleep(check_interval)
    except KeyboardInterrupt:
        logger.warning("Canceled by user; exiting ...")


if __name__ == "__main__":
    main()
