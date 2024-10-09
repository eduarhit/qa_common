import logging as log


class Cluster:
    """Class for Cluster objects in which cluster operations can be applied.
    """
    def __init__(self, cfg=None):
        self.cfg = cfg

    def collect_diags(self, test_name):
        """ diagnostic collection, used by a couple of fixtures
        """
        if test_name == "sessionfinish":
            log.info('diags collection requested on exit (pytest sessionfinish)')
        else:
            log.info(f'diags collection requested for test: {test_name}')

        self.download_cluster_diags(test_name)

    def download_cluster_diags(self, test):
        """ Download diags for a cluster in parallel

        N.B. If we have a hydra cluster class, this could become a method for that class

        For now, if we get passed a GFS style cluster_cfg dictionary, then we'll use that,
        if that is not passed in, if we have a cluster config in test_env.json, we'll use that,
        otherwise we'll assume that we're running a UI test or whatever, and there is no cluster,
        so we just do nothing and return

        :param str test: name of the test related subdirectory used to store the diags
        :param dict cluster_cfg: an optional cluster configuration object
        :return: always returns good since we don't see what the thread function returns
        :rtype: bool
        """
        # node_ips = []
        # for node in [self.cfg["leader"]] + self.cfg["followers"]:
        #     node_ips.append(node["ip"])
        # username = self.cfg["node_username"]
        # password = self.cfg["node_password"]
        #
        #
        log.info(f"diag downloads starting for test {test}")
        #
        # # kick off all the download threads
        # threads = []
        # for ip in node_ips:
        #     thread = Thread(target=download_diags, args=(ip, test, username, password))
        #     threads.append(thread)
        #     thread.start()
        #
        # # wait for all the downloads to complete
        # for thread in threads:
        #     thread.join()
        #
        # # log and return
        # log.info(f"{len(threads)} diag files downloaded for test {test}")
        # return True