#
# This file is part of the zkfarmer package.
# (c) Olivier Poitrey <rs@dailymotion.com>
#
# For the full copyright and license information, please view the LICENSE
# file that was distributed with this source code.

import logging
import threading
from socket import socket, gethostname, AF_INET, SOCK_DGRAM

from zookeeper import EPHEMERAL, NoNodeException, ConnectionLossException
import zc.zk
from watchdog.observers import Observer

from .utils import serialize, unserialize

import logging
logger = logging.getLogger(__name__)

class ZkFarmWatcher(object):
    def __init__(self):
        self.cv = threading.Condition()
        self.handled = False

    def wait(self):
        while True:
            self.handled = False
            self.cv.wait(60)
            if self.handled:
                break

    def notify(self):
        self.handled = True
        self.cv.notify_all()


class ZkFarmExporter(ZkFarmWatcher):
    def __init__(self, zkconn, root_node_path, conf, updated_handler=None, filter_handler=None):
        super(ZkFarmExporter, self).__init__()
        self.watched_paths = {}
        self.zkconn = zkconn

        node_names = self.zkconn.children(root_node_path)
        node_names(self.node_watcher)
        while True:
            with self.cv:
                new_conf = {}
                logger.debug("looking for changes in %s" % root_node_path)
                for name in node_names:
                    subnode_path = '%s/%s' % (root_node_path, name)
                    info = dict(self.zkconn.properties(subnode_path))
                    if not filter_handler or filter_handler(info):
                        new_conf[name] = info
                logger.debug("writing back changes")
                conf.write(new_conf)
                if updated_handler:
                    updated_handler()
                self.wait()

    def node_watcher(self, children=None):
        if children is None:
            # Will be handled by the parent
            return
        with self.cv:
            logger.debug("adding or deleting node in %r" % children)
            paths = []
            for c in children:
                paths.append("%s/%s" % (children.path, c))
            # Remove children that don't exist anymore
            rc = 0
            for path in self.watched_paths.keys():
                if not path.startswith("%s/" % children.path):
                    continue
                if path not in paths:
                    logger.debug("removing path %s from watched paths" % path)
                    del self.watched_paths[path]
                    rc += 1
            if rc != 0:
                logger.debug("removed %d nodes" % rc)

            # Add new children
            rc = 0
            for path in paths:
                if path not in self.watched_paths:
                    logger.debug("adding new path %s to watched paths" % path)
                    children = self.zkconn.children(path)
                    children(self.node_watcher)
                    properties = self.zkconn.properties(path)
                    properties(self.property_watcher)
                    self.watched_paths[path] = (children, properties)
            if rc != 0:
                logger.debug("added %d nodes" % rc)
            # Notify everyone of updates
            self.notify()

    def property_watcher(self, property=None):
        if property is None:
            # Will be handled by the parent
            return
        with self.cv:
            logger.debug("modification of node %r" % property)
            self.notify()

class ZkFarmJoiner(ZkFarmWatcher):
    def __init__(self, zkconn, root_node_path, conf):
        super(ZkFarmJoiner, self).__init__()
        self.update_remote_timer = None
        self.update_local_timer = None

        self.zkconn = zkconn
        self.conf = conf
        self.node_path = '%s/%s' % (root_node_path, self.myip())

        # force the hostname info key
        info = conf.read()
        info['hostname'] = gethostname()
        conf.write(info)

        zkconn.create(self.node_path, serialize(conf.read()), zc.zk.OPEN_ACL_UNSAFE, EPHEMERAL)

        observer = Observer()
        observer.schedule(self, path=conf.file_path, recursive=True)
        observer.start()

        zkconn.get(self.node_path, self.node_watcher)

        while True:
            with self.cv:
                self.wait()

    def dispatch(self, event):
        with self.cv:
            try:
                current_conf = unserialize(self.zkconn.get(self.node_path)[0])
                new_conf = self.conf.read()
                if current_conf != new_conf:
                    logging.info('Local conf changed')
                    self.zkconn.set(self.node_path, serialize(new_conf))
            except ConnectionLossException:
                pass
            self.notify()

    def node_watcher(self, handle, type, state, path):
        with self.cv:
            current_conf = self.conf.read()
            new_conf = unserialize(self.zkconn.get(self.node_path, self.node_watcher)[0])
            if current_conf != new_conf:
                logging.info('Remote conf changed')
                self.conf.write(new_conf)
            self.notify()

    def myip(self):
        # Try to find default IP
        ip = None
        s = socket(AF_INET, SOCK_DGRAM)
        try:
            s.connect(('239.255.0.0', 9))
            ip = s.getsockname()[0]
        except socket.error:
            logging.error("Cannot determine host IP")
            exit(1)
        finally:
            del s
        return ip
