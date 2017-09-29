"""
Backend for django cache
"""
import socket
from functools import wraps
from django.core.cache import InvalidCacheBackendError
from django.core.cache.backends.memcached import PyLibMCCache
from .cluster_utils import get_cluster_info
import logging


def retry(f):
    """
    catch any exception and retry that given call if needed. pylibmc raises
    errors on unsuccessful writes (e.g. it's state transitions) so we need try
    writes again to reach the expected state of the cluster
    """
    @wraps(f)
    def wrapper(self, *args, **kwds):
        max_tries = getattr(self, 'call_retry_count', 1)
        for i in range(max_tries):
            try:
                return f(self, *args, **kwds)
            except Exception as e:
                logging.warning(
                    'pymc operation failed: {0} (try={1})'.format(e, i+1))
                if max_tries-1 == i:
                    raise e

    return wrapper


def invalidate_cache_after_error(f):
    """
    catch any exception and invalidate internal cache with list of nodes
    """
    @wraps(f)
    def wrapper(self, *args, **kwds):
        try:
            return f(self, *args, **kwds)
        except Exception:
            self.clear_cluster_nodes_cache()
            raise
    return wrapper


class ElastiCache(PyLibMCCache):
    """
    backend for Amazon ElastiCache (memcached) with auto discovery mode
    it used pylibmc in binary mode
    """
    def __init__(self, server, params):
        self.update_params(params)

        self.call_retry_count = self._get_class_param(params, 'CALL_RETRY_COUNT', 1)
        if self.call_retry_count < 1:
            raise Warning('CALL_RETRY_COUNT must be >= 1')

        super(ElastiCache, self).__init__(server, params)
        if len(self._servers) > 1:
            raise InvalidCacheBackendError(
                'ElastiCache should be configured with only one server '
                '(Configuration Endpoint)')
        if len(self._servers[0].split(':')) != 2:
            raise InvalidCacheBackendError(
                'Server configuration should be in format IP:port')

        self._ignore_cluster_errors = self._options.get(
            'IGNORE_CLUSTER_ERRORS', False)

    def _get_class_param(self, params, name, default):
        """
        get an item from the param array, and delete it from there
        so that it does not propagate as a behaviour
        """
        if name in params:
            value = params[name]
            del params[name]
            return value
        else:
            return default

    def update_params(self, params):
        """
        update connection params to maximize performance
        """
        if not params.get('BINARY', True):
            raise Warning('To increase performance please use ElastiCache'
                          ' in binary mode')
        else:
            params['BINARY'] = True  # patch params, set binary mode
        if 'OPTIONS' not in params:
            # set special 'behaviors' pylibmc attributes
            params['OPTIONS'] = {
                'tcp_nodelay': True,
                'ketama': True
            }

    def clear_cluster_nodes_cache(self):
        """clear internal cache with list of nodes in cluster"""
        if hasattr(self, '_cluster_nodes_cache'):
            del self._cluster_nodes_cache

    def get_cluster_nodes(self):
        """
        return list with all nodes in cluster
        """
        if not hasattr(self, '_cluster_nodes_cache'):
            server, port = self._servers[0].split(':')
            try:
                self._cluster_nodes_cache = (
                    get_cluster_info(server, port,
                                     self._ignore_cluster_errors)['nodes'])
            except (socket.gaierror, socket.timeout) as err:
                raise Exception('Cannot connect to cluster {0} ({1})'.format(
                    self._servers[0], err
                ))
        return self._cluster_nodes_cache

    @property
    def _cache(self):
        # PylibMC uses cache options as the 'behaviors' attribute.
        # It also needs to use threadlocals, because some versions of
        # PylibMC don't play well with the GIL.

        # instance to store cached version of client
        # in Django 1.7 use self
        # in Django < 1.7 use thread local
        container = getattr(self, '_local', self)
        client = getattr(container, '_client', None)
        if client:
            return client

        client = self._lib.Client(self.get_cluster_nodes())
        if self._options:
            # In Django 1.11, all behaviors are shifted into a behaviors dict
            # Attempt to get from there, and fall back to old behavior if the
            # behaviors key does not exist
            client.behaviors = self._options.get('behaviors', self._options)

        container._client = client

        return client

    @retry
    @invalidate_cache_after_error
    def get(self, *args, **kwargs):
        return super(ElastiCache, self).get(*args, **kwargs)

    @retry
    @invalidate_cache_after_error
    def get_many(self, *args, **kwargs):
        return super(ElastiCache, self).get_many(*args, **kwargs)

    @retry
    @invalidate_cache_after_error
    def set(self, *args, **kwargs):
        return super(ElastiCache, self).set(*args, **kwargs)

    @retry
    @invalidate_cache_after_error
    def set_many(self, *args, **kwargs):
        return super(ElastiCache, self).set_many(*args, **kwargs)

    @retry
    @invalidate_cache_after_error
    def delete(self, *args, **kwargs):
        return super(ElastiCache, self).delete(*args, **kwargs)
