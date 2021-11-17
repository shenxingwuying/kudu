#!/bin/env python
# -*- coding: UTF-8 -*-
import json
import os
import sys
import urllib.request
import urllib.parse
import traceback
import logging

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from utils import sa_utils

root_logger = logging.getLogger()


def ignore_exception(fun):
    def wrapper(*args, **argv):
        ret = None
        try:
            ret = fun(*args, **argv)
        except Exception:
            root_logger.debug('ignore exception')
            root_logger.debug(traceback.format_exc())
        return ret
    return wrapper


class CdhConfigCommon():

    def __init__(self, root_url, user, passwd, logger=None):
        self.logger = sa_utils.init_tmp_logger('config_cdh') if not logger else logger
        self.root_url = root_url
        self.__install_auth(user, passwd)
        self.role_groups = {}

    def __install_auth(self, user, passwd):
        '''用户名密码验证'''
        auth_handler = urllib.request.HTTPBasicAuthHandler()
        auth_handler.add_password(
            realm='Cloudera Manager',
            uri=self.root_url,
            user=user,
            passwd=passwd)
        opener = urllib.request.build_opener(auth_handler)
        urllib.request.install_opener(opener)

    def __get(self, path):
        url = '%s%s' % (self.root_url, path)
        self.logger.debug('GET: url = %s' % url)
        with urllib.request.urlopen(url) as f:
            response = json.loads(f.read().decode('utf8'))
        self.logger.debug('response: %s' % response)
        return response

    def __put(self, path, data):
        url = '%s%s' % (self.root_url, path)
        self.logger.debug('PUT: url = %s\n\tdata=%s' % (url, data))
        headers = {'content-type': 'application/json'}
        req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers=headers, method='PUT')
        with urllib.request.urlopen(req) as f:
            response = json.loads(f.read().decode('utf8'))
        self.logger.debug('response: %s' % response)
        return response

    def get_value(self, path, name):
        response = self.__get('%s?view=full' % path)
        value = [x for x in response['items'] if x['name'] == name]
        if not value:
            raise Exception('cannot find %s value!' % name)
        self.logger.debug('value=%s' % value)
        return value[0].get('value', value[0].get('default', None))

    def put_if_needed(self, path, name, value, key):
        # 1. 查看旧值
        old_value = self.get_value(path, name)
        # 2. 如果不同则修改
        if old_value == value:
            self.logger.info('skip changed %s %s->%s' % (key, name, value))
        else:
            self.__put(path, {'items': [{'name': name, 'value': value}]})
            self.logger.info('changed %s %s->%s' % (key, name, value))

    def check(self, path, name, value, key):
        # 1. 查看旧值
        old_value = self.get_value(path, name)
        # 2. 如果不同则修改
        if old_value == value:
            self.logger.info('The value of config [%s %s] is already set to [%s]' % (key, name, value))
        else:
            self.logger.warn('The value of config [%s %s] is [%s] which is different from target value [%s]' %
                             (key, name, old_value, value))

    @ignore_exception
    def get_role_groups(self, service, role_type, path):
        '''cloudera的一个角色可能有多个配置组'''
        if service not in self.role_groups:
            response = self.__get(path)
            ret = {}
            for item in response['items']:
                if item['roleType'].lower() not in ret:
                    ret[item['roleType'].lower()] = []
                ret[item['roleType'].lower()].append(item['name'])
            self.role_groups[service] = ret
            self.logger.debug('add %s: %s' % (service, ret))
        return self.role_groups[service][role_type.lower()]

    @ignore_exception
    def get_roles(self, service, role_type, path):
        '''cloudera的一个角色类型可能有多个角色示例'''
        if service not in self.role_groups:
            response = self.__get(path)
            ret = {}
            for item in response['items']:
                if item['type'].lower() not in ret:
                    ret[item['type'].lower()] = []
                ret[item['type'].lower()].append(item['name'])
            self.role_groups[service] = {}
            self.role_groups[service]['roles_conf'] = ret
            self.logger.debug('add %s: %s' % (service, ret))
        return self.role_groups[service]['roles_conf'][role_type.lower()]
