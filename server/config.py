# -*- coding: utf-8 -*-
# deprecated

import tempfile

class Config(object):
    DEBUG = False
    TESTING = False
    DD_REPOSITORY_BASE = 'test_repo'

class DevelopmentConfig(Config):
    DEBUG = True

class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    DD_REPOSITORY_BASE = tempfile.mkdtemp()
