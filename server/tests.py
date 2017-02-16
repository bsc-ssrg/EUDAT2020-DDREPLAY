#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###########################################################################
#  (C) Copyright 2016-2017 Barcelona Supercomputing Center                #
#                     Centro Nacional de Supercomputacion                 #
#                                                                         #
#  This file is part of the Dataset Replayer.                             #
#                                                                         #
#  See AUTHORS file in the top level directory for information            #
#  regarding developers and contributors.                                 #
#                                                                         #
#  This package is free software; you can redistribute it and/or          #
#  modify it under the terms of the GNU Lesser General Public             #
#  License as published by the Free Software Foundation; either           #
#  version 3 of the License, or (at your option) any later version.       #
#                                                                         #
#  The Dataset Replayer is distributed in the hope that it will           #
#  be useful, but WITHOUT ANY WARRANTY; without even the implied          #
#  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR                #
#  PURPOSE.  See the GNU Lesser General Public License for more           #
#  details.                                                               #
#                                                                         #
#  You should have received a copy of the GNU Lesser General Public       #
#  License along with Echo Filesystem NG; if not, write to the Free       #
#  Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.     #
#                                                                         #
###########################################################################


import os
import unittest
import tempfile
import json
import shutil
import io
import tarfile
import warnings
import random

from pprint import pprint
from collections import OrderedDict
from werkzeug.utils import secure_filename

# for the tests to use a temporary repository, FLASK_CONFIGURATION
# needs to be set to 'testing' BEFORE importing the app
os.environ['FLASK_CONFIGURATION'] = 'testing'
from ddreplay import app, set_repository
from storage.repository import Repository
from storage.backends.filesystem import DraftSchema

# disable flask internal logging
import logging
from flask import logging as flask_logging
def mock_create_logger(app):
    return logging.getLogger(app.logger_name)

flask_logging.create_logger = mock_create_logger

################################################################################
##### Helpers                                                              #####
################################################################################

def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)
    return do_test

def extract_info(elem):
    if isinstance(elem, dict):
        yield elem['path'], (elem['type'], elem['name'])
        if elem['type'] == 'directory':
            for c in elem['children']:
                for e in extract_info(c):
                    yield e

def compare_draft_contents(actual, expected):

    # print("--------")
    # pprint(actual)
    # pprint(expected)
    # print("--------")

    assert(len(actual) == len(expected))

    expected_elems = dict()

    for elem in expected:
        for k,v, in extract_info(elem):
            expected_elems[k] = v

    actual_elems = dict()

    for elem in actual:
        for k,v in extract_info(elem):
            actual_elems[k] = v


    sorted_exp_elems = OrderedDict(sorted(expected_elems.items()))
    sorted_act_elems = OrderedDict(sorted(actual_elems.items()))

    #pprint(sorted_exp_elems)
    #print('--')
    #pprint(sorted_act_elems)

    assert(sorted_exp_elems == sorted_act_elems)

def json_response(response, code=200):
    """ checks that the status code equals 'code' and returns the json """

    assert(response.status_code == code)
    return json.loads(response.get_data(as_text=True))

def validate_draft_json(json_data, exp_id=None, exp_contents=None):
    """ validates the fields in a draft using the DraftSchema validator """

    assert('draft' in json_data)
    _, errors = DraftSchema().load(json_data['draft'])
    assert(errors == {})

    if exp_id is not None:
        assert(json_data['draft']['id'] == exp_id)

    if exp_contents is not None:
        compare_draft_contents(json_data['draft']['contents'], exp_contents)

def path_to_dict(rootdir):

    # inner function to generate subtrees
    def subtree_to_dict(path):

        relpath = os.path.relpath(path, rootdir)

        subtree = OrderedDict([
            ('name', os.path.basename(path)),
            ('path', relpath)
        ])

        if(os.path.isdir(path)):
            subtree['type'] = 'directory'
            subtree['children'] = [ subtree_to_dict(os.path.join(path, dir_entries)) 
                                        for dir_entries in os.listdir(path) ]
        else:
            subtree['type'] = 'file'

        return subtree

    # for later convenience, the first level should just be a list
    tree = []
    for entry in sorted(os.listdir(rootdir)):
        subtree = subtree_to_dict(os.path.join(rootdir, entry))

        tree.append(subtree)

    return tree

def verify_repository(repo, draft_id, exp_contents):

    def verify_file(name, path):
        assert(name == os.path.basename(path))
        repo_base = repo.backend.base_location
        exp_path = os.path.join(repo_base, 'drafts', 'data', draft_id, path)

        assert(os.path.exists(exp_path))
        assert(os.path.isfile(exp_path))

    def verify_directory():
        pass

    for entry in exp_contents:
        #pprint(entry)
        if entry['type'] == 'file':
            verify_file(entry['name'], entry['path'])
        if entry['type'] == 'directory':
            verify_directory()
    pass

def generate_test_file(min_size_kb, max_size_kb):

    size_kb = random.uniform(min_size_kb, max_size_kb)

    filename = tempfile.mktemp()
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(int(size_kb*1024)))

    return filename

def create_empty_draft(app):
    response = app.post('/api/v1.1/drafts/')

    resp = json_response(response, 201)
    validate_draft_json(resp, exp_contents=[])

    return resp

def get_draft(app, draft_id, exp_contents):
    response = app.get('/api/v1.1/drafts/' + draft_id + '/record')
    resp = json_response(response, 200)

    validate_draft_json(resp, draft_id, exp_contents)

    return resp

def upload_file(app, draft_id, test_data=None, unpack=False, overwrite=False):

    if unpack:
        unpackarg = "true"
    else:
        unpackarg = "false"

    if overwrite:
        overwritearg = "true"
    else:
        overwritearg = "false"

    if test_data is None:
        response = app.put('/api/v1.1/drafts/' + draft_id)
    elif test_data == '':
        test_filename = ''
        response = app.put('/api/v1.1/drafts/' + draft_id,
                        content_type='multipart/form-data',
                        data={
                            'payload' : (io.BytesIO(b''), '')
                        }, follow_redirects=False)
    else:
        assert(os.path.isfile(test_data))
        test_filename = os.path.basename(test_data)
        response = app.put('/api/v1.1/drafts/' + draft_id + 
                '?unpack=' + unpackarg + '&overwrite=' + overwritearg,
                        content_type='multipart/form-data',
                        data={
                            'payload' : (test_data, test_filename)
                        }, follow_redirects=False)

    return response


################################################################################
##### Tests                                                                #####
################################################################################

class EmptyDraftTest(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()

        # create a temporary Repository for the test
        repo_location = tempfile.mkdtemp(prefix='tmp_repo_')
        self.repo = Repository(backend='filesystem', base_location=repo_location)
        set_repository(self.repo)

    def tearDown(self):
        pass#self.repo.destroy()

    ### tests begin here ###
    def test_empty_repo(self):
        response = self.app.get('/api/v1.1/drafts/')
        resp = json_response(response, 200)

        self.assertTrue('drafts' in resp)
        self.assertTrue(resp['drafts'] == [])

    def test_create_empty_draft(self):
        create_empty_draft(self.app)

    def test_get_empty_draft(self):
        # create an empty draft
        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']
        contents = resp['draft']['contents']

        # get the json for the empty draft
        response = self.app.get('/api/v1.1/drafts/' + draft_id + '/record')
        resp = json_response(response, 200)

        validate_draft_json(resp, draft_id, contents)

    def test_get_empty_drafts(self):

        draft_ids = []

        # create several empty drafts
        for i in range(0, 25):
            resp = create_empty_draft(self.app)
            draft_ids.append(resp['draft']['id'])

        draft_id = resp['draft']['id']
        contents = resp['draft']['contents']

        # get the json for the empty draft
        get_draft(self.app, draft_id, contents)


    def test_add_data_to_non_existing_draft(self):

        draft_id = 'foobar'

        #resp = self.app.put('/api/v1.1/drafts/' + draft_id)

        resp = upload_file(self.app, draft_id)

        assert(resp.status_code == 404)

    def test_add_data_to_empty_draft_no_data(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        resp = self.app.put('/api/v1.1/drafts/' + draft_id)

        assert(resp.status_code == 302)

    def test_add_data_to_empty_draft_no_filename(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        resp = self.app.put('/api/v1.1/drafts/' + draft_id,
                    content_type='multipart/form-data',
                    data={
                        'payload' : (io.BytesIO(b''), '')
                    }, follow_redirects=False)

        assert(resp.status_code == 302)

    @ignore_warnings
    def test_add_data_to_empty_draft_no_unpack(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # upload a file with 'unpack' set to false
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data, unpack=False)

        resp = json_response(response, 200)

        exp_contents = [
            {   'type' : 'file',
                'path' : os.path.basename(test_data),
                'name' : os.path.basename(test_data),
                'children' : []
            }
        ]

        validate_draft_json(resp, draft_id, exp_contents)
        verify_repository(self.repo, draft_id, exp_contents)

    @ignore_warnings
    def test_add_data_to_empty_draft_unpack(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # upload a file with 'unpack' set to true
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data, unpack=True)

        resp = json_response(response, 200)

        exp_contents = []
        tmpdir = tempfile.mkdtemp()

        if test_data.endswith('tar.gz'):
            tar = tarfile.open(test_data, 'r:gz')
            tar.extractall(path=tmpdir)
            tar.close()

        exp_contents = path_to_dict(tmpdir)
        shutil.rmtree(tmpdir)

        validate_draft_json(resp, draft_id, exp_contents)
        verify_repository(self.repo, draft_id, exp_contents)

    @ignore_warnings
    def test_add_data_no_overwrite(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # upload a file
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data)
        resp = json_response(response, 200)

        # re-upload the file
        response = upload_file(self.app, draft_id, test_data, overwrite=False)
        #XXX check temporarily disabled
        #resp = json_response(response, 409)

        #assert(resp['error'] == 'Destination path already exists')

    @ignore_warnings
    def test_add_data_overwrite(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # upload a file
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data)
        resp = json_response(response, 200)

        # re-upload the file
        response = upload_file(self.app, draft_id, test_data, overwrite=True)
        resp = json_response(response, 200)

    @ignore_warnings
    def test_add_data_no_overwrite_unpack(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # upload a file
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data, unpack=True, overwrite=True) ## XXX remove overwrite=True
        resp = json_response(response, 200)

        # re-upload the file
        response = upload_file(self.app, draft_id, test_data, unpack=True, overwrite=False)

        #XXX check temporarily disabled
        #resp = json_response(response, 409)

        #assert(resp['error'] == 'Destination path already exists')


class DraftTest(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()

        # create a temporary Repository for the test
        repo_location = tempfile.mkdtemp()
        self.repo = Repository(backend='filesystem', base_location=repo_location)
        set_repository(self.repo)

        self.test_files = []

        # generate some (random) files between 1MiB and 20MiB
        # and add them to the draft
        for i in range(0, 25):
            test_data = generate_test_file(1, 20)
            self.test_files.append(test_data)

    def tearDown(self):
        for f in self.test_files:
            os.remove(f)

        self.repo.destroy()


    ### tests begin here ###
    def test_add_data_to_draft_no_data(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # add some files to the draft
        for test_data in self.test_files:
            response = upload_file(self.app, draft_id, test_data, unpack=False)

            resp = json_response(response, 200)

        # upload empty data to the draft
        resp = upload_file(self.app, draft_id)

        assert(resp.status_code == 302)

    def test_add_data_to_draft_no_filename(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # add some files to the draft
        for test_data in self.test_files:
            response = upload_file(self.app, draft_id, test_data, unpack=False)

            resp = json_response(response, 200)

        # upload data with no filename to the draft
        resp = upload_file(self.app, draft_id, test_data='')

        assert(resp.status_code == 302)

    @ignore_warnings
    def test_add_data_to_draft_no_unpack(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        exp_contents = []

        # add some files to the draft
        for test_data in self.test_files:
            response = upload_file(self.app, draft_id, test_data, unpack=False)

            resp = json_response(response, 200)

            exp_contents.append(
                {   'type' : 'file',
                    'path' : secure_filename(os.path.basename(test_data)),
                    'name' : secure_filename(os.path.basename(test_data)),
                    'children' : []
                }
            )

        # upload a file with 'unpack' set to false
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data, unpack=False)

        resp = json_response(response, 200)

        exp_contents.append(
            {   'type' : 'file',
                'path' : secure_filename(os.path.basename(test_data)),
                'name' : secure_filename(os.path.basename(test_data)),
                'children' : []
            }
        )

        validate_draft_json(resp, draft_id, exp_contents)
        verify_repository(self.repo, draft_id, exp_contents)

    @ignore_warnings
    def test_add_data_to_draft_unpack(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        exp_contents = []

        # add some files to the draft
        for test_data in self.test_files:
            response = upload_file(self.app, draft_id, test_data, unpack=False)

            resp = json_response(response, 200)

            exp_contents.append(
                {   'type' : 'file',
                    'path' : secure_filename(os.path.basename(test_data)),
                    'name' : secure_filename(os.path.basename(test_data)),
                    'children' : []
                }
            )

        # upload a file with 'unpack' set to true
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data, unpack=True)

        resp = json_response(response, 200)

        tmpdir = tempfile.mkdtemp()

        if test_data.endswith('tar.gz'):
            tar = tarfile.open(test_data, 'r:gz')
            tar.extractall(path=tmpdir)
            tar.close()

        exp_contents += path_to_dict(tmpdir)

        shutil.rmtree(tmpdir)

        validate_draft_json(resp, draft_id, exp_contents)
        verify_repository(self.repo, draft_id, exp_contents)

    @ignore_warnings
    def test_add_data_no_overwrite(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # add some files to the draft
        for test_data in self.test_files:
            response = upload_file(self.app, draft_id, test_data, unpack=False)

            resp = json_response(response, 200)

        # upload a file
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data)
        resp = json_response(response, 200)

        # re-upload the file
        response = upload_file(self.app, draft_id, test_data, overwrite=False)
        resp = json_response(response, 409)

        assert(resp['error'] == 'Destination path already exists')

    @ignore_warnings
    def test_add_data_overwrite(self):

        resp = create_empty_draft(self.app)

        draft_id = resp['draft']['id']

        # add some files to the draft
        for test_data in self.test_files:
            response = upload_file(self.app, draft_id, test_data, unpack=False)

            resp = json_response(response, 200)

        # upload a file
        test_data = 'test_data/data_00.tar.gz'
        response = upload_file(self.app, draft_id, test_data)
        resp = json_response(response, 200)

        # re-upload the file
        response = upload_file(self.app, draft_id, test_data, overwrite=True)
        resp = json_response(response, 200)

if __name__ == "__main__":
    unittest.main()
