# -*- coding: utf-8 -*-
###########################################################################
#  (C) Copyright 2016 Barcelona Supercomputing Center                     #
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

import os, glob, shutil, tempfile, tarfile
import json
from collections import OrderedDict
from marshmallow import Schema, fields, pre_load, post_dump
from werkzeug.utils import secure_filename

################################################################################
##### schemas for serialization/deserialization                            #####
################################################################################
class DirectorySchema(Schema):
    """ schema to serialize/deserialize a directory """

    name = fields.String()
    path = fields.String()
    type = fields.String()
    children = fields.Nested('self', exclude=(), default=[], many=True)

    class Meta:
        ordered = True

class DraftSchema(Schema):
    """ schema to serialize/deserialize a draft descriptor """

    id = fields.String(required=True)
    PID = fields.String(required=True, default=None, missing=None)
    parent_version = fields.String(required=True, default=None, missing=None)
    created_at = fields.DateTime(required=True)
    contents = fields.Nested(DirectorySchema, required=True, default=[], many=True)

    @pre_load(pass_many=True)
    def unwrap_if_many(self, data, many):
        if(many):
            return data['drafts']
        return data

    @post_dump(pass_many=True)
    def wrap_if_many(self, data, many):
        if(many):
            return {'drafts': data}
        return data

    class Meta:
        ordered = True

class DatasetSchema(Schema):
    """ schema to serialize/deserialize a PID record """

    PID = fields.String(required=True)
    current = fields.String(required=True)

class VersionSchema(Schema):
    """ schema to serialize/deserialize a version descriptor """

    id = fields.String(required=True)
    PID = fields.String(required=True)
    created_at = fields.DateTime(required=True)
    parent_version = fields.String(required=True, default=None, missing=None)
    author = fields.String(required=True)
    message = fields.String(required=True)
    contents = fields.Nested(DirectorySchema, required=True, default=[], many=True)

    @pre_load(pass_many=True)
    def unwrap_if_many(self, data, many):
        if(many):
            return data['versions']
        return data

    @post_dump(pass_many=True)
    def wrap_if_many(self, data, many):
        if(many):
            return {'versions': data}
        return data

    class Meta:
        ordered = True


class Filesystem:

    default_config = {
        'TMP_FOLDER'                : 'tmp',
        'TRASH_FOLDER'              : 'trash',
        'DRAFTS_FOLDER'             : 'drafts',
        'DRAFTS_METADATA_FOLDER'    : os.path.join('drafts', 'metadata'),
        'DRAFTS_DATA_FOLDER'        : os.path.join('drafts', 'data'),
        'DATASETS_FOLDER'           : 'datasets',
        'VERSIONS_FOLDER'           : 'versions',
        'VERSIONS_METADATA_PREFIX'  : os.path.join('versions', 'metadata'),
        'VERSIONS_DATA_PREFIX'      : os.path.join('versions', 'data')
    }
    
    def _build(self):
        """ This function initializes the configuration options of the backend,
        and creates the basic directory structure of the repository.
        """

        repo_dirs = [ 
            self.config['TMP_FOLDER'],
            self.config['TRASH_FOLDER'],
            self.config['DRAFTS_FOLDER'],
            self.config['DRAFTS_METADATA_FOLDER'],
            self.config['DRAFTS_DATA_FOLDER'],
            self.config['DATASETS_FOLDER'],
        ]

        for rd in repo_dirs:
            if not os.path.exists(rd):
                os.makedirs(rd)

    def __init__(self, base_location):
        """This function creates the necessary structures in the filesystem to
        represent the repository metadta and data contents. The repository 
        organization will eventually end up as follows:

        <base_location>
        ├── datasets
        │   ├── 01f96f238278463c
        │   │   ├── 01f96f238278463c.json
        │   │   └── versions
        │   │       ├── data
        │   │       │   └── 00c63abb
        │   │       │       └── foo
        │   │       │           └── bar
        │   │       │               └── data_00.tar.gz
        │   │       └── metadata
        │   │           └── 00c63abb.json
        ├── drafts
        │   ├── data
        │   │   └── fe5c2d9f
        │   │       └── foo
        │   │           └── bar
        │   │               └── data_00.tar.gz
        │   └── metadata
        │       └── fe5c2d9f.json
        ├── tmp
        └── trash

        ... though this function only creates the main directories of this
        configuration.
        """

        self.base_location = base_location
        self.config = dict()

        # make sure that the base_location is used
        for key in self.default_config:
            if '_FOLDER' in key:
                self.config[key] = \
                    os.path.join(self.base_location, self.default_config[key])
            else:
                self.config[key] = self.default_config[key]

        self.draft_serializer = DraftSchema()
        self.draft_list_serializer = DraftSchema(many=True)

        self.dataset_serializer = DatasetSchema()

        self.version_serializer = VersionSchema()
        self.version_list_serializer = VersionSchema(many=True)

        self._build()

    def destroy(self):
        """This function destroys the repository.
        """
        shutil.rmtree(self.base_location)

    def load_draft_record(self, DID):
        """This function loads the JSON record for draft ``DID`` from the 
        filesystem.
        """

        _, src_file = self._get_draft_metadata_paths(DID)

        if not os.path.exists(src_file):
            return None

        draft = self._read_draft_from_file(src_file)

        return draft

    def save_draft_record(self, draft):
        """This function generates a JSON representation from the draft 
        ``record`` and writes it to the filesystem. It also creates the 
        necessary directory structure.
        """

        if 'id' not in draft:
            return None

        DID = draft['id']

        _, dst_file = self._get_draft_metadata_paths(DID)
        data_dir = self._get_draft_data_path(DID)

        # if this is the first time that the record is saved we need to create
        # the directory structure
        if not os.path.exists(data_dir) or not os.path.exists(dst_file):
            self._create_draft_record_tree(data_dir)

        # serialize 'dict' -> 'json'
        data_out = self.draft_serializer.dump(draft)

        # store the info about the draft as a JSON file
        with open (dst_file, 'w') as outfile:
            json.dump(data_out.data, outfile)

        return data_out.data

    def remove_draft_record(self, DID, remove_data=True):
        """This function moves the ``DID`` draft record's metadata to
        the repository's TRASH folder. If the user sets the ``remove_data``
        argument to True, the draft's data is also moved to the TRASH folder.
        """

        # remove metadata record
        _, src_file = self._get_draft_metadata_paths(DID)
        self._move_file(src_file, self.config['TRASH_FOLDER'])
        
        if remove_data:
            # remove draft data
            src_path = self._get_draft_data_path(DID)
            self._move_directory(src_path, self.config['TRASH_FOLDER'])


    def load_draft_records(self):
        """This function generates a list of all draft records currently 
        stored in the repository.
        """

        src_path = self.config['DRAFTS_METADATA_FOLDER']

        for f in glob.glob(os.path.join(src_path, "*.json")):
            draft = self._read_draft_from_file(f)
            yield draft

    def add_file_to_draft(self, draft, payload, usr_path, unpack, overwrite):
        """This function adds the user-provided file ``payload`` to the draft
        identified by ``DID``.
        """

        # save the user-provided file as a temporary file until we determine
        # its final location
        tmp_filename = self._mktemp(payload)

        DID = draft['id']
        dst_path = self._get_draft_data_path(DID)

        # if the user has provided a destination path we need to honor it
        if usr_path is not None:
            dst_path = os.path.join(dst_path, usr_path)
            if not os.path.exists(dst_path):
                os.makedirs(dst_path)

        # if the user asked for the file to be unpacked, do so
        if(unpack):
            self._unpack_file(tmp_filename, dst_path, overwrite)
        else:
            self._move_file(tmp_filename, dst_path, overwrite)

        # FIXME we could be smarter with this and update only what has changed
        contents = self._load_draft_contents(DID)

        # add the 'contents' to the draft descriptor and 
        # update it in the repository
        # NOTE: 'draft' is already an entry in cached_drafts
        # and we can modify it in place
        draft['contents'] = contents

        # also update it in the backend
        self.save_draft_record(draft)

        return draft#self.draft_serializer.dump(draft)

    def load_dataset_record(self, PID):
        """ load a dataset record from the backend """

        _, src_file = self._get_dataset_paths(PID)

        record = self._read_dataset_from_file(src_file)

        return record

    def save_dataset_record(self, dataset):
        """ save a dataset record to the backend """

        PID = dataset['PID']

        dst_dir, dst_file = self._get_dataset_paths(PID)

        # if it does not exist, create the dataset structure
        # in the backend
        if not os.path.exists(dst_dir) or not os.path.exists(dst_file):
            self._create_dataset_record_tree(dataset, dst_dir, dst_file)

        # serialize 'dataset' -> 'json'
        data_out = self.dataset_serializer.dump(dataset)

        # store the record as a JSON file
        with open(dst_file, 'w') as outfile:
            json.dump(data_out.data, outfile)

    def load_version_record(self, pPID, VID, fetch_data=False):
        """ This function searches for the version record referenced by ``pPID``
        and ``VID``, loads it from the backend, and returns it.
        """

        _, src_file = self._get_version_metadata_paths(pPID, VID)

        record = self._read_version_from_file(src_file)

        data_path = None

        if fetch_data:
            data_path = self._get_version_data_path(pPID, VID)

        return record, data_path

    def save_version_record(self, pPID, record):
        """ This function stores the version record ``record`` in the backend,
        associating it to the dataset referenced by ``pPID``.
        """

        if 'id' not in record:
            return

        record_id = record['id']

        _, dst_file = self._get_version_metadata_paths(pPID, record_id)

        return self._write_version_to_file(record, dst_file)

    def load_dataset_records(self):
        """This function generates a list of all dataset records currently 
        stored in the repository.
        """

        src_path = self.config['DATASETS_FOLDER']

        for sd in os.listdir(src_path):

            subdir = os.path.join(src_path, sd)

            if os.path.isdir(subdir):
                PID = sd

                dataset = self.load_dataset_record(PID)

                yield dataset

    def load_version_records(self, PID):
        """This function generates a list of all version records currently 
        stored in the repository for the dataset identified by ``PID``.
        """

        src_path, _ = self._get_version_metadata_paths(PID, None)

        for f in glob.glob(os.path.join(src_path, "*.json")):
            version = self._read_version_from_file(f)
            yield version

    def transfer_data_from_draft(self, DID, pPID, VID):
        """ This function retrieves all data associated with draft ``DID``
        and stores it in the backend, associating it to the dataset version
        identified by ``<pPID+VID>``.
        """

        src_path = self._get_draft_data_path(DID)
        dst_path = self._get_version_data_path(pPID, VID)

        self._move_directory(src_path, dst_path)

    def transfer_data_to_draft(self, DID, pPID, VID):
        """ This function retrieves all data associated with version 
        ``<pPID+VID>`` and associates it to the draft ``DID``.
        """

        src_path = self._get_version_data_path(pPID, VID)
        dst_path = self._get_draft_data_path(DID)

        self._copy_directory(src_path, dst_path)

    ############################################################################
    ##### private functions for file management                            #####
    ############################################################################

    @staticmethod
    def _move_file(src_filename, dst_filename, overwrite=False):
        if not overwrite:
            shutil.move(src_filename, dst_filename)
        else:
            shutil.copy(src_filename, dst_filename)

    @staticmethod
    def _move_directory(src_path, dst_path):
        shutil.move(src_path, dst_path)

    @staticmethod
    def _copy_directory(src_path, dst_path):

        # shutil.copytree only copies directories that do not exist,
        # ensure that if 'dst_path' exists it is empty, and remove it
        if os.path.exists(dst_path):
            #try:
                os.rmdir(dst_path)
            #except OSError:
            #    return

        shutil.copytree(src_path, dst_path)

    def _mktemp(self, payload):
        sec_filename = secure_filename(payload.filename)
        tmp_filename = os.path.join(self.config['TMP_FOLDER'], sec_filename)
        payload.save(tmp_filename)

        return tmp_filename

    def _unpack_file(self, filename, destination, overwrite):

        if overwrite:
            print('kk')
            # FIXME: for improved security check that no absolute paths or paths with
            # '..' exist in the compressed file 
            if(filename.endswith('tar.gz')):
                tar = tarfile.open(filename, "r:gz")
                tar.extractall(path=destination)
                tar.close()
            elif(filename.endswith('tar.bz2')):
                tar = tarfile.open(filename, "r:bz")
                tar.extractall(path=destination)
                tar.close()
            elif(filename.endswith('tar')):
                tar = tarfile.open(filename, "r:")
                tar.extractall(path=destination)
                tar.close()
        else:
            # FIXME: the following code is not working yet
            # uncompress to a temporary directory so that we can check
            # file by file if they can be copied or not
            tmp_dir = tempfile.mkdtemp(dir=self.config['TMP_FOLDER'])

            if filename.endswith('tar.gz'):
                mode = "r:gz"
            elif filename.endswith('tar.bz'):
                mode = "r:bz"
            elif filename.endswith('tar'):
                mode = "r:"

            if mode is not None:
                with tarfile.open(filename, mode) as tar:
                    tar.extractall(path=tmp_dir)

            for root, dirs, files in os.walk(tmp_dir):
                for filename in files:
                    subdir = os.path.relpath(root, tmp_dir)
                    relpath = os.path.join(subdir, filename)

                    target_path = os.path.join(destination, relpath)

                    print(relpath, '->', target_path, os.path.exists(target_path))

                    try:
                        shutil.copy(relpath, target_path)
                    except shutil.Error as e:
                        print(e)
                        conflicts.append(relpath)

                    print(relpath)


            os.rmdir(tmp_dir)
            pass

    @staticmethod
    def _path_to_dict_v2(rootdir):

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


    ############################################################################
    ##### private functions for managing drafts                            #####
    ############################################################################

    def _get_draft_metadata_paths(self, DID):
        """This function computes the paths to the metadata directory and
        metadata JSON record for the draft identified by ``DID``.
        """ 

        dm_path = self.config['DRAFTS_METADATA_FOLDER']

        record_path = os.path.join(dm_path, DID + ".json")

        return dm_path, record_path

    def _get_draft_data_path(self, DID):
        """This function computes and returns the path for the base directory
        where a draft's data is contained.
        """ 

        dd_path = self.config['DRAFTS_DATA_FOLDER']
        dd_path = os.path.join(dd_path, DID)

        return dd_path

    def _create_draft_record_tree(self, data_dir):
        """This function creates the following directory representation of 
        a draft's data and metadata:

        <base_location>
        └── drafts
            ├── data
            │   └── fe5c2d9f
            └── metadata
                └── fe5c2d9f.json
        """

        os.makedirs(data_dir)

    def _read_draft_from_file(self, src_file):
        """This function reads a draft record from the JSON file ``src_file``.
        """

        with open (src_file, 'r') as infile:
            data_in = json.load(infile)

        # deserialize 'json' -> 'dict'
        record = self.draft_serializer.load(data_in).data

        return record

    def _load_draft_contents(self, DID):

        dst_path = self._get_draft_data_path(DID)

        if(not os.path.exists(dst_path)):
            return None

        contents = self._path_to_dict_v2(dst_path)

        return contents


    ############################################################################
    ##### private functions for managing datasets                          #####
    ############################################################################

    def _get_dataset_paths(self, PID):
        """This function computes the paths to the metadata directory and
        metadata JSON record for the dataset identified by ``PID``.
        """ 

        dataset_path = os.path.join(self.config['DATASETS_FOLDER'], PID)

        record_path = os.path.join(dataset_path, PID + '.json')

        return dataset_path, record_path

    def _create_dataset_record_tree(self, dataset, dst_dir=None, dst_file=None):
        """ This function creates the following directory representation of a
        dataset's metadata:

            <base_location>
            └── datasets
                └── 52e7fb62d0ad4e87
                    ├── 52e7fb62d0ad4e87.json
                    └── versions
                        ├── data
                        └── metadata
        """

        if dst_dir is None or dst_file is None:
            dst_dir, dst_file = self._get_dataset_paths(PID)

        versions_metadata_path = os.path.join(dst_dir, 
                self.config['VERSIONS_METADATA_PREFIX'])
        versions_data_path = os.path.join(dst_dir, 
                self.config['VERSIONS_DATA_PREFIX'])

        os.makedirs(versions_data_path)
        os.makedirs(versions_metadata_path)

        return versions_metadata_path, versions_data_path

    def _read_dataset_from_file(self, src_file):
        """ This function reads a dataset record from the JSON 
        file ``src_file``.
        """

        if not os.path.exists(src_file):
            return None

        with open (src_file, 'r') as infile:
            data_in = json.load(infile)

        # deserialize 'json' -> 'dict'
        record = self.dataset_serializer.load(data_in).data

        return record


    ############################################################################
    ##### private functions for managing versions                          #####
    ############################################################################

    def _get_version_metadata_paths(self, pPID, VID):
        """This function computes the paths to the metadata directory and
        metadata JSON record for the version identified by ``<pPID+VID>``.
        """ 

        dst_dir, _ = self._get_dataset_paths(pPID)

        vm_path = os.path.join(dst_dir, 
                self.config['VERSIONS_METADATA_PREFIX'])

        if VID is not None:
            record_path = os.path.join(vm_path, VID + ".json")
        else:
            record_path = None

        return vm_path, record_path

    def _get_version_data_path(self, pPID, VID):
        """This function computes and returns the path for the base directory
        where a version's data is contained.
        """ 

        dst_dir, _ = self._get_dataset_paths(pPID)

        vd_path = os.path.join(dst_dir, 
                self.config['VERSIONS_DATA_PREFIX'], VID)

        return vd_path

    def _read_version_from_file(self, src_file):
        """ This function reads a version record from the ``src_file`` JSON 
        file stored in the backend.
        """

        if not os.path.exists(src_file):
            return None

        with open (src_file, 'r') as infile:
            data_in = json.load(infile)

        # deserialize 'json' -> 'dict'
        record = self.version_serializer.load(data_in).data

        return record

    def _write_version_to_file(self, record, dst_file):
        """ This function writes the version record ``record`` (as a JSON file) 
        into the file pointed to by the ``dst_file`` argument.
        """

        # serialize 'dict' -> 'json'
        data_out = self.version_serializer.dump(record)

        # store the info about the draft as a JSON file
        with open (dst_file, 'w') as outfile:
            json.dump(data_out.data, outfile)

        return data_out.data

