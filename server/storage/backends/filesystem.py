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

import os, glob, shutil, tempfile, tarfile
import json
from collections import OrderedDict
from marshmallow import Schema, fields, pre_load, post_dump
from werkzeug.utils import secure_filename
import rabin as librp
import _pickle

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

    def __init__(self, base_location, permanent_remove, remove_fingerprints):
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
        self.permanent_remove = permanent_remove
        self.remove_fps = remove_fingerprints
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

    def load_draft_record(self, DID, fetch_data=False, fetch_fingerprints=False):
        """This function loads the JSON record for draft ``DID`` from the 
        filesystem.
        """

        _, src_file, fps_path = self._get_draft_metadata_paths(DID)

        if not os.path.exists(src_file):
            return None, None, None

        draft = self._read_draft_from_file(src_file)

        data_path = None

        if fetch_data:
            data_path = self._get_draft_data_path(DID)

        return draft, data_path, fps_path

    def save_draft_record(self, draft):
        """This function generates a JSON representation from the draft 
        ``record`` and writes it to the filesystem. It also creates the 
        necessary directory structure.
        """

        if 'id' not in draft:
            return None

        DID = draft['id']

        _, dst_file, _ = self._get_draft_metadata_paths(DID)
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
        _, src_file, _ = self._get_draft_metadata_paths(DID)

        if self.permanent_remove:
            self._remove_file(src_file)
        else:
            self._move_file(src_file, self.config['TRASH_FOLDER'])
        
        if remove_data:
            # remove draft data
            src_path = self._get_draft_data_path(DID)

            if self.permanent_remove:
                self._remove_directory(src_path)
            else:
                self._move_directory(src_path, self.config['TRASH_FOLDER'])


    def load_draft_records(self):
        """This function generates a list of all draft records currently 
        stored in the repository.
        """

        src_path = self.config['DRAFTS_METADATA_FOLDER']

        for f in glob.glob(os.path.join(src_path, "*.json")):
            draft = self._read_draft_from_file(f)
            yield draft

    def _create_file(self, draft, stream_iterator, filename, usr_path):

        # create a temporary directory to save the user-provided file until
        # we determine its final location
        tmp_dir = tempfile.mkdtemp(dir=self.config['TMP_FOLDER'])

        payload = stream_iterator.get("file")
        tmp_filename = self._mktemp(payload.filename, tmp_dir)
        payload.save(tmp_filename)

        # if the user provided a destination path we need to honor it
        DID = draft['id']
        base_path = self._get_draft_data_path(DID)
        dst_path = base_path

        if usr_path is not None:
            dst_path = os.path.join(dst_path, usr_path)
            if not os.path.exists(dst_path):
                os.makedirs(dst_path)

        # generate and store fingerprints for the new file
        # XXX this should be multithreaded
        new_fps = { filename: librp.get_file_fingerprints(tmp_filename) }

        old_fps = self._load_draft_fingerprints(DID)

        merged_fps = self._merge_fingerprints(old_fps, new_fps)

        # try to move the file to its final location (this will raise
        # an exception if the destination already exists)
        try:
            self._move_file(tmp_filename, dst_path)
        except shutil.Error as e:
            # remove the temporary file, since the upload failed and re-raise
            # the exception
            self._remove_file(tmp_filename)
            os.rmdir(tmp_dir)
            raise Exception("Destination path already exists")

        # if the move succeeded, store the fingerprints
        self._save_draft_fingerprints(DID, merged_fps)

        # FIXME we could be smarter with this and update only what has changed
        contents = self._load_draft_contents(DID)

        # add the 'contents' to the draft descriptor and 
        # update it in the repository
        # NOTE: 'draft' is already an entry in cached_drafts
        # and we can modify it in place
        draft['contents'] = contents

        # also update it in the backend
        self.save_draft_record(draft)

        # remove the temporary directory (which should now be empty)
        os.rmdir(tmp_dir)

        return draft

    def _rebuild_file(self, out_filename, new_fps, old_fps, orig_filepath, new_parts):

        # get size information from the provided new_parts
        # (order is important)
        patches = OrderedDict()
        patches_size = 0

        for p in new_parts:

            fields = os.path.basename(p).split("_")

            offset = int(fields[-2])
            size = int(fields[-1])

            sz = os.stat(p).st_size

            assert(size == sz)

            patches[offset] = (p, sz)

            patches_size += sz

        old_hashes = {hv:(offset,size) for offset,size,hv in old_fps}

        # we need to build a map to see how to reconstruct it and from where
        rebuild_map = []

        expected_size = 0
        for fps in new_fps:

            new_offset = fps[0]
            new_size = fps[1]
            hv = fps[2]

            expected_size += new_size

            if hv in old_hashes:
                old_offset, old_size = old_hashes[hv]
                assert(old_size == new_size)

                rebuild_map.append((new_offset, new_size, orig_filepath, old_offset, old_size))
            else:
                # find the appropriate part file that matches the required offset
                if new_offset not in patches:
                    assert(False)

                part_file, part_size = patches[new_offset]

                assert(new_size == part_size)

                rebuild_map.append((new_offset, new_size, part_file, 0, part_size))

        # check that the rebuild_map is consistent:
        # first entry must have offset 0
        assert(rebuild_map[0][0] == 0)
        prev_offset = 0
        prev_size = rebuild_map[0][1]
        rebuild_size = prev_size

        for entry in rebuild_map[1:]:
            part_offset = entry[0]
            part_size = entry[1]

            assert(part_offset == (prev_offset + prev_size))

            prev_offset = part_offset
            prev_size = part_size
            rebuild_size += prev_size

        assert(expected_size == rebuild_size)

        # everything looks ok: rebuild the file
        with open(out_filename, "wb") as outfile:
            for entry in rebuild_map:
                part_file = entry[2]
                part_offset = entry[3]
                part_size = entry[4]
                
                with open(part_file, "rb") as infile:
                    infile.seek(part_offset)
                    outfile.write(infile.read(part_size))

        # check that the output size matches what is expected
        assert(os.stat(out_filename).st_size == expected_size)

        # XXX: paranoia mode, check that the fingerprints for the generated file
        # match those sent by the client. This can take a long time for
        # really large files
        # assert(librp.get_file_fingerprints(out_filename) == new_fps)


    def _replace_file(self, draft, stream_iterator, filename, usr_path):

        # check that the file actually exists in the repository
        DID = draft['id']
        base_path = self._get_draft_data_path(DID)
        dst_path = base_path

        # if the user has provided a destination path we need to honor it
        if usr_path is not None:
            dst_path = os.path.join(dst_path, usr_path)

        orig_filepath = os.path.join(dst_path, filename)

        if not os.path.exists(orig_filepath):
            raise Exception("Replacement target does not exist")

        # create a temporary directory to rebuild the file
        tmp_dir = tempfile.mkdtemp(dir=self.config['TMP_FOLDER'])

        # fetch the fingerprints sent by the client
        stream = stream_iterator.get("fingerprints")

        client_file_fps = _pickle.loads(stream.read())

        # now fetch the parts sent by the client and save them to disk, since
        # they may not fit into memory
        new_parts = []
        for part in stream_iterator.getlist("parts"):
            tmp_filename = self._mktemp(part.filename, tmp_dir)
            part.save(tmp_filename)
            new_parts.append(tmp_filename)

        _, _, fps_path = self.load_draft_record(draft['id'], False, True)

        with open(fps_path, "rb") as infile:
            server_fps = _pickle.load(infile)

        stored_file_fps = server_fps[filename]

        tmp_output = self._mktemp(filename + ".rebuilt", tmp_dir) 

        self._rebuild_file(tmp_output, client_file_fps, stored_file_fps, orig_filepath, new_parts)

        # move the rebuilt file to its final location
        self._move_file(tmp_output, orig_filepath, overwrite=True)

        # update the fingerprints

        if usr_path is not None:
            relpath = os.path.join(usr_path, filename)
        else:
            relpath = filename

        # XXX: paranoia mode, we are taking the client sent FPS at face value.
        # for security reasons, it would be better to recompute them, though
        # this may take a long time
        new_fps = { relpath: client_file_fps }
        old_fps = self._load_draft_fingerprints(DID)

        merged_fps = self._merge_fingerprints(old_fps, new_fps)

        self._save_draft_fingerprints(DID, merged_fps)
        
        # NOTE: there is no need to update the 'contents' since the file
        # has the same name and type

        # remove the temporary directory
        shutil.rmtree(tmp_dir)

        # TODO exceptions, return, etc
        return draft

    def add_file_to_draft(self, draft, stream_iterator, filename, usr_path, unpack, replace):
        """This function adds the user-provided file ``stream`` to the draft
        identified by ``DID``.
        """

        if not replace:
            return self._create_file(draft, stream_iterator, filename, usr_path)

        return self._replace_file(draft, stream_iterator, filename, usr_path)


        ### XXX OLD CODE. REMOVE AFTER TESTS

        ### # save the user-provided file as a temporary file until we determine
        ### # its final location
        ### tmp_filename = self._mktemp(filename)

        ### with open(tmp_filename, "wb") as outfile:
        ###     for chunk in stream:
        ###         outfile.write(chunk)
        ###         
        ### DID = draft['id']
        ### base_path = self._get_draft_data_path(DID)
        ### dst_path = base_path

        ### # if the user has provided a destination path we need to honor it
        ### if usr_path is not None:
        ###     dst_path = os.path.join(dst_path, usr_path)
        ###     if not os.path.exists(dst_path):
        ###         os.makedirs(dst_path)

        ### # if the user asked for the file to be unpacked, do so
        ### if(unpack):
        ###     self._unpack_file(tmp_filename, dst_path, overwrite)
        ### else:
        ###     self._move_file(tmp_filename, dst_path, overwrite)

        ### new_fps = {}

        ### # generate and store fingerprints for all files
        ### # XXX this should probably be multithreaded
        ### for root, dirs, files in os.walk(dst_path):
        ###     for filename in files:
        ###         filepath = os.path.join(root, filename)

        ###         relpath = os.path.relpath(filepath, base_path)

        ###         new_fps[relpath] = librp.get_file_fingerprints(filepath)

        ### old_fps = self._load_draft_fingerprints(DID)

        ### merged_fps = self._merge_fingerprints(old_fps, new_fps)

        ### self._save_draft_fingerprints(DID, merged_fps)

        ### # FIXME we could be smarter with this and update only what has changed
        ### contents = self._load_draft_contents(DID)

        ### # add the 'contents' to the draft descriptor and 
        ### # update it in the repository
        ### # NOTE: 'draft' is already an entry in cached_drafts
        ### # and we can modify it in place
        ### draft['contents'] = contents

        ### # also update it in the backend
        ### self.save_draft_record(draft)

        ### return draft#self.draft_serializer.dump(draft)

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

        _, src_file,_ = self._get_version_metadata_paths(pPID, VID)

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

        _, dst_file, _ = self._get_version_metadata_paths(pPID, record_id)

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

        src_path, _, _ = self._get_version_metadata_paths(PID, None)

        for f in glob.glob(os.path.join(src_path, "*.json")):
            version = self._read_version_from_file(f)
            yield version

    def remove_fingerprints_from_version(self, PID, VID):
        """ This function removes the fingerprints associated to the version
        identified by ``PID`` and ``VID``.
        """
        _, _, src_fps_path = self._get_version_metadata_paths(PID, VID)

        if self.remove_fps:
            if self.permanent_remove:
                self._remove_file(src_fps_path)
            else:
                self._move_file(src_fps_path, self.config['TRASH_FOLDER'])


    def transfer_fingerprints_from_draft(self, DID, pPID, VID):
        """ This function retrieves the fingerprints associated to draft ``DID``
        and stores them in the backend, associating it to the dataset version
        identified by ``<pPID+VID>``.
        """

        _, _, src_fps_path = self._get_draft_metadata_paths(DID)
        _, _, dst_fps_path = self._get_version_metadata_paths(pPID, VID)

        self._move_file(src_fps_path, dst_fps_path)

    def transfer_fingerprints_to_draft(self, DID, pPID, VID):
        """ This function retrieves the fingerprints associated to version 
        ``<pPID+VID>`` and copies them to the draft ``DID``.
        """

        _, _, src_fps_path = self._get_version_metadata_paths(pPID, VID)
        _, _, dst_fps_path = self._get_draft_metadata_paths(DID)

        self._copy_file(src_fps_path, dst_fps_path)


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
    def _remove_file(filename):
        os.remove(filename)

    @staticmethod
    def _move_directory(src_path, dst_path):
        shutil.move(src_path, dst_path)

    @staticmethod
    def _remove_directory(directory):
        shutil.rmtree(directory)

    @staticmethod
    def _copy_file(src_filename, dst_filename):
        shutil.copy(src_filename, dst_filename)

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

    def _mktemp(self, filename, parent=None):

        if parent is None:
            parent = self.config['TMP_FOLDER']

        sec_filename = secure_filename(filename)
        tmp_filename = os.path.join(parent, sec_filename)

        return tmp_filename

    def _unpack_file(self, filename, destination, overwrite):

        if overwrite:
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
            ### # FIXME: the following code is not working yet
            ### # uncompress to a temporary directory so that we can check
            ### # file by file if they can be copied or not
            ### tmp_dir = tempfile.mkdtemp(dir=self.config['TMP_FOLDER'])

            ### if filename.endswith('tar.gz'):
            ###     mode = "r:gz"
            ### elif filename.endswith('tar.bz'):
            ###     mode = "r:bz"
            ### elif filename.endswith('tar'):
            ###     mode = "r:"

            ### if mode is not None:
            ###     with tarfile.open(filename, mode) as tar:
            ###         tar.extractall(path=tmp_dir)

            ### for root, dirs, files in os.walk(tmp_dir):
            ###     for filename in files:
            ###         subdir = os.path.relpath(root, tmp_dir)
            ###         relpath = os.path.join(subdir, filename)

            ###         target_path = os.path.join(destination, relpath)

            ###         print(relpath, '->', target_path, os.path.exists(target_path))

            ###         try:
            ###             shutil.copy(relpath, target_path)
            ###         except shutil.Error as e:
            ###             print(e)
            ###             conflicts.append(relpath)

            ###         print(relpath)


            ### os.rmdir(tmp_dir)
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

        fps_path = os.path.join(dm_path, DID + ".fps")

        return dm_path, record_path, fps_path

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
                ├── fe5c2d9f.json
                └── fe5c2d9f.fps


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

    def _load_draft_fingerprints(self, DID):

        _, _, fps_path = self._get_draft_metadata_paths(DID)

        fps = None

        if not os.path.exists(fps_path):
            return None

        with open(fps_path, 'rb') as infile:
            fps = _pickle.load(infile) 

        return fps

    def _save_draft_fingerprints(self, DID, fps):

        _, _, fps_path = self._get_draft_metadata_paths(DID)

        with open(fps_path, 'wb') as outfile:
            _pickle.dump(fps, outfile) 

    def _merge_fingerprints(self, old_fps, new_fps):

        if old_fps is None:
            merged_fps = dict()
        else:
            merged_fps = old_fps.copy()
        merged_fps.update(new_fps)

        return merged_fps





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
            fps_path = os.path.join(vm_path, VID + ".fps")
        else:
            record_path = None
            fps_path = None

        return vm_path, record_path, fps_path

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

