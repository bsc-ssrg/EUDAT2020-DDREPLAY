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

import uuid
import datetime as dt
from storage.backends.filesystem import Filesystem

################################################################################
##### main class                                                           #####
################################################################################
class Repository:

    def __init__(self, backend='filesystem', **kwargs):

        if backend == 'filesystem':
            self.backend = Filesystem(**kwargs)

        ## self.cached_drafts = []
        ## self.cached_versions = {}

    def destroy(self):
        """This function destroys the repository.
        """
        self.backend.destroy()

    def generate_DID(self):
        return uuid.uuid4().hex[0:8]

    def generate_PID(self):
        return uuid.uuid4().hex[0:16]

    def _refresh_cached_drafts(self):
        pass
##         self.cached_drafts = []
## ## XXX
##        src_path = self.config['DRAFTS_METADATA_FOLDER']
##
##        for f in glob.glob(os.path.join(src_path, "*.json")):
##            draft = self._load_draft_record(f)
##            self.cached_drafts.append(draft)

    def _refresh_cached_versions(self):

        self.cached_versions = {}

        for d in self.backend.load_dataset_records():
            PID = d['PID']

            for v in self.backend.load_version_records(PID):
                if PID not in self.cached_versions:
                    self.cached_versions[PID] = []
                self.cached_versions[PID].append(v)


        #self.cached_versions = {}
        #src_path = self.config['DATASETS_FOLDER']

        #for sd in os.listdir(src_path):

        #    subdir = os.path.join(src_path, sd)

        #    if os.path.isdir(subdir):
        #        PID = sd
        #        records_path = os.path.join(subdir, 
        #                self.config['VERSIONS_METADATA_PREFIX'])

        #        for f in glob.glob(os.path.join(records_path, "*.json")):
        #            version = self._read_version_record(f)

        #            if sd not in self.cached_versions:
        #                self.cached_versions[sd] = []
        #            self.cached_versions[sd].append(version)

    def refresh(self):
        self._refresh_cached_drafts()
        self._refresh_cached_versions()

    def create_empty_draft(self, draft_contents):
        """This function creates an empty draft that is not yet associated to 
        any dataset.
        """

        # add to cache
##        self.cached_drafts.append(draft_contents)

        # add to backend
        return self.backend.save_draft_record(draft_contents)

    def create_draft_from_dataset(self, PID):
        """This function creates an draft based on the existing contents of the 
        latest version available of the dataset identified by ``PID``. The 
        created draft is automatically associated to the dataset.
        """

        # fetch the latest version published
        current_version = self.lookup_current_version(PID)

        DID = self.generate_DID()

        new_draft = {
            "id" : DID,
            "PID" : PID,
            "parent_version" : current_version["id"],
            "created_at" : dt.datetime.now(),
            "contents" : current_version["contents"]
        }

        result = self.backend.save_draft_record(new_draft)

        # since we are not deduplicating data, just copy the data
        # from 'current_version' to 'new_draft'
        self.backend.transfer_data_to_draft(DID, PID, current_version['id'])

        return result

    def add_file_to_draft(self, draft, data_iterator, filename, usr_path, unpack, overwrite):

        DID = draft['id']

        result = self.backend.add_file_to_draft(draft, data_iterator, filename, usr_path, unpack, overwrite)

        return result

    def lookup_draft(self, DID, fetch_data=False, fetch_fingerprints=False):
        """This version searches for the draft identified by ``DID`` and 
        returns its JSON record. The draft is initially searched for in the
        repository's cache and, if not found there, the request is forwarded
        to the backend.
        """

##        for i,draft in enumerate(self.cached_drafts):
##            if draft['id'] == DID:
##                return draft

        draft, data_path, fps_path = self.backend.load_draft_record(DID, fetch_data, fetch_fingerprints)

        return draft, data_path, fps_path

    def list_all_drafts(self, refresh_cache=False):
        """This version lists all the drafts managed by the repository. 
        For efficiency's sake, the information generated comes from the 
        repository's draft cache, unless the user sets the ``refresh_cache`` 
        argument to True.
        """

        for d in self.backend.load_draft_records():
            yield d

        # FIXME: cache issues with serialization
        ### if refresh_cache:
        ###     self._refresh_cached_drafts()

        # FIXME++: this returns an already JSON serialized representation
        # when the other functions return non-serialized primitives (e.g. drafts)

    def delete_draft(self, DID, remove_data=True):

        ### for i,draft in enumerate(self.cached_drafts):
        ###     if draft['id'] == DID:
        ###         break

        ### if draft is None or draft['id'] != DID:
        ###     return None

        # remove draft from cached_drafts
        ### del self.cached_drafts[i]

        # remove draft from backend
        self.backend.remove_draft_record(DID, remove_data)


    ############################################################################
    ##### functionality for versions begins here                           #####
    ############################################################################
    def publish_draft(self, draft, author, message):

        DID = draft['id']
        PID = draft['PID']
        VID = draft['id'] # XXX could be different from the DID

        if PID is None:
            PID = self.generate_PID()

        # load the dataset record for this PID
        dataset = self.backend.load_dataset_record(PID)
        parent_version = None

        if dataset is None:
            dataset = {
                "PID" : PID,
                "current" : VID
            }
        else:
            parent_version = dataset["current"]
            dataset["current"] = VID

        # IMPORTANT: in order to prevent history conflicts, a non-empty draft
        # (i.e. a draft with a valid 'parent_version') can only be published as
        # long as no other versions have been published since it was created
        # from the dataset's contents. That is, we need to check if the
        # 'current' version of the dataset is still the rightful parent in
        # order to allow the draft to be published
        if draft['parent_version'] is not None and draft['parent_version'] != parent_version:
            return None

        # update the dataset record
        self.backend.save_dataset_record(dataset) 

        # save a new version record to the backend
        new_version = self.backend.save_version_record(PID, 
            {
                "id" : draft['id'],
                "PID" : PID,
                "parent_version": parent_version,
                "created_at" : dt.datetime.now(),
                "author" : author,
                "message" : message,
                "contents" : draft['contents']
            })

        # the new version inherits all the data from the promoted draft
        self.backend.transfer_data_from_draft(DID, PID, VID)

        # delete record for the 'old draft'
        print('removing draft', DID)
        self.delete_draft(DID, remove_data=False)

        # update caches
        # XXX

        #return self.version_serializer.dump(new_version)
        return new_version
    
    def lookup_current_version(self, PID, fetch_data=False, from_cache=True):

        # XXX dataset cache?

        dataset = self.backend.load_dataset_record(PID)

        VID = dataset['current']

        current_version, data_path = self.backend.load_version_record(PID, VID, fetch_data)

        return current_version, data_path


    def lookup_version(self, PID, VID, fetch_data=False, from_cache=True):

        ### if len(self.cached_versions) == 0:
        ###     self._refresh_cached_versions()

        ### if from_cache:
        ###     for i,draft in enumerate(self.cached_versions):
        ###         if draft['id'] == draft_id:
        ###             return draft
#       ###      return None
#        else:
        version, data_path = self.backend.load_version_record(PID, VID, fetch_data)

        return version, data_path


    def list_all_versions(self, PID, refresh_cache=False):

        for v in self.backend.load_version_records(PID):
            yield v

        #if refresh_cache:
        #    self._refresh_cached_versions()

        #if PID in self.cached_versions:
        #    return self.cached_versions[PID]
        #    #return self.version_list_serializer.dump(self.cached_versions[PID])
        #else:
        #    #return self.version_list_serializer.dump({})
        #    return {}

    def list_all_datasets(self):

        for d in self.backend.load_dataset_records():
            yield d
