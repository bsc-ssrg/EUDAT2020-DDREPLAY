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

import os
from datafile import DataFile

class Session:

    def __init__(self):
        self.staged_files = dict()
        self.root_dir = os.getcwd()
        self.remote_repositories = dict()

    def get_root_dir(self):
        return self.root_dir

    def get_staged_files(self):
        for k,v in self.staged_files.items():
            yield k, v

    def get_staged_files_count(self):
        return len(self.staged_files)

    def set_root_dir(self, dirpath):
        self.root_dir = dirpath

    def get_remote_repositories(self):
        for k,v in self.remote_repositories.items():
            yield k, v

    def add_remote_repository(self, name, url):

        if name in self.remote_repositories:
            raise DuplicateRemoteError

        self.remote_repositories[name] = url

    def remove_remote_repository(self, name):

        if name not in self.remote_repositories:
            raise MissingRemoteError

        del self.remote_repositories[name]

    def rename_remote_repository(self, old, new):

        if old not in self.remote_repositories:
            raise MissingRemoteError

        if new in self.remote_repositories:
            raise DuplicateRemoteError

        self.remote_repositories[new] = self.remote_repositories.pop(old)

    def add_file(self, usrpath):
        relpath = os.path.relpath(usrpath, self.root_dir)
        abspath = os.path.abspath(usrpath)

        self.staged_files[usrpath] = DataFile(relpath, abspath)

        #for df in self.staged_files:
        #    print(df.relpath, df.abspath, df.blocks)


    def remove_file(self, usrpath):

        if usrpath not in self.staged_files:
            raise MissingFileError

        del self.staged_files[usrpath]

    def rm_all(self):
        self.staged_files.clear()

    def save(self, filename):

        filename += ".session"

        with open(filename, "wb") as outfile:
            _pickle.dump(self, outfile)

        return filename

    def load(self, filename):

        with open(filename, "rb") as infile:
            saved_session = _pickle.load(infile)

        self.staged_files = saved_session.staged_files
        self.root_dir = saved_session.root_dir
        self.remote_repositories = saved_session.remote_repositories


class SessionError(Exception):
    """ Base class for exceptions in this module """
    pass

class MissingRemoteError(SessionError):
    pass

class DuplicateRemoteError(SessionError):
    pass

class MissingFileError(SessionError):
    pass

