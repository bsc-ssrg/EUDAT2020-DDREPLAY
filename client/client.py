#!/usr/bin/env python3
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

#import rabin as librp
import cmd
import signal
import sys
import glob

import random
import os
from array import array

window_size = 32
min_block_size = 2**14
avg_block_size = 2**15
max_block_size = 2**16
buf_size = 512*1024

#TARGET = '../test_data/data_00.tar.gz' 
#TARGET = './bar.bin' 
TARGET = 'file1'

#librp.set_min_block_size(2**14)
#librp.set_average_block_size(2**15)
#librp.set_max_block_size(2**16)

def run():
    import struct
    import itertools

    r = librp.Rabin()

    blocks = []

    for t in librp.get_file_fingerprints(TARGET):
        #blocks.extend([t[0], t[1], t[2]])
        blocks.append(t)

    import _pickle

    with open('result.bin', 'wb') as outfile:
        _pickle.dump(blocks, outfile)

    with open('result.bin', 'rb') as infile:
        rblocks = _pickle.load(infile)


    for e1,e2 in zip(blocks, rblocks):
        print(e1, e2)
        assert e1 == e2

def test_my_code(benchmark):
    result = benchmark(run)

class SessionError(Exception):
    """ Base class for exceptions in this module """
    pass

class MissingRemoteError(SessionError):
    pass

class DuplicateRemoteError(SessionError):
    pass

class MissingFileError(SessionError):
    pass

class Session:

    def __init__(self):
        self.staged_files = set()
        self.root_dir = os.getcwd()
        self.remote_repositories = dict()

    def get_root_dir(self):
        return self.root_dir

    def get_staged_files(self):
        return self.staged_files

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

    def add_file(self, path):
        self.staged_files.add(path)

    def remove_file(self, path):

        if path not in self.staged_files:
            raise MissingFileError

        self.staged_files.remove(path)

    def rm_all(self):
        self.staged_files.clear()

    def save(self):
        pass

    def load(self):
        pass

class DatasetUploaderShell(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.intro = ("Welcome to the Dataset Uploader shell.\n"
                "Type \"help\" or \"?\" to list commands.\n")
        self.prompt = "(dupper) "
        self.session = Session()

        self.remote_subcommands = {
            "add"    : self._do_remote_add,
            "remove" : self._do_remote_remove,
            "rename" : self._do_remote_rename,
            "list"   : self._do_remote_list,
        }


    def _autocomplete_path(self, text, line, begidx, endidx, dirs_only=False):
        """ path autocompletion """

        before_arg = line.rfind(" ", 0, begidx)

        if before_arg == -1:
            return  # arg not found

        # remember the fixed portion of the arg
        fixed = line[before_arg+1:begidx]
        arg = line[before_arg+1:endidx]

        patterns = []

        if arg == '':
            patterns = ["*", ".*"]
        else:
            patterns = [arg + "*"]

        paths = [path for pathlist in [glob.glob(p) for p in patterns] for path in pathlist]

        completions = []
        for path in paths:
            if os.path.isdir(path) and path[-1] != os.sep:
                path += os.sep
                completions.append(path.replace(fixed, "", 1))
            else:
                if not dirs_only:
                    completions.append(path.replace(fixed, "", 1))

        return completions

    def _autocomplete_staged_files(self, text, line, begidx, endidx):
        """ autocompletion for staged files """

        before_arg = line.rfind(" ", 0, begidx)

        if before_arg == -1:
            return  # arg not found

        # remember the fixed portion of the arg
        fixed = line[before_arg+1:begidx]
        arg = line[before_arg+1:endidx]

        completions = []
        for path in self.session.get_staged_files():
            if path.startswith(arg):
                completions.append(path.replace(fixed, "", 1))

        return completions


    # commands
    def do_add(self, arg):
        """ stage a file/directory to be uploaded to the repository """

        if arg == "*":
            targets = glob.glob("*")
            targets += glob.glob(".*")
        else:
            if not os.path.exists(arg):
                print("error: File '" + arg + "' does not exist\n")
                return

            targets = [arg]

        for t in sorted(targets):
            self.session.add_file(t)
            print("add '" + t + "'")
        print("")

    def do_remove(self, arg):
        """ prevent a file from being uploaded to the repository """

        if arg == "*":

            for f in sorted(self.session.get_staged_files()):
                print("rm '" + f + "'")
            print("")

            self.session.rm_all()
            return

        if arg == "":
            print("error: missing argument\n")
            return

        try:
            self.session.remove_file(arg)
        except MissingFileError:
            print("error: File '" + arg + "' is not staged for upload\n")
            return

        print("rm '" + arg + "'\n")

    def complete_remove(self, text, line, begidx, endidx):
        """ staged files autocompletion """

        return self._autocomplete_staged_files(text, line, begidx, endidx)

    def complete_add(self, text, line, begidx, endidx):
        """ path autocompletion """

        return self._autocomplete_path(text, line, begidx, endidx)

    def do_ls(self, arg):
        """ list current CWD """

        if arg != "":
            print("error: Unrecognized argument '" + arg + "'\n")
            return

        for p in sorted(os.listdir()):
            if os.path.isdir(p):
                print('\t' + p + '/')
            else:
                print('\t' + p)

    def do_pwd(self, arg):
        """ print current CWD """

        if arg != "":
            print("error: Unrecognized argument '" + arg + "'\n")
            return

        print(os.getcwd())

    def do_cd(self, arg):
        """ change CWD """

        if arg == "":
            os.chdir(self.session.get_root_dir())
            arg = self.session.get_root_dir()
        else:
            try:
                os.chdir(arg)
            except (FileNotFoundError, NotADirectoryError):
                print("error: Argument '" + arg + "' is either not a directory or does not exist\n")
                return

        print("Working directory changed to '" + os.path.abspath(arg) + "'\n")

    def complete_cd(self, text, line, begidx, endidx):
        """ dir path autocompletion """

        return self._autocomplete_path(text, line, begidx, endidx, True)

    def do_status(self, arg):
        """ print a list of the files currently staged for upload """

        if arg != "":
            print("error: Unrecognized argument '" + arg + "'\n")
            return

        staged_files = self.session.get_staged_files()

        if len(staged_files) == 0:
            print("No files staged for upload:")
            print("  (use \"add <file>...\" to update what will be uploaded)\n")
            return

        print("Files staged for upload:")
        print("  (use \"rm <file>...\" to unstage a file)\n")

        for f in sorted(staged_files):
            print("\t" + f)

        print("")

    def _do_remote_list(self, args):
        for name,url in sorted(self.session.get_remote_repositories()):
            print("\t" + name + " -> " + url);

    def _do_remote_add(self, args):

        subargs = args.split(" ")

        if len(subargs) < 2:
            print("error: missing arguments")
            print("  (use \"remote add <name> <url>\" to add a remote repository named <name> at <url>)\n")
            return

        if len(subargs) > 2:
            print("error: too many arguments")
            print("  (use \"remote add <name> <url>\" to add a remote repository named <name> at <url>)\n")
            return

        name, url = subargs[0], subargs[1]

        try:
            self.session.add_remote_repository(name, url)
        except DuplicateRemoteError:
            print("error: A repository named '" + name + "' already exists")
            print("  (use \"remote add <name> <url>\" to add a remote repository named <name> at <url>)\n")

    def _do_remote_rename(self, args):

        subargs = args.split(" ")

        if len(subargs) < 2:
            print("error: missing arguments")
            print("  (use \"remote rename <old> <new>\" to rename the remote named <old> to <new>)\n")
            return

        if len(subargs) > 2:
            print("error: too many arguments")
            print("  (use \"remote rename <old> <new>\" to rename the remote named <old> to <new>)\n")
            return

        old, new = subargs[0], subargs[1]
    
        try:
            self.session.rename_remote_repository(old, new)
        except MissingRemoteError:
            print("error: Repository named '" + old + "' does not exist")
            print("  (use \"remote rename <old> <new>\" to rename the remote named <old> to <new>)\n")
        except DuplicateRemoteError:
            print("error: A repository named '" + new + "' already exists")
            print("  (use \"remote rename <old> <new>\" to rename the remote named <old> to <new>)\n")

    def _do_remote_remove(self, args):

        subargs = args.split(" ")

        if len(subargs) < 1:
            print("error: missing arguments")
            print("  (use \"remote remove <name>\" to remove the remote named <name>)\n")
            return
        if len(subargs) > 1:
            print("error: too many arguments")
            print("  (use \"remote remove <name>\" to remove the remote named <name>)\n")
            return

        name = subargs[0]

        try:
            self.session.remove_remote_repository(name)
        except MissingRemoteError:
            print("error: Repository named '" + old + "' does not exist")
            print("  (use \"remote remove <name>\" to remove the remote named <name>)\n")

    def do_remote(self, args):
        """ Manage remote repositories """

        if args == "":
            print("error: missing argument")
            print("  (use \"remote list\" to show a list of existing remotes)")
            print("  (use \"remote add <name> <url>\" to add a remote repository named <name> at <url>)")
            print("  (use \"remote rename <old> <new>\" to rename the remote named <old> to <new>)")
            print("  (use \"remote remove <name>\" to remove the remote named <name>)\n")
            return

        try:
            cmd, subargs = args.split(" ", 1)
        except ValueError:
            cmd = args
            subargs = ""

        if cmd in self.remote_subcommands:
            self.remote_subcommands[cmd](subargs)
        else:
            print("error: Unrecognized command '" + args + "'")
            print("  (use \"remote list\" to show a list of existing remotes)")
            print("  (use \"remote add <name> <url>\" to add a remote repository named <name> at <url>)")
            print("  (use \"remote rename <old> <new>\" to rename the remote named <old> to <new>)")
            print("  (use \"remote remove <name>\" to remove the remote named <name>)\n")

    def complete_remote(self, text, line, begidx, endidx):

        subcommands = self.remote_subcommands.keys()

        before_arg = line.rfind(" ", 0, begidx)

        if before_arg == -1:
            return  # arg not found

        # remember the fixed portion of the arg
        fixed = line[before_arg+1:begidx]
        arg = line[before_arg+1:endidx]

        completions = []
        for cmd in subcommands:
            if cmd.startswith(arg):
                completions.append(cmd.replace(fixed, "", 1))

        return completions

    def do_upload(self, arg):
        """ upload staged files to the remote repository """

        if arg != "":
            print("error: Unrecognized argument '" + arg + "'\n")
            return


    def do_EOF(self, arg):
        """ gracefully handle Ctrl+D """
        print("Bye!")
        return True


def sigint_handler(signal, frame):
    """ gracefully handle Ctrl+C """

    print("")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    DatasetUploaderShell().cmdloop()
