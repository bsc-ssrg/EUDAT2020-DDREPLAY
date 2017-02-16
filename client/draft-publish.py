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

import sys
import os
import requests

from api import __api_version__

def publish_draft(repo_url, DID, author, message):

    if "http" not in repo_url:
        repo_url = "http://" + repo_url

    repo_url += "/api/" + __api_version__

    req_url = repo_url + "/drafts/" + DID + "/publish?"
    req_url += "author=" + author
    req_url += "&message=" + message

    try:
        r = requests.get(req_url)
        r.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(err)
        sys.exit(1)

    if r.status_code != 201:
        print("Unable to publish draft: HTTP Error", r.status_code)
        sys.exit(1)

    print(r.text, end='')


def help():
    print("Usage:", os.path.basename(sys.argv[0]), "<URL> <DID> <author> <message>")
    print("Creates an immutable Dataset Version using the data associated to the Draft <DID>.")
    print()
    print("Arguments:")
    print("    <URL> - repository url")
    print("    <DID> - Draft ID")
    print("    <author> - Publication author")
    print("    <message> - Publication message")

if __name__ == "__main__":

    if len(sys.argv) < 1 or len(sys.argv) != 5:
        help()
        sys.exit(1)

    repo_url = sys.argv[1]
    DID = sys.argv[2]
    author = sys.argv[3]
    message = sys.argv[4]

    publish_draft(repo_url, DID, author, message)
