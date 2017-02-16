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

def show_dataset(repo_url, PID=None, VID=None):

    if "http" not in repo_url:
        repo_url = "http://" + repo_url

    repo_url += "/api/" + __api_version__

    req_url = repo_url + "/datasets"

    if PID is not None:
        req_url += "/" + PID + "/versions/"

        if VID is not None and VID != 'all':
            req_url += VID + "/record"

    try:
        r = requests.get(req_url)
        r.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(err)
        sys.exit(1)

    if r.status_code != 200:
        print("Unable to list dataset: HTTP Error", r.status_code)
        sys.exit(1)

    print(r.text, end='')

def help():
    print("Usage:", os.path.basename(sys.argv[0]), "<URL> [PID] [VID|all]")
    print("Arguments:")
    print("    <URL> - repository url")
    print("    <PID> - Dataset ID (if unspecified, all datasets are shown)")
    print("    <VID> - Version ID (if unspecified, the latest version is shown)")

if __name__ == "__main__":

    if len(sys.argv) != 2 and len(sys.argv) != 3 and len(sys.argv) != 4:
        help()
        sys.exit(1)

    repo_url = sys.argv[1]

    if len(sys.argv) >= 3:
        PID = sys.argv[2]
    else:
        PID = None

    if len(sys.argv) == 4:
        VID = sys.argv[3]
    else:
        VID = None

    show_dataset(repo_url, PID, VID)
