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
import re

from api import __api_version__

def download_draft(repo_url, DID):

    if "http" not in repo_url:
        repo_url = "http://" + repo_url

    repo_url += "/api/" + __api_version__

    req_url = repo_url + "/drafts/" + DID

    try:
        r = requests.get(req_url, stream=True)
        r.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(err)
        sys.exit(1)

    if 'Content-Disposition' not in r.headers:
        print('Missing \'Content-Disposition\' headers in response.')
        sys.exit(1)

    cd = r.headers['Content-Disposition']
    filename = re.findall('filename=(.+)', cd)

    if len(filename) == 0:
        print('Missing \'attachment; filename\' field in response')
        sys.exit(1)

    if len(filename) != 1:
        print('Too many \'attachment; filename\' fields in response')
        sys.exit(1)

    filename = filename[0]

    with open(filename, "wb") as outfile:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                outfile.write(chunk)

    print('Draft contents saved to \'' + filename + '\'')

def help():
    print("Usage:", os.path.basename(sys.argv[0]), "<URL> <DID>")
    print("Arguments:")
    print("    <URL> - repository url")
    print("    <DID> - draft ID")

if __name__ == "__main__":

    if len(sys.argv) != 3:
        help()
        sys.exit(1)

    repo_url = sys.argv[1]
    DID = sys.argv[2]

    download_draft(repo_url, DID)
