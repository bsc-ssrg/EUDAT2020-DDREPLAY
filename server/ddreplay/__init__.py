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
from flask import Flask
from ddreplay.config import configure_app
from storage.repository import Repository

# convienience function to allow unit tests to set their own Repository
def set_repository(usr_repo):
    global repo
    repo = usr_repo

app = Flask(__name__)

# configure the app
configure_app(app)

# create the repository
if 'DD_REPOSITORY_BASE' in app.config:

    # we need an absolute path for the repository
    if not os.path.isabs(app.config['DD_REPOSITORY_BASE']):
        relpath = app.config['DD_REPOSITORY_BASE']
        abspath = os.path.join(os.getcwd(), relpath)
        app.config['DD_REPOSITORY_BASE'] = abspath

    if 'DD_REMOVE_OLD_METADATA' in app.config:
        permanent_remove = app.config['DD_REMOVE_OLD_METADATA']
    else:
        permanent_remove = False

    if 'DD_REMOVE_OLD_FINGERPRINTS' in app.config:
        remove_fps = app.config['DD_REMOVE_OLD_FINGERPRINTS']
    else:
        remove_fps = False

    repo = Repository(backend='filesystem', base_location=app.config['DD_REPOSITORY_BASE'], permanent_remove=permanent_remove, remove_fingerprints=remove_fps)
else:
    repo = None

from ddreplay import views
