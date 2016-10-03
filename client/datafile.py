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

import rabin as librp
import os

class DataFile:

    def __init__(self, relpath, abspath):
        self.relpath = relpath
        self.abspath = abspath

        self.mtime = None
        self.blocks = []

    def compute_deltas(self, fingerprints):

        r = librp.Rabin()

        mtime = os.path.getmtime(self.abspath)

        # file didn't change from the last fingerprint computation
        if self.mtime is not None and self.mtime == mtime:
            return

        self.mtime = mtime
        self.blocks.clear()

        for fp in librp.get_file_fingerprints(self.abspath):
            self.blocks.append(fp)
