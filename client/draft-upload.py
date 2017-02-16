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
import urllib
import requests
import _pickle
import tempfile
import rabin as librp
from requests_toolbelt.streaming_iterator import StreamingIterator
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor
import math

from api import __api_version__

def digits(n):
    if n > 0:
        digits = int(math.log10(n))+1
    elif n == 0:
        digits = 1
    else:
        digits = int(math.log10(-n))+2 # +1 if you don't count the '-'

    return digits

def file_exists(entries, filepath):

    for e in entries:
        if e["type"] == "directory":
            if file_exists(e["children"], filepath):
                return True
        else:
            if e["name"] == filepath:
                return True

    return False

def find_deltas(local_fps, server_fps):
    #  offset, size, fingerprint
    # (0, 6018, 16016700538401195842),
    # (6018, 4660, 4907972708255653084),
    # (10678, 46013, 4704480773185713968)

    s = set(h[2] for h in server_fps)

    deltas = [ e for e in local_fps if e[2] not in s ]

    #if len(deltas) > 2:
    #    deltas = [ (d[0], d[1]) for d in deltas ]
    #    #deltas = compact_deltas(deltas)

    return deltas

#def compact_deltas(deltas):
#    #  offset, size, fingerprint
#    # (6018, 4660, 4907972708255653084),
#    # (10678, 46013, 4704480773185713968)
#
#    compact_deltas = []
#    frag_start = None
#    frag_end = None
#
#    for df in deltas:
#        if frag_start is None:
#            frag_start = df[0]
#            frag_end = frag_start + df[1]
#        else:
#            if df[0] == frag_end:
#                frag_end += df[1]
#            else:
#                compact_deltas.append((frag_start, frag_end - frag_start))
#                frag_start = df[0]
#                frag_end = frag_start + df[1]
#
#    compact_deltas.append((frag_start, frag_end - frag_start))
#
#    return compact_deltas

def create_callback(encoder):
    from clint.textui.progress import Bar as ProgressBar

    encoder_len = encoder.len
    bar = ProgressBar(expected_size=encoder_len, filled_char='=')

    def callback(monitor):
        bar.show(monitor.bytes_read)

    return callback

class FileView:
    def __init__(self, filepath, start_offset, read_limit):

        self.fh = open(filepath, "rb")
        self.start_offset = start_offset
        self.read_limit = read_limit

        self.current_offset = start_offset
        self.amount_seen = 0
        self.len = read_limit

    def __del__(self):
        self.fh.close()

    def read(self, amount=-1):
        if self.amount_seen >= self.read_limit:
            self.len = 0
            return b''

        self.fh.seek(self.current_offset)

        remaining_amount = self.read_limit - self.amount_seen

        to_read = remaining_amount if amount < 0 else min(amount, remaining_amount)

        data = self.fh.read(to_read)

        self.amount_seen += len(data)
        self.current_offset = self.start_offset + self.amount_seen 
        self.len = self.read_limit - self.amount_seen

        return data


def remote_replace(repo_url, draft_id, filepath):
    # step 1. get the dataset's fingerprints from the server
    req_url = repo_url + "/drafts/" + draft_id + "/fingerprints"

    print("    Fetching fingerprints from server...")
    r = requests.get(req_url)

    server_fps = None

    if r.status_code != 200:
        print("No draft with ID '" + draft_id + "' found in the remote repository")
        print("It may have been deleted by another user.")
        print("Please check and try again.")
        sys.exit(1)

    stream = b'';

    for chunk in r.iter_content(4096):
        stream += chunk

    server_fps = _pickle.loads(stream)

    # step 2. compute the filepath's fingerprints
    print("    Computing fingerprints for local copy...")
    local_fps = librp.get_file_fingerprints(filepath)

    # step 3. compare the fingerprints for differences
    print("    Comparing fingerprints and computing deltas...")
    deltas = find_deltas(local_fps, server_fps[filepath])

    if len(deltas) == 0:
        print("No differences found between local and remote files...")
        return

    # XXX instead of using Python's pickle, this would be better with BSON or
    # something similar
    metadata = _pickle.dumps(local_fps)


    # step 4. upload differing fragments from the local file
    fields = [
        ("fingerprints", ("filepath" + ".fps", metadata, "application/octet-stream"))
    ]

    # statistics
    bytes_in_file = os.path.getsize(filepath)
    bytes_to_transfer = 0

    for i, d in enumerate(deltas):
        offset = d[0]
        size = d[1]

        bytes_to_transfer += size

        fields.append(
            ("parts",
            (filepath + ".__part_" + str(offset) + "_" + str(size) + "__",
            FileView(filepath, offset, size),
            "application/octet-stream"
            ))
        )

    multipart_data = MultipartEncoder(
        fields = fields
    )

    callback = create_callback(multipart_data)

    monitor = MultipartEncoderMonitor(multipart_data, callback)

    # the user-provided filename is passed using the content-disposition
    # HTTP header
    headers = {"Content-Type": multipart_data.content_type,
                "content-disposition": "attachment; filename=" + filepath}

    req_url = repo_url + "/drafts/" + draft_id + "?replace=true"
    r = requests.put(req_url, headers=headers, data=monitor)

    print('\nUpload finished! (Returned status {0} {1})'.format(
        r.status_code, r.reason)
        )

    print(bytes_to_transfer, "bytes transferred from a total of", bytes_in_file) 


def remote_upload(repo_url, draft_id, filepath):

    req_url = repo_url + "/drafts/" + draft_id

    print("uploading file...")

    bytes_in_file = os.path.getsize(filepath)

    with open(filepath, "rb") as infile:

        filename = os.path.basename(filepath)

        multipart_data = MultipartEncoder(
                fields = {"file": (filename, infile, "application/octet-stream")
        })

        callback = create_callback(multipart_data)

        monitor = MultipartEncoderMonitor(multipart_data, callback)

        # the user-provided filename is passed using the content-disposition
        # HTTP header
        headers = {"Content-Type": multipart_data.content_type,
                   "content-disposition": "attachment; filename=" + filename}

        r = requests.put(req_url, headers=headers, data=monitor)

        print('\nUpload finished! (Returned status {0} {1})'.format(
            r.status_code, r.reason)
            )

        print(bytes_in_file, "bytes transferred") 

def upload_file(repo_url, draft_id, filepath):
    #print(repo_url, draft_id, filepath)

    print("Uploading file to remote repository:")
    print("    repository:", repo_url)
    print("    filepath:", filepath)
    print("    draft:", draft_id)

    if "http" not in repo_url:
        repo_url = "http://" + repo_url

    repo_url += "/api/" + __api_version__

    # step 0. check if the file is already in the server
    req_url = repo_url + "/drafts/" + draft_id + "/record"

    r = requests.get(req_url)

    if r.status_code == 404:
        print("ERROR: No draft with ID '" + draft_id + "' found in the remote repository")
        sys.exit(1)

    if r.status_code != 200:
        print("ERROR: Unknown error returned by repository")
        sys.exit(1)

    # draft exists
    tmp = r.json()

    if "draft" not in tmp:
        print("ERROR: malformed JSON description for draft")
        sys.exit(1)

    draft = tmp["draft"]

    if not file_exists(draft["contents"], filepath):
        print("File '" + filepath + "' not found in server... uploading")
        remote_upload(repo_url, draft_id, filepath)
    else:
        print("File '" + filepath + "' found in server... replacing")
        remote_replace(repo_url, draft_id, filepath)


    sys.exit(0)

def help():
    print("Usage:", os.path.basename(sys.argv[0]), "<URL> <DID> <filepath>")
    print("Arguments:")
    print("    <URL> - repository url")
    print("    <DID> - draft ID")
    print("    <filepath> - file to upload")

if __name__ == "__main__":

    if len(sys.argv) < 1 or len(sys.argv) != 4:
        help()
        sys.exit(1)

    repo_url = sys.argv[1]
    draft_id = sys.argv[2]
    filepath = sys.argv[3]

    upload_file(repo_url, draft_id, filepath)
