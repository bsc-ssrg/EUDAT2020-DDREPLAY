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

from ddreplay import app
from flask import jsonify, abort, make_response, request, redirect, Response, send_file
from webargs.flaskparser import use_args, use_kwargs, parser
from marshmallow import fields
import datetime as dt
import json
json.JSONEncoder.default = lambda self,obj: (obj.isoformat() if isinstance(obj, datetime.datetime) else None)

import datetime
from time import mktime
from collections import OrderedDict
import zipstream
import re

class CustomEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)

def json_response(data, status):

    return Response(
            response=(json.dumps(data, indent=2, separators=(', ', ': ')), '\n'),
            status = status,
            mimetype="application/json"
    )

def get_repo():
    import ddreplay
    return ddreplay.repo

################################################################################
##### Error management                                                     #####
################################################################################

@app.errorhandler(400)
def bad_request(error):
    return make_response(
            jsonify({'error' : 'Bad request (missing arguments?)'}), 400)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error' : 'Not found'}), 404)

@app.errorhandler(409)
def destination_conflict(error):
    return make_response(
            jsonify({'error' : error.description}), 409)


################################################################################
##### API (drafts)                                                         #####
################################################################################

__api_version__ = "v1.1"

@app.route("/api/" + __api_version__ + "/drafts/")
def get_draft_list():
    """ generate a JSON record with a list of all registered drafts """

    repo = get_repo()

    result = list(repo.list_all_drafts())

    return json_response({'drafts' : result}, 200)

@app.route("/api/" + __api_version__ + "/drafts/", methods=['POST'])
def create_empty_draft():
    """ create a new (empty) draft """

    repo = get_repo()

    new_draft = repo.create_empty_draft({
        "id": repo.generate_DID(),
        "created_at": dt.datetime.now(),
        "PID" : None,
        "contents": []
    })

    return json_response({"draft" : new_draft}, 201)

@app.route("/api/" + __api_version__ + "/drafts/<DID>/record")
def get_draft_record(DID):
    """ generate a JSON record of the draft with DID """

    repo = get_repo()

    draft, _, _ = repo.lookup_draft(DID)

    if(draft is None):
        abort(404)

    return json_response({"draft" : draft}, 200)

@app.route("/api/" + __api_version__ + "/drafts/<DID>")
def get_draft_data(DID):
    """ download the associated data of the draft with DID """

    repo = get_repo()

    draft, data_path, _ = repo.lookup_draft(DID, fetch_data=True)

    if(draft is None):
        abort(404)

    pkg = _build_package(data_path)

    response = Response(pkg, mimetype='application/zip')
    response.headers['Content-Disposition'] = 'attachment; filename={}'.format(DID + '.zip')
    return response

add_to_draft_args = {
    'unpack' : fields.Boolean(required=False, missing=False),
    'replace' : fields.Boolean(required=False, missing=False),
}

@app.route("/api/" + __api_version__ + "/drafts/<DID>", methods=['PUT'])
@app.route("/api/" + __api_version__ + "/drafts/<DID>/<path:usr_path>", methods=['PUT'])
@use_kwargs(add_to_draft_args)
def add_to_draft(DID, unpack, replace, usr_path=None):
    """ add data to an existing draft """

    app.logger.debug("add_to_draft(DID=%s, unpack=%s, replace=%s, usr_path='%s')", DID, unpack, replace, usr_path)

    repo = get_repo()

    draft, _, _ = repo.lookup_draft(DID)

    if(draft is None):
        app.logger.debug("DID: %s not found", DID)
        abort(404)

    # the client should have provided a desired filename using the 
    # content-disposition HTTP header. If they didn't, 
    filenames = re.findall("filename=(.+)", request.headers['content-disposition'])

    if len(filenames) == 0 or len(filenames) != 1:
        abort(400)

    filename = filenames[0]

    # stream the actual file contents from the request and save them
    # in the repository as temporary data
    import shutil
    try:
        result = repo.add_file_to_draft(draft, request.files, filename, usr_path, unpack, replace)
    except shutil.Error: #XXX use our own exceptions
        abort(409, 'Destination path already exists')

    return json_response({'draft': result}, 200)

@app.route("/api/" + __api_version__ + "/drafts/<DID>", methods=['DELETE'])
@app.route("/api/" + __api_version__ + "/drafts/<DID>/<path:usr_path>", methods=['DELETE'])
def delete_draft(DID, usr_path=None):
    """ remove an existing draft """

    repo = get_repo()

    app.logger.debug("delete_draft(DID=%s, usr_path='%s')", DID, usr_path)

    repo.delete_draft(DID)

    # This response is special in the sense that it does not send back a JSON
    # message
    return ('', 204)


publish_draft_args = {
    'author' : fields.String(required=True, missing=None),
    'message' : fields.String(required=True, missing=None),
}

@app.route("/api/" + __api_version__ + "/drafts/<DID>/publish", methods=['POST', 'GET']) #XXX remove GET
@use_kwargs(publish_draft_args)
def publish_draft(DID, author, message):
    """ create a new version from an existing draft """

    if author is None or message is None:
        abort(400)

    app.logger.debug("publish_draft(DID=%s, author=%s, message=%s)", DID, author, message)

    repo = get_repo()

    draft, _, fps = repo.lookup_draft(DID)

    if(draft is None):
        abort(404)
        
    version = repo.publish_draft(draft, author, message)

    if version is None:
        abort(409, 'This draft cannot be published as a new version because '
                   'another version of the dataset was published after it was '
                   'created.')
    else:
        return json_response({'version' : version}, 201)


@app.route("/api/" + __api_version__ + "/drafts/<DID>/fingerprints", methods=['GET'])
def get_fingerprints(DID):
    """ get the Rabin fingerprints from an existing file """

    repo = get_repo()

    draft, _, fps_path = repo.lookup_draft(DID, fetch_fingerprints=True)

    if(draft is None):
        abort(404)

    response = Response(open(fps_path, "rb"))#, "application/octet-stream")
    response.headers['Content-Disposition'] = 'attachment; filename={}'.format(DID + '.fps')

    return response


################################################################################
##### API (datasets + versions)                                            #####
################################################################################

@app.route("/api/" + __api_version__ + "/datasets/")
def get_dataset_list():
    """ generate a JSON record with a list of all registered datasets """

    repo = get_repo()

    result = list(repo.list_all_datasets())

    return json_response({'datasets' : result}, 200)

@app.route("/api/" + __api_version__ + "/datasets/<PID>/record", methods=['GET'])
def get_current_version_record(PID):
    """ get the current (i.e. latest) version version from the dataset
        referenced by <PID>
    """

    repo = get_repo()

    version,_ = repo.lookup_current_version(PID)

    return json_response({'version' : version}, 200)

@app.route("/api/" + __api_version__ + "/datasets/<PID>/", methods=['GET'])
def get_current_version_data(PID):
    """ get the current (i.e. latest) version version from the dataset
        referenced by <PID>
    """

    repo = get_repo()

    version, data_path = repo.lookup_current_version(PID, fetch_data=True)

    if(version is None):
        abort(404)

    pkg = _build_package(data_path)

    response = Response(pkg, mimetype='application/zip')
    response.headers['Content-Disposition'] = 'attachment; filename={}'.format(PID + '.zip')
    return response

@app.route("/api/" + __api_version__ + "/datasets/<PID>/versions/<VID>/record")
def get_version_record(PID, VID):
    """ generate a JSON record for the version identified by PID + VID """

    repo = get_repo()

    version,_ = repo.lookup_version(PID, VID)

    if(version is None):
        abort(404)

    return json_response({'version' : version}, 200)

@app.route("/api/" + __api_version__ + "/datasets/<PID>/versions/<VID>/")
def get_version_data(PID, VID):
    """ generate a JSON record for the version identified by PID + VID """

    repo = get_repo()

    version, data_path = repo.lookup_version(PID, VID, fetch_data=True)

    if(version is None):
        abort(404)

    pkg = _build_package(data_path)

    response = Response(pkg, mimetype='application/zip')
    response.headers['Content-Disposition'] = 'attachment; filename={}'.format(VID + '.zip')
    return response

@app.route("/api/" + __api_version__ + "/datasets/<PID>/versions/")
def get_version_list(PID):
    """ generate a JSON record with a list of all registered versions for 
        the dataset identified by <PID>
    """

    repo = get_repo()

    ### FIXME: this should be ordered chronologically
    result = list(repo.list_all_versions(PID))

    return json_response({'versions' : result}, 200)

@app.route("/api/" + __api_version__ + "/datasets/<PID>/", methods=['PUT'])
def create_draft_from_dataset(PID):
    """ create a new draft based on the contents of the dataset referenced by
        <PID>
    """

    repo = get_repo()

    draft = repo.create_draft_from_dataset(PID)

    return json_response({'draft': draft}, 200)


################################################################################
##### Utility functions                                                    #####
################################################################################
def _build_package(data_path):
    """ This function collects all files contained in directory ``data_path``
    and packs them into a zipstream object that can be streamed back to the
    client.
    """

    import os

    pkg = zipstream.ZipFile(mode='w')

    for root, dir, files in os.walk(data_path):
        for f in files:
            file_path = os.path.join(root, f)
            file_alias = os.path.relpath(file_path, data_path)
            pkg.write(file_path, file_alias)

    return pkg

