# encoding: utf-8
import os
import logging
import mimetypes

import flask

from botocore.exceptions import ClientError

from ckantoolkit import config as ckan_config
from ckantoolkit import _, request, c, g
import ckantoolkit as toolkit
import ckan.logic as logic
import ckan.lib.base as base
import ckan.lib.uploader as uploader
from ckan.lib.uploader import get_storage_path

import ckan.model as model

config = toolkit.config

log = logging.getLogger(__name__)

Blueprint = flask.Blueprint
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action
abort = base.abort
redirect = toolkit.redirect_to


s3_resource = Blueprint(
    "s3_resource",
    __name__,
    url_prefix="/dataset/<id>/resource",
    url_defaults={"package_type": "dataset"},
)


def resource_download(package_type, id, resource_id, filename=None):
    """
    Provide a download by either redirecting the user to the url stored or
    downloading the uploaded file from S3.
    """
    context = {
        "model": model,
        "session": model.Session,
        "user": c.user or c.author,
        "auth_user_obj": c.userobj,
    }
    owner_org = None

    try:
        rsc = get_action("resource_show")(context, {"id": resource_id})
        package = get_action("package_show")(context, {"id": id})
        owner_org = package.get("owner_org")
    except NotFound:
        return abort(404, _("Resource not found"))
    except NotAuthorized:
        return abort(401, _("Unauthorized to read resource %s") % id)

    if rsc.get("url_type") == "upload":
        upload = uploader.get_resource_uploader(rsc)
        preview = request.args.get("preview", False)

        if filename is None:
            filename = os.path.basename(rsc["url"])
        key_path = upload.get_path(rsc["id"], filename)
        key = filename

        if owner_org:
            key_path = os.path.join(owner_org, key_path)

        if key is None:
            log.warn(
                "Key '{0}' not found in bucket '{1}'".format(
                    key_path, upload.bucket_name
                )
            )
        try:
            if preview:
                url = upload.get_signed_url_to_key(key_path)
            else:
                params = {
                    "ResponseContentDisposition": "attachment; filename=" + filename,
                }
                url = upload.get_signed_url_to_key(key_path, params)
            return redirect(url)

        except ClientError as ex:
            if ex.response["Error"]["Code"] in ["NoSuchKey", "404"]:
                if rsc.get("key"):
                    s3key_paths = rsc.get("key").split("/")
                    s3key = (
                        "/".join(s3key_paths[1:])
                        if config.get("ckanext.s3filestore.aws_access_key_id")
                        == "minioadmin"
                        else "/".join(s3key_paths)
                    )
                    try:
                        if preview:
                            url = upload.get_signed_url_to_key(s3key)
                        else:
                            params = {
                                "ResponseContentDisposition": "attachment; filename="
                                + filename,
                            }
                            url = upload.get_signed_url_to_key(s3key, params)
                        return redirect(url)
                    except ClientError as ex:
                        return abort(404, _("Resource data not found"))
            else:
                raise ex
    else:
        return redirect(rsc["url"])


def filesystem_resource_download(package_type, id, resource_id, filename=None):
    """
    A fallback view action to download resources from the
    filesystem. A copy of the action from
    `ckan.views.resource:download`.

    Provides a direct download by either redirecting the user to the url
    stored or downloading an uploaded file directly.
    """
    context = {
        "model": model,
        "session": model.Session,
        "user": g.user,
        "auth_user_obj": g.userobj,
    }
    preview = request.args.get("preview", False)

    try:
        rsc = get_action("resource_show")(context, {"id": resource_id})
        get_action("package_show")(context, {"id": id})
    except (NotFound, NotAuthorized):
        return abort(404, _("Resource not found"))

    mimetype, enc = mimetypes.guess_type(rsc.get("url", ""))

    if rsc.get("url_type") == "upload":
        path = get_storage_path()
        storage_path = os.path.join(path, "resources")
        directory = os.path.join(storage_path, resource_id[0:3], resource_id[3:6])
        filepath = os.path.join(directory, resource_id[6:])
        if preview:
            return flask.send_file(filepath, mimetype=mimetype)
        else:
            return flask.send_file(filepath)
    elif "url" not in rsc:
        return abort(404, _("No download is available"))
    return redirect(rsc["url"])


s3_resource.add_url_rule("/<resource_id>/download", view_func=resource_download)
s3_resource.add_url_rule(
    "/<resource_id>/download/<filename>", view_func=resource_download
)
s3_resource.add_url_rule(
    "/<resource_id>/fs_download/<filename>", view_func=filesystem_resource_download
)


def get_blueprints():
    return [s3_resource]
