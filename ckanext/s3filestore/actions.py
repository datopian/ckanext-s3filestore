import os
from ckan.types import Context, DataDict, AuthResult
from ckan.model.types import make_uuid
import ckan.plugins.toolkit as tk
import ckan.logic as logic
import ckan.lib.uploader as uploader

ValidationError = logic.ValidationError

def get_signed_url(context: Context, data_dict: DataDict) -> AuthResult:
    package_id = data_dict.get("package_id", None)
    filename = data_dict.get("filename", None)
    if package_id is None:
        raise ValidationError(
            {"package_id": "Package ID is required"}
        )
    if filename is None:
        raise ValidationError(
            {"filename": "Filename is required"})
    url_type = 'upload'
    id = make_uuid()
    upload = uploader.get_resource_uploader({
        "package_id": package_id,
        "resource_id": id,
        "url_type": url_type,
    })
    key_path = upload.get_path(id, filename)
    if upload.owner_org:
        key_path = os.path.join(upload.owner_org, key_path)
    print("Generated signed URL for:", key_path, flush=True)
    print(key_path, flush=True)
    return {
        "resource_id": id,
        "signed_url": upload.generate_put_presigned_url(key_path)
    }


