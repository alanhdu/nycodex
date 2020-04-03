from typing import Any, Dict

import dateutil.parser
import pytz

from nycodex import db

MAPPING = {
    "calendar date": db.DataType.calendar_date,
    "calendar_date": db.DataType.calendar_date,
    "checkbox": db.DataType.checkbox,
    "date": db.DataType.date,
    "multiline": db.DataType.multi_line,
    "multipolygon": db.DataType.multi_polygon,
    "number": db.DataType.number,
    "point": db.DataType.point,
    "text": db.DataType.text,
    "url": db.DataType.url,
}


def json_to_dataset(result: Dict[str, Any]) -> db.Dataset:
    resource = result["resource"]

    fields = [
        db.Field(
            dataset_id=resource["id"],
            name=name,
            datatype=MAPPING[datatype.lower()],
            description=description,
            field_name=field_name,
        )
        for name, field_name, description, datatype in zip(
            resource["columns_name"],
            resource["columns_field_name"],
            resource["columns_description"],
            resource["columns_datatype"],
        )
    ]

    return db.Dataset(
        asset_type=resource["type"],
        created_at=resource["createdAt"],
        description=resource["description"],
        fields=fields,
        id=resource["id"],
        is_official=resource["provenance"] == "official",
        name=resource["name"],
        updated_at=dateutil.parser.parse(resource["updatedAt"]).astimezone(
            pytz.utc
        ),
    )
