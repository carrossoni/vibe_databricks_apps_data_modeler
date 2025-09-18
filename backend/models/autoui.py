import sys
from types import NoneType, UnionType
from typing import Any, Dict, Literal, Optional, Union, get_args, get_origin

import annotated_types
from pydantic import BaseModel
from pydantic.fields import FieldInfo, PydanticUndefined


class AutoUIOptions(BaseModel):
    editable: Optional[bool] = None
    width: Optional[int] = None
    continuous_update: Optional[bool] = None
    label: Optional[str] = None
    group: Optional[str] = None


class AutoUIWidgetSpec(BaseModel, arbitrary_types_allowed=True):
    field_name: str
    python_type: str
    widget_cls: str
    widget_params: Dict[str, Any]
    widget_group: str
    optional: bool


def title_from_snake_case(name):
    return ' '.join(word.title() for word in name.split('_'))


def _get_widget_spec(model: BaseModel, field_info: FieldInfo, field_name: str):
    python_type = field_info.annotation
    ui_opts: AutoUIOptions = (field_info.json_schema_extra or {}).get('ui', AutoUIOptions())
    params = {
        "title": field_info.title or title_from_snake_case(field_name),
        "description_tooltip": field_info.description,
        "disabled": False if ui_opts.editable is None else not ui_opts.editable,
        "default_value": field_info.default if field_info.default != PydanticUndefined else None,
    }

    for m in field_info.metadata:
        if isinstance(m, annotated_types.Ge):
            params['min'] = m.ge
        elif isinstance(m, annotated_types.Gt):
            params['min'] = m.gt
        elif isinstance(m, annotated_types.Le):
            params['max'] = m.le
        elif isinstance(m, annotated_types.Lt):
            params['max'] = m.lt
        elif isinstance(m, annotated_types.MultipleOf):
            params['step'] = m.multiple_of

    field_is_optional, inner_python_type = _isoptional(python_type)
    if field_is_optional:
        python_type = inner_python_type

    field_group = ui_opts.group
    if not field_group:
        field_group = 'Extra' if field_is_optional else 'General'

    origin_type = get_origin(python_type)

    widget_cls = None

    if origin_type == Union:
        element_python_type = get_args(python_type)[0]
    else:
        element_python_type = python_type

    if origin_type is Literal:
        params['options'] = get_args(python_type)
        widget_cls = 'Dropdown'
    elif element_python_type is int:
        widget_cls = 'BoundedIntText'
        params['min'] = params.get('min', -sys.maxsize)
        params['max'] = params.get('max', sys.maxsize)
    elif element_python_type is float:
        widget_cls = 'BoundedFloatText'
        params['min'] = params.get('min', -sys.maxsize)
        params['max'] = params.get('max', sys.maxsize)
    elif element_python_type is bool:
        widget_cls = 'Checkbox'
    else:
        widget_cls = 'Text'

    return AutoUIWidgetSpec(field_name=field_name,
                            field_info=field_info,
                            python_type=element_python_type.__name__,
                            requested_ui_options=ui_opts,
                            widget_cls=widget_cls,
                            widget_params=params,
                            widget_group=field_group,
                            optional=field_is_optional)


def _isoptional(python_type: Any):
    origin_type = get_origin(python_type)
    if not origin_type:
        False, None

    if origin_type in [Union, UnionType]:
        args = get_args(python_type)
        for arg in args:
            if arg is NoneType:
                return True, args[0]
        return False, None

    if origin_type is Optional:
        return True, get_args(python_type)

    return False, None


def get_ui_spec(model: BaseModel):
    return [
        _get_widget_spec(model, field_info, field_name)
        for field_name, field_info in model.model_fields.items()
    ]
