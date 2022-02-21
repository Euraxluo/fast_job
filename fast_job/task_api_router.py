# -*- coding: utf-8 -*- 
# Time: 2021-06-30 19:03
# Copyright (c) 2021
# author: Euraxluo

import typing
from fastapi.dependencies.utils import *
from fastapi.routing import *


def get_typed_annotation(param: inspect.Parameter, globalns: typing.Dict[str, typing.Any]) -> typing.Any:
    annotation = param.annotation
    if isinstance(annotation, str):
        annotation = typing.ForwardRef(annotation)
        annotation = typing.evaluate_forwardref(annotation, globalns, globalns)
    return annotation


def get_typed_signature(call: typing.Callable[..., typing.Any]) -> inspect.Signature:
    signature = inspect.signature(call)
    globalns = getattr(call, "__globals__", {})
    typed_params = [
        inspect.Parameter(
            name=param.name,
            kind=param.kind,
            default=param.default,
            annotation=get_typed_annotation(param, globalns),
        )
        for param in signature.parameters.values()
    ]
    typed_signature = inspect.Signature(typed_params)
    return typed_signature


def new_typed_signature(call: typing.Callable[..., typing.Any], new_params: typing.Mapping[str, typing.Any]) -> inspect.Signature:
    signature = inspect.signature(call)
    globalns = getattr(call, "__globals__", {})
    typed_params = []
    for param in signature.parameters.values():
        if new_params and param.name in new_params:
            param._default = new_params.get(param.name)
        typed_params.append(inspect.Parameter(
            name=param.name,
            kind=param.kind,
            default=param.default,
            annotation=get_typed_annotation(param, globalns),
        ))
    typed_signature = inspect.Signature(typed_params)
    return typed_signature


def new_dependant(
        *,
        path: str,
        call: typing.Callable[..., typing.Any],
        endpoint_signature: inspect.Signature = None,
        name: typing.Optional[str] = None,
        security_scopes: typing.Optional[typing.List[str]] = None,
        use_cache: bool = True,
) -> Dependant:
    path_param_names = get_path_param_names(path)
    if not endpoint_signature:
        endpoint_signature = get_typed_signature(call)
    signature_params = endpoint_signature.parameters
    if is_gen_callable(call) or is_async_gen_callable(call):
        check_dependency_contextmanagers()
    dependant = Dependant(call=call, name=name, path=path, use_cache=use_cache)
    for param_name, param in signature_params.items():
        if isinstance(param.default, params.Depends):
            sub_dependant = get_param_sub_dependant(
                param=param, path=path, security_scopes=security_scopes
            )
            dependant.dependencies.append(sub_dependant)
            continue
        if add_non_field_param_to_dependency(param=param, dependant=dependant):
            continue
        param_field = get_param_field(
            param=param, default_field_info=params.Query, param_name=param_name
        )
        if param_name in path_param_names:
            assert is_scalar_field(
                field=param_field
            ), "Path params must be of one of the supported types"
            if isinstance(param.default, params.Path):
                ignore_default = False
            else:
                ignore_default = True
            param_field = get_param_field(
                param=param,
                param_name=param_name,
                default_field_info=params.Path,
                force_type=params.ParamTypes.path,
                ignore_default=ignore_default,
            )
            add_param_to_fields(field=param_field, dependant=dependant)
        elif is_scalar_field(field=param_field):
            add_param_to_fields(field=param_field, dependant=dependant)
        elif isinstance(
                param.default, (params.Query, params.Header)
        ) and is_scalar_sequence_field(param_field):
            add_param_to_fields(field=param_field, dependant=dependant)
        else:
            field_info = param_field.field_info
            assert isinstance(
                field_info, params.Body
            ), f"Param: {param_field.name} can only be a request body, using Body(...)"
            dependant.body_params.append(param_field)
    return dependant


class TaskAPIRoute(APIRoute):
    def __init__(
            self,
            path: str,
            endpoint: Callable[..., Any],
            *,
            endpoint_params: Optional[Dict[str, Any]] = None,
            response_model: Optional[Type[Any]] = None,
            status_code: int = 200,
            tags: Optional[List[str]] = None,
            dependencies: Optional[Sequence[params.Depends]] = None,
            summary: Optional[str] = None,
            description: Optional[str] = None,
            response_description: str = "Successful Response",
            responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
            deprecated: Optional[bool] = None,
            name: Optional[str] = None,
            methods: Optional[Union[Set[str], List[str]]] = None,
            operation_id: Optional[str] = None,
            response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
            response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
            response_model_by_alias: bool = True,
            response_model_exclude_unset: bool = False,
            response_model_exclude_defaults: bool = False,
            response_model_exclude_none: bool = False,
            include_in_schema: bool = True,
            response_class: Union[Type[Response], DefaultPlaceholder] = Default(
                JSONResponse
            ),
            dependency_overrides_provider: Optional[Any] = None,
            callbacks: Optional[List[BaseRoute]] = None,
    ) -> None:
        # normalise enums e.g. http.HTTPStatus
        if isinstance(status_code, enum.IntEnum):
            status_code = int(status_code)
        self.path = path
        self.endpoint_params = endpoint_params
        self.endpoint = endpoint
        self.name = get_name(endpoint) if name is None else name
        self.path_regex, self.path_format, self.param_convertors = compile_path(path)
        if methods is None:
            methods = ["GET"]
        self.methods: Set[str] = set([method.upper() for method in methods])
        self.unique_id = generate_operation_id_for_path(
            name=self.name, path=self.path_format, method=list(methods)[0]
        )
        self.response_model = response_model
        if self.response_model:
            assert (
                    status_code not in STATUS_CODES_WITH_NO_BODY
            ), f"Status code {status_code} must not have a response body"
            response_name = "Response_" + self.unique_id
            self.response_field = create_response_field(
                name=response_name, type_=self.response_model
            )
            # Create a clone of the field, so that a Pydantic submodel is not returned
            # as is just because it's an instance of a subclass of a more limited class
            # e.g. UserInDB (containing hashed_password) could be a subclass of User
            # that doesn't have the hashed_password. But because it's a subclass, it
            # would pass the validation and be returned as is.
            # By being a new field, no inheritance will be passed as is. A new model
            # will be always created.
            self.secure_cloned_response_field: Optional[
                ModelField
            ] = create_cloned_field(self.response_field)
        else:
            self.response_field = None  # type: ignore
            self.secure_cloned_response_field = None
        self.status_code = status_code
        self.tags = tags or []
        if dependencies:
            self.dependencies = list(dependencies)
        else:
            self.dependencies = []
        self.summary = summary
        self.description = description or inspect.cleandoc(self.endpoint.__doc__ or "")
        # if a "form feed" character (page break) is found in the description text,
        # truncate description text to the content preceding the first "form feed"
        self.description = self.description.split("\f")[0]
        self.response_description = response_description
        self.responses = responses or {}
        response_fields = {}
        for additional_status_code, response in self.responses.items():
            assert isinstance(response, dict), "An additional response must be a dict"
            model = response.get("model")
            if model:
                assert (
                        additional_status_code not in STATUS_CODES_WITH_NO_BODY
                ), f"Status code {additional_status_code} must not have a response body"
                response_name = f"Response_{additional_status_code}_{self.unique_id}"
                response_field = create_response_field(name=response_name, type_=model)
                response_fields[additional_status_code] = response_field
        if response_fields:
            self.response_fields: Dict[Union[int, str], ModelField] = response_fields
        else:
            self.response_fields = {}
        self.deprecated = deprecated
        self.operation_id = operation_id
        self.response_model_include = response_model_include
        self.response_model_exclude = response_model_exclude
        self.response_model_by_alias = response_model_by_alias
        self.response_model_exclude_unset = response_model_exclude_unset
        self.response_model_exclude_defaults = response_model_exclude_defaults
        self.response_model_exclude_none = response_model_exclude_none
        self.include_in_schema = include_in_schema
        self.response_class = response_class

        assert callable(endpoint), "An endpoint must be a callable"
        self.dependant = new_dependant(path=self.path_format, call=endpoint, endpoint_signature=new_typed_signature(endpoint, self.endpoint_params))
        for depends in self.dependencies[::-1]:
            self.dependant.dependencies.insert(
                0,
                get_parameterless_sub_dependant(depends=depends, path=self.path_format),
            )
        self.body_field = get_body_field(dependant=self.dependant, name=self.unique_id)
        self.dependency_overrides_provider = dependency_overrides_provider
        self.callbacks = callbacks
        self.app = request_response(self.get_route_handler())


class TaskAPIRouter(APIRouter):
    def add_task_api_route(
            self,
            path: str,
            endpoint: Callable[..., Any],
            *,
            endpoint_params: Optional[Dict[str, Any]] = None,
            response_model: Optional[Type[Any]] = None,
            status_code: int = 200,
            tags: Optional[List[str]] = None,
            dependencies: Optional[Sequence[params.Depends]] = None,
            summary: Optional[str] = None,
            description: Optional[str] = None,
            response_description: str = "Successful Response",
            responses: Optional[Dict[Union[int, str], Dict[str, Any]]] = None,
            deprecated: Optional[bool] = None,
            methods: Optional[Union[typing.Set[str], List[str]]] = None,
            operation_id: Optional[str] = None,
            response_model_include: Optional[Union[SetIntStr, DictIntStrAny]] = None,
            response_model_exclude: Optional[Union[SetIntStr, DictIntStrAny]] = None,
            response_model_by_alias: bool = True,
            response_model_exclude_unset: bool = False,
            response_model_exclude_defaults: bool = False,
            response_model_exclude_none: bool = False,
            include_in_schema: bool = True,
            response_class: Union[Type[Response], DefaultPlaceholder] = Default(
                JSONResponse
            ),
            name: Optional[str] = None,
            route_class_override: Optional[Type[APIRoute]] = TaskAPIRoute,
            callbacks: Optional[List[BaseRoute]] = None,
    ) -> None:
        route_class = route_class_override or self.route_class
        responses = responses or {}
        combined_responses = {**self.responses, **responses}
        current_response_class = get_value_or_default(
            response_class, self.default_response_class
        )
        current_tags = self.tags.copy()
        if tags:
            current_tags.extend(tags)
        current_dependencies = self.dependencies.copy()
        if dependencies:
            current_dependencies.extend(dependencies)
        current_callbacks = self.callbacks.copy()
        if callbacks:
            current_callbacks.extend(callbacks)
        route = route_class(
            self.prefix + path,
            endpoint=endpoint,
            endpoint_params=endpoint_params,
            response_model=response_model,
            status_code=status_code,
            tags=current_tags,
            dependencies=current_dependencies,
            summary=summary,
            description=description,
            response_description=response_description,
            responses=combined_responses,
            deprecated=deprecated or self.deprecated,
            methods=methods,
            operation_id=operation_id,
            response_model_include=response_model_include,
            response_model_exclude=response_model_exclude,
            response_model_by_alias=response_model_by_alias,
            response_model_exclude_unset=response_model_exclude_unset,
            response_model_exclude_defaults=response_model_exclude_defaults,
            response_model_exclude_none=response_model_exclude_none,
            include_in_schema=include_in_schema and self.include_in_schema,
            response_class=current_response_class,
            name=name,
            dependency_overrides_provider=self.dependency_overrides_provider,
            callbacks=current_callbacks,
        )
        self.routes.append(route)


__all__ = ["TaskAPIRouter"]
