#!/user/bin/env python
"""
@Time   : 2022-02-24 11:12
@Author : LFY
@File   : route_handler.py
"""

# here put the import lib
import json
import logging
import re
from urllib.parse import parse_qs, urljoin, urlparse

import playwright.async_api

from config.crawl_config import *
from crawler.models.nav_graph import NavigationGraph
from crawler.models.request import Request
from crawler.models.url import URL
from crawler.utils import *

_METHOD_OVERRIDE_KEYS = {"_method"}
_METHOD_OVERRIDE_HEADERS = {"x-http-method-override", "x-http-method", "x-method-override"}
_SUPPORTED_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD", "FETCH", "TRACE"}

# ---------------------------------------------------------------------------
# gRPC-web → REST transcoding for nav graph recording
# ---------------------------------------------------------------------------
_GRPC_WEB_CONTENT_TYPES = frozenset({"application/grpc-web+proto", "application/grpc-web"})

# Maps gRPC-web paths to equivalent Memos REST API endpoints.
# path_field: int or tuple of ints — proto field path yielding the resource name for URL building.
# stub_body: JSON body to record for POST/PATCH requests (BACScan token insertion target).
_GRPC_TO_REST_MAP: dict[str, dict] = {
    "/memos.api.v1.WorkspaceService/GetWorkspaceProfile": {"method": "GET", "path": "/api/v1/workspace/profile"},
    "/memos.api.v1.WorkspaceSettingService/GetWorkspaceSetting": {"method": "GET", "path": "/api/v1/workspace/setting"},
    "/memos.api.v1.WorkspaceSettingService/SetWorkspaceSetting": {
        "method": "PATCH", "path": "/api/v1/workspace/setting",
        "stub_body": {"generalSetting": {}},
    },
    "/memos.api.v1.MemoService/ListMemos": {"method": "GET", "path": "/api/v1/memos"},
    "/memos.api.v1.MemoService/GetMemo": {
        "method": "GET", "path_template": "/api/v1/{name}", "path_field": 1,
    },
    "/memos.api.v1.MemoService/CreateMemo": {
        "method": "POST", "path": "/api/v1/memos",
        "stub_body": {"content": "test", "visibility": "PRIVATE"},
    },
    "/memos.api.v1.MemoService/UpdateMemo": {
        # field 1 = Memo sub-message; field 1 within that = name string
        "method": "PATCH", "path_template": "/api/v1/{name}", "path_field": (1, 1),
        "stub_body": {"content": "test"},
    },
    "/memos.api.v1.MemoService/DeleteMemo": {
        "method": "DELETE", "path_template": "/api/v1/{name}", "path_field": 1,
    },
    "/memos.api.v1.UserService/ListUsers": {"method": "GET", "path": "/api/v1/users"},
    "/memos.api.v1.UserService/GetUser": {
        "method": "GET", "path_template": "/api/v1/{name}", "path_field": 1,
    },
    "/memos.api.v1.UserService/CreateUser": {
        "method": "POST", "path": "/api/v1/users",
        "stub_body": {"username": "testuser", "role": "USER", "password": "Test1234!"},
    },
    "/memos.api.v1.UserService/UpdateUser": {
        # field 1 = User sub-message; field 1 within that = name string
        "method": "PATCH", "path_template": "/api/v1/{name}", "path_field": (1, 1),
        "stub_body": {"nickname": "test"},
    },
    "/memos.api.v1.UserService/DeleteUser": {
        "method": "DELETE", "path_template": "/api/v1/{name}", "path_field": 1,
    },
    "/memos.api.v1.ResourceService/ListResources": {"method": "GET", "path": "/api/v1/resources"},
    "/memos.api.v1.ResourceService/GetResource": {
        "method": "GET", "path_template": "/api/v1/{name}", "path_field": 1,
    },
    "/memos.api.v1.ResourceService/DeleteResource": {
        "method": "DELETE", "path_template": "/api/v1/{name}", "path_field": 1,
    },
}

_GRPC_STRIP_HEADERS = frozenset({
    "content-type", "te", "grpc-timeout", "grpc-accept-encoding", "content-length",
})

# When a gRPC GET path is intercepted, also inject REST stubs for these write paths.
# The source GET request's proto body is used to extract the resource ID for path templates.
_GRPC_INFER_WRITES: dict[str, list[str]] = {
    "/memos.api.v1.MemoService/ListMemos": [
        "/memos.api.v1.MemoService/CreateMemo",
    ],
    "/memos.api.v1.MemoService/GetMemo": [
        "/memos.api.v1.MemoService/UpdateMemo",
        "/memos.api.v1.MemoService/DeleteMemo",
    ],
    "/memos.api.v1.UserService/ListUsers": [
        "/memos.api.v1.UserService/CreateUser",
    ],
    "/memos.api.v1.UserService/GetUser": [
        "/memos.api.v1.UserService/UpdateUser",
        "/memos.api.v1.UserService/DeleteUser",
    ],
    "/memos.api.v1.ResourceService/GetResource": [
        "/memos.api.v1.ResourceService/DeleteResource",
    ],
}


def _is_grpc_web(request: playwright.async_api.Request) -> bool:
    ct = (request.headers.get("content-type") or "").lower()
    return any(ct.startswith(g) for g in _GRPC_WEB_CONTENT_TYPES)


def _decode_varint(data: bytes, pos: int):
    result = shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift; shift += 7
        if not (b & 0x80):
            break
    return result, pos


def _grpc_web_decode_field(raw: bytes, field_path) -> "str | None":
    """Extract a UTF-8 string from a gRPC-web binary frame at the given field path.

    field_path is an int (top-level field) or a tuple of ints (nested path).
    Returns None if the field is absent, not a UTF-8 string, or decoding fails.
    """
    if not raw or len(raw) < 5:
        return None
    msg_len = int.from_bytes(raw[1:5], "big")
    data = raw[5:5 + msg_len]
    if isinstance(field_path, int):
        field_path = (field_path,)

    def _extract(buf: bytes, path: tuple) -> "str | None":
        target = path[0]
        pos = 0
        while pos < len(buf):
            try:
                tag, pos = _decode_varint(buf, pos)
            except Exception:
                return None
            fn, wt = tag >> 3, tag & 0x7
            if wt == 0:
                _, pos = _decode_varint(buf, pos)
            elif wt == 2:
                try:
                    length, pos = _decode_varint(buf, pos)
                except Exception:
                    return None
                chunk = buf[pos:pos + length]; pos += length
                if fn == target:
                    if len(path) == 1:
                        try:
                            return chunk.decode("utf-8")
                        except UnicodeDecodeError:
                            return None
                    return _extract(chunk, path[1:])
            elif wt == 1:
                pos += 8
            elif wt == 5:
                pos += 4
            else:
                break
        return None

    return _extract(data, field_path)


def _grpc_to_rest_req(
    request: playwright.async_api.Request,
    req_obj: "Request",
) -> "Request | None":
    """Build a REST Request equivalent for a gRPC-web request, or return None."""
    if not _is_grpc_web(request):
        return None
    path = urlparse(request.url).path
    mapping = _GRPC_TO_REST_MAP.get(path)
    if mapping is None:
        return None

    raw = request.post_data_buffer or b""
    rest_method = mapping["method"]
    base_origin = request.url[: request.url.find(path)]

    if "path_template" in mapping:
        field_val = _grpc_web_decode_field(raw, mapping["path_field"])
        rest_path = mapping["path_template"].format(name=field_val or "")
    else:
        rest_path = mapping["path"]
    rest_url = base_origin + rest_path

    rest_headers = {k: v for k, v in req_obj.headers.items() if k.lower() not in _GRPC_STRIP_HEADERS}
    if rest_method in ("POST", "PUT", "PATCH"):
        rest_headers["content-type"] = "application/json"

    body_bytes = None
    if "stub_body" in mapping and rest_method in ("POST", "PUT", "PATCH"):
        try:
            body_bytes = json.dumps(mapping["stub_body"], ensure_ascii=False).encode()
        except Exception:
            body_bytes = b"{}"

    return Request(
        rest_url,
        method=rest_method,
        headers=rest_headers,
        post_data=body_bytes,
        redirect_flag=req_obj.redirect_flag,
        base_url=req_obj.base_url,
    )


def _grpc_infer_write_reqs(
    request: playwright.async_api.Request,
    req_obj: "Request",
    navgraph: "NavigationGraph",
) -> "list[Request]":
    """Return REST write-operation stubs inferred from an intercepted gRPC GET request.

    Only produced for non-visitor roles.  The resource ID (if needed for a path
    template) is decoded from the source GET request's proto body.
    """
    role = getattr(navgraph, "role", None) or ""
    if role.lower() in ("visitor", ""):
        return []

    path = urlparse(request.url).path
    write_paths = _GRPC_INFER_WRITES.get(path)
    if not write_paths:
        return []

    raw = request.post_data_buffer or b""
    base_origin = request.url[: request.url.find(path)]

    # Extract the resource name/ID from the source (GET) request's proto body.
    read_mapping = _GRPC_TO_REST_MAP.get(path, {})
    read_path_field = read_mapping.get("path_field")
    source_name: "str | None" = (
        _grpc_web_decode_field(raw, read_path_field) if read_path_field is not None else None
    )

    results: list[Request] = []
    for write_path in write_paths:
        write_mapping = _GRPC_TO_REST_MAP.get(write_path)
        if write_mapping is None:
            continue

        write_method = write_mapping["method"]

        if "path_template" in write_mapping:
            if not source_name:
                continue
            rest_path = write_mapping["path_template"].format(name=source_name)
        else:
            rest_path = write_mapping["path"]

        rest_url = base_origin + rest_path
        rest_headers = {k: v for k, v in req_obj.headers.items() if k.lower() not in _GRPC_STRIP_HEADERS}
        if write_method in ("POST", "PUT", "PATCH"):
            rest_headers["content-type"] = "application/json"

        body_bytes: "bytes | None" = None
        if "stub_body" in write_mapping and write_method in ("POST", "PUT", "PATCH"):
            try:
                body_bytes = json.dumps(write_mapping["stub_body"], ensure_ascii=False).encode()
            except Exception:
                body_bytes = b"{}"

        results.append(Request(
            rest_url,
            method=write_method,
            headers=rest_headers,
            post_data=body_bytes,
            redirect_flag=req_obj.redirect_flag,
            base_url=req_obj.base_url,
        ))
    return results


def _should_continue_duplicate_runtime_request(
    request: playwright.async_api.Request,
    req_obj: Request,
) -> bool:
    method = (req_obj.method or request.method or "").upper()
    return not request.is_navigation_request() and method == "GET" and request.resource_type in {"xhr", "fetch"}


def _get_method_override(method: str, headers, post_data, url: str) -> str:
    if not method:
        return method
    if not headers and not post_data and not url:
        return method

    override = None
    if isinstance(headers, dict):
        for k, v in headers.items():
            if k.lower() in _METHOD_OVERRIDE_HEADERS:
                override = v
                break

    if override is None and url:
        try:
            query_params = parse_qs(urlparse(url).query, keep_blank_values=True)
        except Exception:
            query_params = {}
        for key in _METHOD_OVERRIDE_KEYS:
            if key in query_params:
                values = query_params.get(key)
                if values:
                    override = values[0]
                break

    if override is None and post_data:
        try:
            if isinstance(post_data, (bytes, bytearray)):
                body = post_data.decode(errors="ignore")
            else:
                body = str(post_data)
        except Exception:
            body = ""

        if body:
            content_type = ""
            if isinstance(headers, dict):
                for k, v in headers.items():
                    if k.lower() == "content-type":
                        content_type = v
                        break
            if "application/json" in content_type:
                try:
                    data = json.loads(body)
                except Exception:
                    data = None
                if isinstance(data, dict):
                    for key in _METHOD_OVERRIDE_KEYS:
                        if key in data:
                            override = data.get(key)
                            break
            else:
                params = parse_qs(body, keep_blank_values=True)
                for key in _METHOD_OVERRIDE_KEYS:
                    if key in params:
                        values = params.get(key)
                        if values:
                            override = values[0]
                        break

    if override:
        override = str(override).strip().upper()
        if override in _SUPPORTED_METHODS:
            return override
    return method


async def vuln_route_handler(route: playwright.async_api.Route, main_req: Request):

    await route.continue_(
        method=main_req.method.upper(),
        headers=main_req.headers,
        post_data=bytes(main_req.post_data, "utf-8") if main_req.post_data else None,
    )
    req = route.request
    print(type(main_req.headers))
    logging.debug(f"Request continued with main request info: {main_req.url}")


async def route_handler(
    route: playwright.async_api.Route, main_req: Request, collected_urls: set[Request], navgraph: NavigationGraph = None
):
    """
    Deal with every request

    1. backend 302 - with content
        - request target and add Request object to filter with response

    2. backend 302 - without content
        - request target and add Request object to filter without response

    3. frontend navigation
        - return 204 and add navigation's Request object to filter

    :param main_req: main_frame req
    :param route: interceptor
    :param collected_urls: collected_urls
    :return:
    """
    # 对不同host的请求直接abort
    request = route.request
    if not is_same_host_without_port(main_req.url, request.url):
        logging.debug(f"[+] Abort by host rule: {request.url}")
        if request.is_navigation_request():
            await route.fulfill(status=204)
        else:
            await route.abort()
        return
    # 对url中包含一下关键的请求abort
    if check_error_request(request):
        logging.debug(f"[+] Abort by url'key: {request.url}")
        await route.abort()
        return

    # TODO
    #  https://github.com/microsoft/playwright/issues/9648
    #  chromium can not identify the post_data of multipart/form-data
    original_method = request.method.upper() if request.method else request.method
    req_obj = get_request_object(request, main_req.url, main_req.redirect_flag)

    if req_obj.url is None:
        await route.abort()
        return
    if req_obj.url != main_req.url:
        req_obj.from_url = main_req.url

    # Handle gRPC-web: record REST equivalent in nav graph; continue original unchanged.
    if _is_grpc_web(request):
        if navgraph is not None:
            _rest_req = _grpc_to_rest_req(request, req_obj)
            if _rest_req is not None:
                navgraph.record_param_variant(_rest_req)
                if navgraph.should_execute_request(_rest_req):
                    navgraph.add_link(_rest_req)
                    logging.info(f"[grpc→rest] {request.url} → {_rest_req.method} {_rest_req.url}")

            # Inject inferred write-operation stubs derived from this GET request.
            for _write_req in _grpc_infer_write_reqs(request, req_obj, navgraph):
                navgraph.record_param_variant(_write_req)
                if navgraph.should_execute_request(_write_req):
                    navgraph.add_link(_write_req)
                    logging.info(
                        f"[grpc→rest infer] {_write_req.method} {_write_req.url}"
                    )
        # Block protected gRPC-web DELETE operations (record in nav graph but don't execute).
        _protected_grpc = getattr(crawler_config, "PROTECTED_GRPC_DELETE_PATHS", [])
        if _protected_grpc and any(request.url.endswith(p) for p in _protected_grpc):
            logging.info(f"[+] Blocked protected gRPC DELETE (recorded): {request.url}")
            ct = (request.headers.get("content-type") or "application/grpc-web+proto").split(";")[0].strip()
            # Minimal gRPC-web OK frame: empty data frame + status-0 trailer
            _grpc_ok = b"\x00\x00\x00\x00\x00\x80\x00\x00\x00\x10grpc-status: 0\r\n"
            await route.fulfill(status=200, body=_grpc_ok, content_type=ct)
            return
        await route.continue_()
        return

    seq_display = "-"
    if getattr(main_req, "seq", None) is not None:
        seq_display = main_req.seq
    seq_prefix = "[#%s] " % seq_display

    req_label = f"{req_obj.method} {req_obj.url}"
    if navgraph is not None:
        try:
            req_label = navgraph.get_signature(req_obj)
        except Exception:
            req_label = req_label
    if req_obj.method:
        mapped_method = req_obj.method.upper()
        if original_method and mapped_method != original_method:
            logging.info(f"[+] Method override: {original_method} -> {mapped_method} {req_label}")

    # 忽略黑名单关键字请求
    if is_ignored_by_keywords(req_obj.url) and request.resource_type not in crawler_config.RESOURCE_SKIP_TYPES:
        logging.debug(f"[+] Abort by ignore rule: {request.url}")
        await route.abort()
        return

    # 处理所有静态资源请求
    if await resource_handler(route, req_obj):
        logging.debug(f"[+] Abort by media rule: {req_obj.url}")
        return

    if navgraph is not None:
        if request.resource_type not in crawler_config.RESOURCE_SKIP_TYPES:
            ext = URL(req_obj.url).file_ext()
            if ext not in crawler_config.RESOURCE_SKIP_EXTS:
                is_main_doc = request.resource_type == "document" and request.frame.parent_frame is None
                navgraph.record_param_variant(req_obj)
                should_execute = True
                if not is_main_doc:
                    should_execute = navgraph.should_execute_request(req_obj)
                if not should_execute:
                    logging.info(f"[+] Param collect {req_label}")
                    if _should_continue_duplicate_runtime_request(request, req_obj):
                        logging.debug(f"[+] Allow duplicate runtime request: {req_label}")
                        await route.continue_()
                        return
                    await route.fulfill(status=204)
                    return
                if req_obj.method and req_obj.method.upper() != "GET":
                    logging.info(f"[+] Intercepted {req_label}")
                navgraph.add_link(req_obj)

    # TODO HandleHostBinding()

    # 处理前后端跳转请求
    if request.is_navigation_request():
        if (
            request.resource_type == "document"
            and request.frame.parent_frame is None
            and is_same_url_with_fragment(main_req.url, req_obj.url)
        ):
            await main_frame_handler(route, main_req)
            return
        elif request.method == RequestMethod.POST.value and request.frame.parent_frame is not None:
            collected_urls.add(req_obj)
            await route.continue_()
            return
        else:
            logging.debug(
                f"[+] Frontend navigate:{req_obj.url} {req_obj.method} main_frame:{main_req.url} resource:{request.resource_type}"
            )
            collected_urls.add(req_obj)
            await route.fulfill(status=204)
            return

    # Block DELETE requests that would destroy user accounts during crawling.
    # The request is already recorded in the nav_graph above; we fake a 200 so the UI
    # doesn't crash, but we never let the actual deletion reach the server.
    if req_obj.method and req_obj.method.upper() == "DELETE":
        protected_patterns = getattr(crawler_config, "PROTECTED_DELETE_URL_PATTERNS", [])
        url_path = req_obj.url or ""
        if any(re.search(pat, url_path) for pat in protected_patterns):
            logging.info(f"[+] Blocked protected DELETE (recorded): {url_path}")
            await route.fulfill(status=200, body=b"{}", content_type="application/json")
            return

    # 默认continue_发出请求
    logging.debug(f"[+] Continue_: {req_obj.url} resource:{request.resource_type}")
    collected_urls.add(req_obj)
    await route.continue_()


async def main_frame_handler(route: playwright.async_api.Route, main_req: Request):
    if main_req.redirect_flag is True:
        response = main_req.request()
        await route.fulfill(
            status=200,
            body=response,
        )
    elif route.request.method.upper() == RequestMethod.GET.value and main_req.method.upper() != RequestMethod.GET.value:
        await route.continue_(method=main_req.method.upper(), headers=main_req.headers, post_data=main_req.post_data)
    else:
        await route.continue_()


def get_request_object(request: playwright.async_api.Request, base_url: str, redirect_flag: bool = False) -> Request:
    """
    Assemble the request object by main_req and current request.
    If current request is main frame navigation, we should add main_req's data into req_obj

    :param redirect_flag:
    :param request:
    :param base_url:
    :return:
    """
    post_data = None
    if request.post_data_buffer is not None:
        post_data = request.post_data_buffer
    elif post_data is not None:
        post_data = request.post_data
    url = format_url(request.url, base_url)
    main_url = urlparse(base_url)
    if is_same_url(url, base_url) and main_url.fragment != "":
        logging.debug(f"[+] Combine with #: url {url} main_url {base_url}")
        url = urljoin(url, "#" + main_url.fragment)
    method = _get_method_override(request.method, request.headers, post_data, request.url)
    return Request(
        url, method=method, headers=request.headers, post_data=post_data, redirect_flag=redirect_flag, base_url=base_url
    )


async def resource_handler(route: playwright.async_api.Route, req_obj: Request) -> bool:
    """
    Intercept all resource request

    :param route:
    :param req_obj:
    :return:
    """
    if route.request.resource_type == "image":
        await route.fulfill(status=200, content_type="image/png", body=get_minimal_img())
        return True
    if URL(req_obj.url).file_ext().endswith(tuple(STATIC_SUFFIX)) or route.request.resource_type == "media":
        await route.abort()
        return True
    return False


async def request_handler(request: playwright.async_api.Request, collected_urls: set):
    """
    For backend navigation

    :param request:
    :param collected_urls:
    :return:
    """
    # if any(f in request.url for f in config.FORBIDDEN_URL):
    #     return

    if request.redirected_from is not None:
        req_obj = get_request_object(request.redirected_from, request.redirected_from.url, redirect_flag=True)
        if req_obj.url is not None:
            collected_urls.add(req_obj)
        nav_req_repeat = get_request_object(request, request.url)
        if nav_req_repeat.url is not None:
            collected_urls.add(nav_req_repeat)
