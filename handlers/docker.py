"""
Docker Registry 特殊处理器
处理 Docker Registry 的认证、重定向等逻辑
"""
import logging
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from fastapi import Request, Response


def _parse_www_authenticate(authenticate_str: str) -> dict:
    """解析 WWW-Authenticate 头
    示例: Bearer realm="https://auth.docker.io/token",service="registry.docker.io"
    """
    matches = re.findall(r'([a-zA-Z]+)="([^"]*)"', authenticate_str)
    return {k: v for k, v in matches}


def _make_www_authenticate_realm(public_host: str, is_https: bool = True) -> str:
    """构建改写后的 WWW-Authenticate 头，指向代理的 /v2/auth"""
    scheme = "https" if is_https else "http"
    return f'Bearer realm="{scheme}://{public_host}/v2/auth",service="docker-proxy"'


async def _handle_docker_auth(request: Request, upstream: str, http_client) -> Response:
    """处理 Docker Registry 认证请求
    
    1. 向上游 /v2/ 发送请求获取 WWW-Authenticate
    2. 解析 realm 和 service
    3. 代理 token 请求
    """
    # 获取上游的认证信息
    upstream_url = f"{upstream}/v2/"
    headers = {}
    auth_header = request.headers.get('authorization')
    if auth_header:
        headers['Authorization'] = auth_header
    
    resp = await http_client.get(upstream_url, headers=headers, follow_redirects=True)
    
    if resp.status_code != 401:
        # 不需要认证，直接返回
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items() 
                    if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}
        )
    
    www_auth = resp.headers.get('www-authenticate')
    if not www_auth:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items() 
                    if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}
        )
    
    # 解析 WWW-Authenticate
    auth_info = _parse_www_authenticate(www_auth)
    realm = auth_info.get('realm')
    service = auth_info.get('service', '')
    
    if not realm:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items() 
                    if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}
        )
    
    # 构建 token URL
    realm_url = urlparse(realm)
    
    # 获取 scope 参数
    scope = request.query_params.get('scope', '')
    
    # 构建新的查询参数
    query_params = parse_qs(realm_url.query)
    query_params['service'] = [service]
    if scope:
        query_params['scope'] = [scope]
    
    new_query = urlencode(query_params, doseq=True)
    token_url = urlunparse((
        realm_url.scheme,
        realm_url.netloc,
        realm_url.path,
        realm_url.params,
        new_query,
        realm_url.fragment
    ))
    
    logging.info(f"Fetching token from: {token_url}")
    
    # 请求 token
    token_headers = {}
    if auth_header:
        token_headers['Authorization'] = auth_header
    
    token_resp = await http_client.get(token_url, headers=token_headers, follow_redirects=True)
    
    return Response(
        content=token_resp.content,
        status_code=token_resp.status_code,
        headers={k: v for k, v in token_resp.headers.items() 
                if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}
    )


async def exec_path(request: Request, full_path: str, config: dict, http_client) -> tuple:
    """
    Docker Registry 特殊处理逻辑
    
    Args:
        request: FastAPI 请求对象
        full_path: 请求路径
        config: 全局配置
        http_client: HTTP 客户端
    
    Returns:
        tuple: (handled, response)
            - handled: True 表示已处理，不再走后续流程；False 表示未处理，继续后续流程
            - response: 当 handled=True 时返回 Response 对象，否则为 None
    """
    # 获取 public_host 配置
    public_host = config['server'].get('public_host', f"127.0.0.1:{config['server']['port']}")
    
    # 获取 docker registry 上游配置
    docker_upstream = None
    rules = config.get('rules', [])
    for rule in rules:
        if rule.get('name') == "docker-registry":
            docker_upstream = rule.get('upstream')
            break
    
    if not docker_upstream:
        logging.warning("docker-registry rule not found in config")
        return False, None
    
    # 1. 处理 /v2/auth - token 代理
    if full_path == "v2/auth" or full_path.startswith("v2/auth"):
        response = await _handle_docker_auth(request, docker_upstream, http_client)
        return True, response
    
    # 2. 处理 /v2/ - 检查认证并改写 401 响应
    if full_path == "v2/" or full_path == "v2":
        upstream_url = f"{docker_upstream}/v2/"
        headers = {}
        auth_header = request.headers.get('authorization')
        if auth_header:
            headers['Authorization'] = auth_header
        
        resp = await http_client.get(upstream_url, headers=headers, follow_redirects=True)
        
        if resp.status_code == 401:
            # 返回自定义 401，让客户端向我们的 /v2/auth 请求 token
            is_https = request.url.scheme == "https"
            www_auth = _make_www_authenticate_realm(public_host, is_https)
            return True, Response(
                content=b'{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}',
                status_code=401,
                headers={
                    'Content-Type': 'application/json',
                    'WWW-Authenticate': www_auth
                }
            )
        
        # 不需要认证，直接返回上游响应
        return True, Response(
            content=resp.content,
            status_code=resp.status_code,
            headers={k: v for k, v in resp.headers.items() 
                    if k.lower() not in ['content-length', 'transfer-encoding', 'content-encoding']}
        )
    
    # 3. 处理 DockerHub library 镜像重定向
    # /v2/busybox/manifests/latest => /v2/library/busybox/manifests/latest
    # 注意：只处理没有 namespace 的官方镜像（如 ubuntu, nginx 等）
    if full_path.startswith("v2/"):
        path_parts = full_path.split("/")
        # 官方镜像路径格式: v2/{repo}/manifests/{tag} 或 v2/{repo}/blobs/{digest}
        # 即 4 个部分: ['v2', 'repo', 'manifests|blobs', 'reference']
        if len(path_parts) == 4 and path_parts[2] in ['manifests', 'blobs']:
            repo = path_parts[1]
            # 如果 repo 不包含 / 且不是以 library/ 开头，说明是官方镜像需要重定向
            if "/" not in repo and not repo.startswith("library"):
                # 重定向到 library/ 路径
                new_path = f"/v2/library/{repo}/{path_parts[2]}/{path_parts[3]}"
                logging.info(f"Redirecting library image: /{full_path} -> {new_path}")
                return True, Response(
                    status_code=301,
                    headers={"Location": f"{request.url.scheme}://{public_host}{new_path}"}
                )
    
    # 未处理，继续后续流程
    return False, None
