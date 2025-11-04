#!/usr/bin/env python3
"""
Command-line helper for invoking the EDL API.

Usage examples:

python tools/api_cli.py create-config \
    --base-url http://localhost:8100 \
    --profile-id 13cc1742-7cc3-4b47-aaf3-f9d7960bd0c3 \
    --api-key pEddhRNAtgv4bwsWS2OVmTJ46kW3FgMnFgtvoj-q6Jk \
    --sources config/sources.yaml \
    --augment config/augmentor_config.yaml \
    --rules config/rules.yaml \
    --output test_output_data/sample.json \
    --refresh 5 \
    --created-by "CLI demo"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests

PROXY_URL = "http://proxy-nsp.wellsfargo.com:8080"


class ApiClient:
    def __init__(self, base_url: str, api_key: Optional[str], timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._proxies: Optional[Dict[str, str]] = (
            {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
        )

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        req_headers = self._headers()
        if headers:
            req_headers.update(headers)
        response = requests.request(
            method,
            url,
            params=params,
            json=json_body,
            data=data,
            headers=req_headers,
            timeout=self.timeout,
            proxies=self._proxies,
        )
        return response

    # Admin endpoints
    def create_admin(self, name: str, role: str, is_super_admin: bool) -> requests.Response:
        payload = {"name": name, "role": role, "is_super_admin": is_super_admin}
        return self.request("POST", "/admin/accounts", json_body=payload)

    def list_admins(self) -> requests.Response:
        return self.request("GET", "/admin/accounts")

    def link_admin_profile(self, admin_id: str, profile_id: str) -> requests.Response:
        return self.request("POST", f"/admin/accounts/{admin_id}/profiles/{profile_id}")

    def unlink_admin_profile(self, admin_id: str, profile_id: str) -> requests.Response:
        return self.request("DELETE", f"/admin/accounts/{admin_id}/profiles/{profile_id}")

    # Profile endpoints
    def create_profile(self, name: str, description: Optional[str], owner_admin_id: Optional[str]) -> requests.Response:
        payload = {"name": name}
        if description:
            payload["description"] = description
        query = f"?owner_admin_id={owner_admin_id}" if owner_admin_id else ""
        return self.request("POST", f"/profiles{query}", json_body=payload)

    def list_profiles(self) -> requests.Response:
        return self.request("GET", "/profiles")

    # Config endpoints
    def create_profile_config(
        self,
        profile_id: str,
        sources: str,
        augment: Optional[str],
        rules: str,
        settings: Dict[str, Any],
        refresh_minutes: Optional[int],
        created_by: Optional[str],
    ) -> requests.Response:
        payload: Dict[str, Any] = {
            "sources_yaml": sources,
            "rules_yaml": rules,
            "pipeline_settings": settings,
        }
        if augment is not None:
            payload["augment_yaml"] = augment
        if refresh_minutes is not None:
            payload["refresh_interval_minutes"] = refresh_minutes
        if created_by:
            payload["created_by"] = created_by
        return self.request("POST", f"/profiles/{profile_id}/configs", json_body=payload)

    def update_profile_config(
        self,
        profile_id: str,
        config_id: str,
        sources: Optional[str],
        augment: Optional[str],
        rules: Optional[str],
        settings: Optional[Dict[str, Any]],
        refresh_minutes: Optional[int],
    ) -> requests.Response:
        payload: Dict[str, Any] = {}
        if sources is not None:
            payload["sources_yaml"] = sources
        if augment is not None:
            payload["augment_yaml"] = augment
        if rules is not None:
            payload["rules_yaml"] = rules
        if settings is not None and settings:
            payload["pipeline_settings"] = settings
        if refresh_minutes is not None:
            payload["refresh_interval_minutes"] = refresh_minutes
        return self.request("PATCH", f"/profiles/{profile_id}/configs/{config_id}", json_body=payload)

    def list_profile_configs(self, profile_id: str) -> requests.Response:
        return self.request("GET", f"/profiles/{profile_id}/configs")

    # Pipeline endpoints
    def create_pipeline(
        self,
        profile_id: str,
        profile_config_id: str,
        name: str,
        description: Optional[str],
        concurrency_limit: Optional[int],
        created_by: Optional[str],
    ) -> requests.Response:
        payload: Dict[str, Any] = {
            "profile_id": profile_id,
            "profile_config_id": profile_config_id,
            "name": name,
        }
        if description:
            payload["description"] = description
        if concurrency_limit is not None:
            payload["concurrency_limit"] = concurrency_limit
        if created_by:
            payload["created_by"] = created_by
        return self.request("POST", "/pipelines", json_body=payload)

    def point_pipeline_to_config(self, pipeline_id: str, profile_config_id: str) -> requests.Response:
        payload = {"profile_config_id": profile_config_id}
        return self.request("PATCH", f"/pipelines/{pipeline_id}/config", json_body=payload)

    def delete_pipeline(self, pipeline_id: str) -> requests.Response:
        return self.request("DELETE", f"/pipelines/{pipeline_id}")

    def list_pipelines(self, params: Dict[str, Any]) -> requests.Response:
        return self.request("GET", "/pipelines", params=params)

    def list_profile_pipelines(self, profile_id: str, params: Dict[str, Any]) -> requests.Response:
        return self.request("GET", f"/profiles/{profile_id}/pipelines", params=params)

    def list_active_pipelines(self, profile_id: str) -> requests.Response:
        return self.request("GET", f"/profiles/{profile_id}/pipelines/active")

    def search_pipelines(self, profile_id: str, indicator: str) -> requests.Response:
        params = {"indicator": indicator}
        return self.request("GET", f"/profiles/{profile_id}/pipelines/search", params=params)

    # Run endpoints
    def submit_run(self, pipeline_id: str, overrides: Optional[Dict[str, Any]], requested_by: Optional[str]) -> requests.Response:
        payload: Dict[str, Any] = {"pipeline_id": pipeline_id}
        if overrides:
            payload["overrides"] = overrides
        if requested_by:
            payload["requested_by"] = requested_by
        return self.request("POST", "/runs", json_body=payload)

    def list_runs(self, params: Dict[str, Any]) -> requests.Response:
        return self.request("GET", "/runs", params=params)

    def get_run_detail(self, run_id: str) -> requests.Response:
        return self.request("GET", f"/runs/{run_id}")

    def get_run_logs(self, run_id: str, limit: Optional[int]) -> requests.Response:
        params = {"limit": limit} if limit is not None else {}
        return self.request("GET", f"/runs/{run_id}/logs", params=params)

    def cancel_run(self, run_id: str, reason: Optional[str]) -> requests.Response:
        payload = {"reason": reason} if reason else {}
        return self.request("POST", f"/runs/{run_id}/cancel", json_body=payload)

    # Jobs & feeds
    def list_jobs(self, params: Dict[str, Any]) -> requests.Response:
        return self.request("GET", "/jobs", params=params)

    def get_feed(self, indicator_type: str, pipeline_id: Optional[str]) -> requests.Response:
        if pipeline_id:
            return self.request("GET", f"/pipelines/{pipeline_id}/edl/{indicator_type}")
        return self.request("GET", f"/edl/{indicator_type}")

    # Health
    def health(self) -> requests.Response:
        return self.request("GET", "/health")


def print_response(resp: requests.Response) -> None:
    status = resp.status_code
    print(f"Status: {status}")
    content = resp.text.strip()
    if content:
        try:
            json_payload = resp.json()
            print(json.dumps(json_payload, indent=2, ensure_ascii=False))
        except ValueError:
            print(content)
    if not resp.ok:
        raise SystemExit(1)


def load_file(path: Optional[str]) -> Optional[str]:
    if path is None:
        return None
    file_path = Path(path)
    return file_path.read_text(encoding="utf-8")


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", required=True, help="Base URL for the API (e.g. http://localhost:8100)")
    parser.add_argument("--api-key", help="API key to use (admin or profile key, depending on the endpoint)")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds (default: 30)")


def main() -> None:
    parser = argparse.ArgumentParser(description="EDL API helper CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    # Admin commands
    admin_create = sub.add_parser("create-admin", help="Create a managed admin account")
    add_common_arguments(admin_create)
    admin_create.add_argument("--name", required=True)
    admin_create.add_argument("--role", choices=["operator", "reader"], default="operator")
    admin_create.add_argument("--super-admin", action="store_true")

    admin_list = sub.add_parser("list-admins", help="List admin accounts")
    add_common_arguments(admin_list)

    admin_link = sub.add_parser("link-admin-profile", help="Link admin to profile")
    add_common_arguments(admin_link)
    admin_link.add_argument("--admin-id", required=True)
    admin_link.add_argument("--profile-id", required=True)

    admin_unlink = sub.add_parser("unlink-admin-profile", help="Unlink admin from profile")
    add_common_arguments(admin_unlink)
    admin_unlink.add_argument("--admin-id", required=True)
    admin_unlink.add_argument("--profile-id", required=True)

    # Profile commands
    profile_create = sub.add_parser("create-profile", help="Create a profile")
    add_common_arguments(profile_create)
    profile_create.add_argument("--name", required=True)
    profile_create.add_argument("--description")
    profile_create.add_argument("--owner-admin-id")

    profile_list = sub.add_parser("list-profiles", help="List profiles visible to the caller")
    add_common_arguments(profile_list)

    # Config commands
    config_create = sub.add_parser("create-config", help="Create a profile config")
    add_common_arguments(config_create)
    config_create.add_argument("--profile-id", required=True)
    config_create.add_argument("--sources", required=True)
    config_create.add_argument("--augment")
    config_create.add_argument("--rules", required=True)
    config_create.add_argument("--cfg-mode", default="augment")
    config_create.add_argument("--cfg-output", required=True, help="Pipeline output path")
    config_create.add_argument("--cfg-persist", choices=["true", "false"], default="true", help="Persist to DB (true/false)")
    config_create.add_argument("--cfg-timeout", type=int, default=15, help="Pipeline timeout seconds")
    config_create.add_argument("--cfg-log-level", default="INFO")
    config_create.add_argument("--refresh", type=int, default=None)
    config_create.add_argument("--created-by", default=None)

    config_update = sub.add_parser("update-config", help="Update an existing profile config")
    add_common_arguments(config_update)
    config_update.add_argument("--profile-id", required=True)
    config_update.add_argument("--config-id", required=True)
    config_update.add_argument("--sources")
    config_update.add_argument("--augment")
    config_update.add_argument("--rules")
    config_update.add_argument("--cfg-mode")
    config_update.add_argument("--cfg-output")
    config_update.add_argument("--cfg-persist", choices=["true", "false"])
    config_update.add_argument("--cfg-timeout", type=int)
    config_update.add_argument("--cfg-log-level")
    config_update.add_argument("--refresh", type=int)

    config_list = sub.add_parser("list-configs", help="List configs for a profile")
    add_common_arguments(config_list)
    config_list.add_argument("--profile-id", required=True)

    # Pipeline commands
    pipeline_create = sub.add_parser("create-pipeline", help="Create a pipeline")
    add_common_arguments(pipeline_create)
    pipeline_create.add_argument("--profile-id", required=True)
    pipeline_create.add_argument("--config-id", required=True)
    pipeline_create.add_argument("--name", required=True)
    pipeline_create.add_argument("--description")
    pipeline_create.add_argument("--concurrency", type=int)
    pipeline_create.add_argument("--created-by")

    pipeline_point = sub.add_parser("point-pipeline", help="Point a pipeline to a different config")
    add_common_arguments(pipeline_point)
    pipeline_point.add_argument("--pipeline-id", required=True)
    pipeline_point.add_argument("--config-id", required=True)

    pipeline_delete = sub.add_parser("delete-pipeline", help="Soft-delete a pipeline")
    add_common_arguments(pipeline_delete)
    pipeline_delete.add_argument("--pipeline-id", required=True)

    pipeline_list = sub.add_parser("list-pipelines", help="List pipelines")
    add_common_arguments(pipeline_list)
    pipeline_list.add_argument("--profile-id")
    pipeline_list.add_argument("--config-id")
    pipeline_list.add_argument("--active-only", action="store_true")
    pipeline_list.add_argument("--limit", type=int)
    pipeline_list.add_argument("--offset", type=int)

    pipeline_list_profile = sub.add_parser("list-profile-pipelines", help="List pipelines for a profile")
    add_common_arguments(pipeline_list_profile)
    pipeline_list_profile.add_argument("--profile-id", required=True)
    pipeline_list_profile.add_argument("--active-only", action="store_true")
    pipeline_list_profile.add_argument("--limit", type=int)
    pipeline_list_profile.add_argument("--offset", type=int)

    pipeline_active = sub.add_parser("list-active-pipelines", help="List active pipelines for a profile")
    add_common_arguments(pipeline_active)
    pipeline_active.add_argument("--profile-id", required=True)

    pipeline_search = sub.add_parser("search-pipelines", help="Search pipelines by indicator")
    add_common_arguments(pipeline_search)
    pipeline_search.add_argument("--profile-id", required=True)
    pipeline_search.add_argument("--indicator", required=True)

    # Run commands
    run_submit = sub.add_parser("submit-run", help="Submit a pipeline run")
    add_common_arguments(run_submit)
    run_submit.add_argument("--pipeline-id", required=True)
    run_submit.add_argument("--run-mode")
    run_submit.add_argument("--run-timeout", type=int)
    run_submit.add_argument("--run-log-level")
    run_submit.add_argument("--run-persist", choices=["true", "false"])
    run_submit.add_argument("--requested-by")

    run_list = sub.add_parser("list-runs", help="List runs")
    add_common_arguments(run_list)
    run_list.add_argument("--profile-id")
    run_list.add_argument("--pipeline-id")
    run_list.add_argument("--state")
    run_list.add_argument("--limit", type=int)
    run_list.add_argument("--offset", type=int)

    run_detail = sub.add_parser("run-detail", help="Get run detail")
    add_common_arguments(run_detail)
    run_detail.add_argument("--run-id", required=True)

    run_logs = sub.add_parser("run-logs", help="Get run logs")
    add_common_arguments(run_logs)
    run_logs.add_argument("--run-id", required=True)
    run_logs.add_argument("--limit", type=int)

    run_cancel = sub.add_parser("cancel-run", help="Cancel a run")
    add_common_arguments(run_cancel)
    run_cancel.add_argument("--run-id", required=True)
    run_cancel.add_argument("--reason")

    # Jobs
    jobs_list = sub.add_parser("list-jobs", help="List background jobs")
    add_common_arguments(jobs_list)
    jobs_list.add_argument("--profile-id")
    jobs_list.add_argument("--pipeline-id")
    jobs_list.add_argument("--limit", type=int)
    jobs_list.add_argument("--offset", type=int)

    # Feeds
    feed_get = sub.add_parser("get-feed", help="Fetch hosted feed data")
    add_common_arguments(feed_get)
    feed_get.add_argument("--indicator-type", required=True, choices=["url", "fqdn", "ipv4", "ipv6", "cidr"])
    feed_get.add_argument("--pipeline-id")

    # Health
    health = sub.add_parser("health", help="Health check")
    add_common_arguments(health)

    args = parser.parse_args()
    client = ApiClient(args.base_url, args.api_key, timeout=args.timeout)

    try:
        if args.command == "create-admin":
            resp = client.create_admin(args.name, args.role, args.super_admin)
        elif args.command == "list-admins":
            resp = client.list_admins()
        elif args.command == "link-admin-profile":
            resp = client.link_admin_profile(args.admin_id, args.profile_id)
        elif args.command == "unlink-admin-profile":
            resp = client.unlink_admin_profile(args.admin_id, args.profile_id)
        elif args.command == "create-profile":
            resp = client.create_profile(args.name, args.description, args.owner_admin_id)
        elif args.command == "list-profiles":
            resp = client.list_profiles()
        elif args.command == "create-config":
            settings = {
                "mode": args.cfg_mode,
                "output_path": args.cfg_output,
                "persist_to_db": args.cfg_persist.lower() == "true",
                "timeout": args.cfg_timeout,
                "log_level": args.cfg_log_level,
            }
            resp = client.create_profile_config(
                profile_id=args.profile_id,
                sources=load_file(args.sources),
                augment=load_file(args.augment),
                rules=load_file(args.rules),
                settings=settings,
                refresh_minutes=args.refresh,
                created_by=args.created_by,
            )
        elif args.command == "update-config":
            settings: Dict[str, Any] = {}
            if args.cfg_mode:
                settings["mode"] = args.cfg_mode
            if args.cfg_output:
                settings["output_path"] = args.cfg_output
            if args.cfg_persist is not None:
                settings["persist_to_db"] = args.cfg_persist.lower() == "true"
            if args.cfg_timeout is not None:
                settings["timeout"] = args.cfg_timeout
            if args.cfg_log_level:
                settings["log_level"] = args.cfg_log_level
            resp = client.update_profile_config(
                profile_id=args.profile_id,
                config_id=args.config_id,
                sources=load_file(args.sources),
                augment=load_file(args.augment),
                rules=load_file(args.rules),
                settings=settings if settings else None,
                refresh_minutes=args.refresh,
            )
        elif args.command == "list-configs":
            resp = client.list_profile_configs(args.profile_id)
        elif args.command == "create-pipeline":
            resp = client.create_pipeline(
                profile_id=args.profile_id,
                profile_config_id=args.config_id,
                name=args.name,
                description=args.description,
                concurrency_limit=args.concurrency,
                created_by=args.created_by,
            )
        elif args.command == "point-pipeline":
            resp = client.point_pipeline_to_config(args.pipeline_id, args.config_id)
        elif args.command == "delete-pipeline":
            resp = client.delete_pipeline(args.pipeline_id)
        elif args.command == "list-pipelines":
            params = {
                "profile_id": args.profile_id,
                "profile_config_id": args.config_id,
                "active_only": "true" if args.active_only else None,
                "limit": args.limit,
                "offset": args.offset,
            }
            params = {k: v for k, v in params.items() if v is not None}
            resp = client.list_pipelines(params)
        elif args.command == "list-profile-pipelines":
            params = {
                "active_only": "true" if args.active_only else None,
                "limit": args.limit,
                "offset": args.offset,
            }
            params = {k: v for k, v in params.items() if v is not None}
            resp = client.list_profile_pipelines(args.profile_id, params)
        elif args.command == "list-active-pipelines":
            resp = client.list_active_pipelines(args.profile_id)
        elif args.command == "search-pipelines":
            resp = client.search_pipelines(args.profile_id, args.indicator)
        elif args.command == "submit-run":
            overrides: Dict[str, Any] = {}
            if args.run_mode:
                overrides["mode"] = args.run_mode
            if args.run_timeout is not None:
                overrides["timeout"] = args.run_timeout
            if args.run_log_level:
                overrides["log_level"] = args.run_log_level
            if args.run_persist is not None:
                overrides["persist_to_db"] = args.run_persist.lower() == "true"
            resp = client.submit_run(args.pipeline_id, overrides if overrides else None, args.requested_by)
        elif args.command == "list-runs":
            params = {
                "profile_id": args.profile_id,
                "pipeline_id": args.pipeline_id,
                "state": args.state,
                "limit": args.limit,
                "offset": args.offset,
            }
            params = {k: v for k, v in params.items() if v is not None}
            resp = client.list_runs(params)
        elif args.command == "run-detail":
            resp = client.get_run_detail(args.run_id)
        elif args.command == "run-logs":
            resp = client.get_run_logs(args.run_id, args.limit)
        elif args.command == "cancel-run":
            resp = client.cancel_run(args.run_id, args.reason)
        elif args.command == "list-jobs":
            params = {
                "profile_id": args.profile_id,
                "pipeline_id": args.pipeline_id,
                "limit": args.limit,
                "offset": args.offset,
            }
            params = {k: v for k, v in params.items() if v is not None}
            resp = client.list_jobs(params)
        elif args.command == "get-feed":
            resp = client.get_feed(args.indicator_type, args.pipeline_id)
        elif args.command == "health":
            resp = client.health()
       	else:
            parser.error(f"Unknown command {args.command}")
            return
    except requests.RequestException as exc:
        print(f"HTTP request failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print_response(resp)


if __name__ == "__main__":
    main()
