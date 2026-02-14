from __future__ import annotations

import json
import logging
import socket
from typing import Any, Optional
from urllib.parse import urlparse

import dns.resolver
import requests
from sqlalchemy import select

from ..config import load_config
from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Domain, WhoisCheck

logger = logging.getLogger(__name__)

PARKED_KEYWORDS = [
    "domain for sale",
    "buy this domain",
    "this domain is for sale",
    "domain parked",
    "parkingcrew",
    "sedo",
    "afternic",
    "bodis",
    "namecheap",
    "dan.com",
    "cashparking",
    "click here to inquire",
]

PARKED_HOST_HINTS = [
    "parkingcrew",
    "sedoparking",
    "bodis",
    "afternic",
    "dan.com",
    "namecheap",
    "hugedomains",
]


class RdapClient:
    def __init__(self) -> None:
        self.config = load_config()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.config.http_user_agent})

    def fetch(self, domain: str) -> tuple[Optional[dict[str, Any]], Optional[int]]:
        url = f"{self.config.rdap_base_url.rstrip('/')}/{domain}"
        try:
            resp = self.session.get(url, timeout=self.config.http_timeout)
        except requests.RequestException:
            return None, None

        if resp.status_code == 404:
            return None, 404
        if resp.status_code >= 400:
            return None, resp.status_code

        try:
            return resp.json(), resp.status_code
        except json.JSONDecodeError:
            return None, resp.status_code


def _query_records(domain: str, record_type: str, timeout: int) -> tuple[bool, list[str], Optional[str]]:
    resolver = dns.resolver.Resolver()
    resolver.timeout = timeout
    resolver.lifetime = timeout
    try:
        answer = resolver.resolve(domain, record_type, lifetime=timeout)
        values = [entry.to_text().strip().lower().rstrip(".") for entry in answer]
        return answer.rrset is not None, values, None
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
        # These are expected for domains without specific record types
        return False, [], None
    except (dns.exception.Timeout, dns.resolver.NoNameservers, dns.exception.DNSException) as exc:
        # Catch specific DNS errors before the general DNSException base class
        # Note: Order matters - specific exceptions before base class
        return False, [], exc.__class__.__name__


def dns_check(domain: str, timeout: int, check_www: bool) -> dict[str, Any]:
    apex_checks = {}
    www_checks = {}
    errors: list[str] = []

    for record_type in ("A", "AAAA", "CNAME", "MX", "NS"):
        has_record, values, error_name = _query_records(domain, record_type, timeout=timeout)
        apex_checks[record_type] = {"has_record": has_record, "values": values}
        if error_name:
            errors.append(f"apex:{record_type}:{error_name}")

    if check_www:
        www_domain = f"www.{domain}"
        for record_type in ("A", "AAAA", "CNAME"):
            has_record, values, error_name = _query_records(www_domain, record_type, timeout=timeout)
            www_checks[record_type] = {"has_record": has_record, "values": values}
            if error_name:
                errors.append(f"www:{record_type}:{error_name}")

    has_a = apex_checks["A"]["has_record"] or www_checks.get("A", {}).get("has_record", False)
    has_aaaa = apex_checks["AAAA"]["has_record"] or www_checks.get("AAAA", {}).get("has_record", False)
    has_cname = apex_checks["CNAME"]["has_record"] or www_checks.get("CNAME", {}).get("has_record", False)
    has_mx = apex_checks["MX"]["has_record"]
    has_ns = apex_checks["NS"]["has_record"]

    cname_targets = []
    cname_targets.extend(apex_checks["CNAME"]["values"])
    cname_targets.extend(www_checks.get("CNAME", {}).get("values", []))

    return {
        "has_a": bool(has_a),
        "has_aaaa": bool(has_aaaa),
        "has_cname": bool(has_cname),
        "has_mx": bool(has_mx),
        "has_ns": bool(has_ns),
        "cname_targets": cname_targets,
        "dns_error": bool(errors),
        "dns_errors": errors,
    }


def http_probe(
    domain: str,
    timeout: int,
    user_agent: str,
    check_www: bool,
) -> tuple[bool, Optional[int], Optional[str], Optional[str], Optional[str]]:
    headers = {"User-Agent": user_agent}
    hosts = [domain]
    if check_www:
        hosts.append(f"www.{domain}")

    for host in hosts:
        for scheme in ("https", "http"):
            url = f"{scheme}://{host}"
            try:
                resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
                status = resp.status_code
                if status >= 500:
                    continue
                final_url = resp.url
                content_type = resp.headers.get("Content-Type", "")
                if "text" in content_type or "html" in content_type:
                    body = resp.text[:200_000]
                else:
                    body = None
                return True, status, final_url, body, host
            except requests.RequestException:
                continue
    return False, None, None, None, None


def tcp_probe(
    domain: str,
    ports: tuple[int, ...],
    timeout: int,
    check_www: bool,
) -> tuple[bool, Optional[str], Optional[int]]:
    hosts = [domain]
    if check_www:
        hosts.append(f"www.{domain}")

    for host in hosts:
        for port in ports:
            try:
                with socket.create_connection((host, port), timeout=timeout):
                    return True, host, port
            except OSError:
                continue
    return False, None, None


def detect_parked(body: Optional[str], final_url: Optional[str], cname_targets: list[str]) -> bool:
    if not body and not final_url and not cname_targets:
        return False

    if final_url:
        host = urlparse(final_url).netloc.lower()
        if any(hint in host for hint in PARKED_HOST_HINTS):
            return True

    if cname_targets and any(any(hint in target for hint in PARKED_HOST_HINTS) for target in cname_targets):
        return True

    if body:
        text = body.lower()
        if any(keyword in text for keyword in PARKED_KEYWORDS):
            return True

    return False


def extract_registrar(rdap_data: Optional[dict[str, Any]]) -> Optional[str]:
    if not rdap_data:
        return None

    for entity in rdap_data.get("entities", []):
        roles = entity.get("roles", [])
        if "registrar" in roles:
            vcard = entity.get("vcardArray", [])
            if isinstance(vcard, list) and len(vcard) > 1:
                for item in vcard[1]:
                    # vcard items should have at least 4 elements: [type, params, value_type, value]
                    if isinstance(item, list) and len(item) > 3 and item[0] == "fn":
                        return item[3]
    return None


def process_domain(domain_row: Domain, rdap_client: RdapClient) -> WhoisCheck:
    rdap_data, rdap_status = rdap_client.fetch(domain_row.domain)
    if rdap_status is None:
        is_registered = None
    elif rdap_status == 404:
        is_registered = False
    else:
        is_registered = True

    dns_result = dns_check(
        domain_row.domain,
        timeout=rdap_client.config.dns_timeout,
        check_www=rdap_client.config.dns_check_www,
    )
    # Use .get() for defensive dict access in case dns_result is malformed
    has_a = dns_result.get("has_a", False)
    has_aaaa = dns_result.get("has_aaaa", False)
    has_cname = dns_result.get("has_cname", False)
    has_mx = dns_result.get("has_mx", False)
    has_http, http_status, final_url, body, http_host = http_probe(
        domain_row.domain,
        timeout=rdap_client.config.http_timeout,
        user_agent=rdap_client.config.http_user_agent,
        check_www=rdap_client.config.dns_check_www,
    )
    has_tcp = False
    tcp_host = None
    tcp_port = None
    if rdap_client.config.tcp_probe_enabled and rdap_client.config.tcp_probe_ports:
        has_tcp, tcp_host, tcp_port = tcp_probe(
            domain_row.domain,
            ports=rdap_client.config.tcp_probe_ports,
            timeout=rdap_client.config.tcp_probe_timeout,
            check_www=rdap_client.config.dns_check_www,
        )

    is_parked = False
    cname_targets = dns_result.get("cname_targets", [])
    if has_http or cname_targets:
        is_parked = detect_parked(body, final_url, cname_targets)

    registrar = extract_registrar(rdap_data)

    is_hosted = has_a or has_aaaa or has_cname or has_http or has_tcp
    diagnostics = {
        "rdap_status_code": rdap_status,
        "http_final_url": final_url,
        "http_host_checked": http_host,
        "dns": dns_result,
        "tcp_probe": {
            "enabled": rdap_client.config.tcp_probe_enabled,
            "open": has_tcp,
            "host": tcp_host,
            "port": tcp_port,
            "ports_checked": list(rdap_client.config.tcp_probe_ports),
        },
    }

    check = WhoisCheck(
        domain_id=domain_row.id,
        is_registered=is_registered,
        is_parked=is_parked,
        has_a=has_a,
        has_aaaa=has_aaaa,
        has_cname=has_cname,
        has_mx=has_mx,
        has_http=bool(has_http),
        http_status=http_status,
        registrar=registrar,
        raw={
            "rdap": rdap_data,
            "diagnostics": diagnostics,
        },
    )

    if is_parked:
        domain_row.status = "parked"
    elif is_hosted:
        domain_row.status = "hosted"
    elif rdap_status == 404:
        domain_row.status = "unregistered_candidate"
    elif rdap_status is None:
        domain_row.status = "rdap_error"
    elif dns_result["dns_error"]:
        domain_row.status = "dns_error"
    elif not has_mx:
        domain_row.status = "mx_missing"
    else:
        domain_row.status = "verified_unhosted"

    return check


def run_batch(
    limit: Optional[int] = None,
    scope: Optional[str] = None,
    statuses: Optional[list[str]] = None,
) -> int:
    config = load_config()
    rdap_client = RdapClient()
    processed = 0

    with session_scope() as session:
        run = start_job(session, "rdap_check_domains", scope=scope)
        # When limit is None, use config.batch_size. When limit <= 0, process all (no limit)
        if limit is None:
            batch_size = config.batch_size
        elif limit <= 0:
            batch_size = None  # No limit
        else:
            batch_size = limit

        try:
            target_statuses = statuses or ["new"]
            stmt = (
                select(Domain)
                .where(Domain.status.in_(target_statuses))
                .order_by(Domain.created_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
            domains = session.execute(stmt).scalars().all()

            for domain_row in domains:
                check = process_domain(domain_row, rdap_client)
                session.add(check)
                processed += 1

            complete_job(
                session,
                run,
                processed_count=processed,
                details={"statuses": target_statuses},
            )
        except Exception as exc:
            fail_job(session, run, error=str(exc), details={"statuses": statuses or ["new"]})
            raise

    return processed


if __name__ == "__main__":
    count = run_batch()
    logger.info("Processed %d domains", count)
