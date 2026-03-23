#!/usr/bin/env python3
"""
Theseus MCP Server — exposes Hack Club's mail system (Theseus) as MCP tools.

Provides tools for managing warehouse orders, letters, letter queues, tags,
and user info via the Theseus API at https://mail.hackclub.com.

Usage:
    THESEUS_API_KEY=th_api_live_... uv run server.py
"""

import json
import os
import urllib.error
import urllib.request
from typing import Any

from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

THESEUS_BASE_URL = os.environ.get("THESEUS_BASE_URL", "https://mail.hackclub.com")
THESEUS_API_KEY = os.environ.get("THESEUS_API_KEY", "")

mcp = FastMCP(
    "theseus",
    instructions=(
        "Theseus MCP Server — Hack Club's mail system. "
        "Create and manage warehouse orders (packages), letters, letter queues, and tags. "
        "Use the THESEUS_API_KEY environment variable for authentication."
    ),
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {THESEUS_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "theseus-mcp/1.0",
    }


def _request(method: str, path: str, body: dict | None = None, params: dict | None = None) -> dict:
    """Make an HTTP request to the Theseus API and return the JSON response."""
    url = f"{THESEUS_BASE_URL.rstrip('/')}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        if qs:
            url = f"{url}?{qs}"

    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method, headers=_headers())

    try:
        resp = urllib.request.urlopen(req, timeout=120)
        raw = resp.read()
        return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        try:
            error_body = json.loads(exc.read().decode())
        except Exception:
            error_body = {"raw": str(exc)}
        return {"error": True, "http_status": exc.code, "detail": error_body}
    except urllib.error.URLError as exc:
        return {"error": True, "detail": str(exc.reason)}


def _get(path: str, params: dict | None = None) -> dict:
    return _request("GET", path, params=params)


def _post(path: str, body: dict | None = None) -> dict:
    return _request("POST", path, body=body)


def _patch(path: str, body: dict | None = None) -> dict:
    return _request("PATCH", path, body=body)


def _delete(path: str) -> dict:
    return _request("DELETE", path)


# ---------------------------------------------------------------------------
# User tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_current_user() -> dict:
    """Get the currently authenticated Theseus user."""
    return _get("/api/v1/user")


# ---------------------------------------------------------------------------
# Warehouse Order tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_warehouse_orders() -> dict:
    """List all warehouse orders visible to the current API key."""
    return _get("/api/v1/warehouse_orders")


@mcp.tool()
def get_warehouse_order(order_id: str) -> dict:
    """Get a specific warehouse order by its ID (e.g. 'pkg!abc123').

    Args:
        order_id: The warehouse order ID, like 'pkg!abc123'.
    """
    return _get(f"/api/v1/warehouse_orders/{order_id}")


@mcp.tool()
def create_warehouse_order(
    recipient_email: str,
    tags: list[str],
    first_name: str,
    line_1: str,
    city: str,
    state: str,
    postal_code: str,
    country: str,
    contents: list[dict[str, Any]],
    last_name: str = "",
    line_2: str = "",
    user_facing_title: str = "",
    idempotency_key: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Create a new warehouse order and dispatch it to Zenventory for fulfillment.

    This creates a physical package to be shipped. The order is immediately dispatched.
    Blocked countries: IR, PS, CU, KP, RU.

    Args:
        recipient_email: Email address of the recipient (required).
        tags: List of tags for this order (required, must be non-empty). E.g. ["mail-cannon"].
        first_name: Recipient's first name.
        line_1: Street address line 1.
        city: City name.
        state: State/province (auto-normalized for US addresses).
        postal_code: ZIP/postal code.
        country: Country name or ISO alpha-2 code (auto-normalized).
        contents: List of items, each with 'sku' (string) and 'quantity' (int).
                  Example: [{"sku": "HC-STICKER-V2", "quantity": 3}]
        last_name: Recipient's last name (optional).
        line_2: Street address line 2 (optional).
        user_facing_title: Title shown to recipient (optional).
        idempotency_key: Unique key to prevent duplicate orders (optional).
        metadata: Arbitrary key-value metadata (optional).
    """
    payload: dict[str, Any] = {
        "warehouse_order": {
            "recipient_email": recipient_email,
            "tags": tags,
        },
        "address": {
            "first_name": first_name,
            "line_1": line_1,
            "city": city,
            "state": state,
            "postal_code": postal_code,
            "country": country,
        },
        "contents": contents,
    }

    if last_name:
        payload["address"]["last_name"] = last_name
    if line_2:
        payload["address"]["line_2"] = line_2
    if user_facing_title:
        payload["warehouse_order"]["user_facing_title"] = user_facing_title
    if idempotency_key:
        payload["warehouse_order"]["idempotency_key"] = idempotency_key
    if metadata:
        payload["warehouse_order"]["metadata"] = metadata

    return _post("/api/v1/warehouse_orders", payload)


@mcp.tool()
def create_warehouse_order_from_template(
    template_id: str,
    recipient_email: str,
    tags: list[str],
    first_name: str,
    line_1: str,
    city: str,
    state: str,
    postal_code: str,
    country: str,
    last_name: str = "",
    line_2: str = "",
    additional_contents: list[dict[str, Any]] | None = None,
    idempotency_key: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Create a warehouse order from a pre-defined template.

    The template pre-populates line items. You can optionally add more items.

    Args:
        template_id: Template public ID (e.g. 'wot!abc').
        recipient_email: Email address of the recipient.
        tags: List of tags (required, non-empty).
        first_name: Recipient's first name.
        line_1: Street address line 1.
        city: City name.
        state: State/province.
        postal_code: ZIP/postal code.
        country: Country name or alpha-2 code.
        last_name: Recipient's last name (optional).
        line_2: Street address line 2 (optional).
        additional_contents: Extra items on top of template defaults (optional).
        idempotency_key: Unique key to prevent duplicates (optional).
        metadata: Arbitrary key-value metadata (optional).
    """
    payload: dict[str, Any] = {
        "warehouse_order": {
            "recipient_email": recipient_email,
            "tags": tags,
        },
        "address": {
            "first_name": first_name,
            "line_1": line_1,
            "city": city,
            "state": state,
            "postal_code": postal_code,
            "country": country,
        },
    }

    if last_name:
        payload["address"]["last_name"] = last_name
    if line_2:
        payload["address"]["line_2"] = line_2
    if additional_contents:
        payload["contents"] = additional_contents
    if idempotency_key:
        payload["warehouse_order"]["idempotency_key"] = idempotency_key
    if metadata:
        payload["warehouse_order"]["metadata"] = metadata

    return _post(f"/api/v1/warehouse_orders/from_template/{template_id}", payload)


# ---------------------------------------------------------------------------
# Letter tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_letters() -> dict:
    """List all letters visible to the current API key."""
    return _get("/api/v1/letters")


@mcp.tool()
def get_letter(letter_id: str, expand_label: bool = False) -> dict:
    """Get a specific letter by ID.

    Args:
        letter_id: The letter ID (e.g. 'ltr!abc123' or 'hackapost!xyz').
        expand_label: If true, include the label_url (requires PII key).
    """
    params = {"expand": "label"} if expand_label else None
    return _get(f"/api/v1/letters/{letter_id}", params=params)


@mcp.tool()
def create_letter(
    recipient_email: str,
    first_name: str,
    line_1: str,
    city: str,
    state: str,
    postal_code: str,
    country: str,
    last_name: str = "",
    line_2: str = "",
    rubber_stamps: str = "",
    idempotency_key: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Create a new letter.

    Args:
        recipient_email: Recipient's email address.
        first_name: Recipient's first name.
        line_1: Street address line 1.
        city: City.
        state: State/province.
        postal_code: ZIP/postal code.
        country: Country name or alpha-2 code.
        last_name: Recipient's last name (optional).
        line_2: Street address line 2 (optional).
        rubber_stamps: Text to stamp on the letter (optional).
        idempotency_key: Unique key to prevent duplicates (optional).
        metadata: Arbitrary metadata (optional).
    """
    payload: dict[str, Any] = {
        "recipient_email": recipient_email,
        "address": {
            "first_name": first_name,
            "line_1": line_1,
            "city": city,
            "state": state,
            "postal_code": postal_code,
            "country": country,
        },
    }
    if last_name:
        payload["address"]["last_name"] = last_name
    if line_2:
        payload["address"]["line_2"] = line_2
    if rubber_stamps:
        payload["rubber_stamps"] = rubber_stamps
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key
    if metadata:
        payload["metadata"] = metadata

    return _post("/api/v1/letters", payload)


@mcp.tool()
def update_letter(letter_id: str, updates: dict[str, Any]) -> dict:
    """Update a letter's fields.

    Args:
        letter_id: The letter ID (e.g. 'ltr!abc123').
        updates: Dictionary of fields to update.
    """
    return _patch(f"/api/v1/letters/{letter_id}", updates)


@mcp.tool()
def delete_letter(letter_id: str) -> dict:
    """Delete a letter.

    Args:
        letter_id: The letter ID (e.g. 'ltr!abc123').
    """
    return _delete(f"/api/v1/letters/{letter_id}")


@mcp.tool()
def mark_letter_printed(letter_id: str) -> dict:
    """Mark a letter as printed.

    Args:
        letter_id: The letter ID (e.g. 'ltr!abc123').
    """
    return _post(f"/api/v1/letters/{letter_id}/mark_printed")


@mcp.tool()
def mark_letter_mailed(letter_id: str) -> dict:
    """Mark a letter as mailed.

    Args:
        letter_id: The letter ID (e.g. 'ltr!abc123').
    """
    return _post(f"/api/v1/letters/{letter_id}/mark_mailed")


# ---------------------------------------------------------------------------
# Letter Queue tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_letter_queues() -> dict:
    """List all letter queues."""
    return _get("/api/v1/letter_queues")


@mcp.tool()
def get_letter_queue(queue_slug: str) -> dict:
    """Get a specific letter queue by its slug.

    Args:
        queue_slug: The queue's URL slug identifier.
    """
    return _get(f"/api/v1/letter_queues/{queue_slug}")


@mcp.tool()
def create_letter_queue(name: str) -> dict:
    """Create a new letter queue.

    Args:
        name: The name for the new queue.
    """
    return _post("/api/v1/letter_queues", {"name": name})


@mcp.tool()
def update_letter_queue(queue_slug: str, updates: dict[str, Any]) -> dict:
    """Update a letter queue.

    Args:
        queue_slug: The queue's URL slug.
        updates: Fields to update.
    """
    return _patch(f"/api/v1/letter_queues/{queue_slug}", updates)


@mcp.tool()
def delete_letter_queue(queue_slug: str) -> dict:
    """Delete a letter queue.

    Args:
        queue_slug: The queue's URL slug.
    """
    return _delete(f"/api/v1/letter_queues/{queue_slug}")


@mcp.tool()
def add_letter_to_queue(
    queue_slug: str,
    recipient_email: str,
    first_name: str,
    line_1: str,
    city: str,
    state: str,
    postal_code: str,
    country: str,
    last_name: str = "",
    line_2: str = "",
    rubber_stamps: str = "",
    return_address_name: str = "",
    idempotency_key: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Add a letter to a letter queue.

    Args:
        queue_slug: The queue's URL slug.
        recipient_email: Recipient's email.
        first_name: Recipient's first name.
        line_1: Street address line 1.
        city: City.
        state: State/province.
        postal_code: ZIP/postal code.
        country: Country name or alpha-2 code.
        last_name: Last name (optional).
        line_2: Address line 2 (optional).
        rubber_stamps: Stamp text (optional).
        return_address_name: Override the return address name (optional).
        idempotency_key: Unique key to prevent duplicates (optional).
        metadata: Arbitrary metadata (optional).
    """
    payload: dict[str, Any] = {
        "recipient_email": recipient_email,
        "address": {
            "first_name": first_name,
            "line_1": line_1,
            "city": city,
            "state": state,
            "postal_code": postal_code,
            "country": country,
        },
    }
    if last_name:
        payload["address"]["last_name"] = last_name
    if line_2:
        payload["address"]["line_2"] = line_2
    if rubber_stamps:
        payload["rubber_stamps"] = rubber_stamps
    if return_address_name:
        payload["return_address_name"] = return_address_name
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key
    if metadata:
        payload["metadata"] = metadata

    return _post(f"/api/v1/letter_queues/{queue_slug}", payload)


@mcp.tool()
def create_instant_letter(
    queue_slug: str,
    recipient_email: str,
    first_name: str,
    line_1: str,
    city: str,
    state: str,
    postal_code: str,
    country: str,
    last_name: str = "",
    line_2: str = "",
    rubber_stamps: str = "",
    return_address_name: str = "",
    idempotency_key: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Create an instant letter via an instant queue (processed immediately, includes label).

    Args:
        queue_slug: The instant queue's URL slug.
        recipient_email: Recipient's email.
        first_name: Recipient's first name.
        line_1: Street address line 1.
        city: City.
        state: State/province.
        postal_code: ZIP/postal code.
        country: Country name or alpha-2 code.
        last_name: Last name (optional).
        line_2: Address line 2 (optional).
        rubber_stamps: Stamp text (optional).
        return_address_name: Override return address name (optional).
        idempotency_key: Prevent duplicates (optional).
        metadata: Arbitrary metadata (optional).
    """
    payload: dict[str, Any] = {
        "recipient_email": recipient_email,
        "address": {
            "first_name": first_name,
            "line_1": line_1,
            "city": city,
            "state": state,
            "postal_code": postal_code,
            "country": country,
        },
    }
    if last_name:
        payload["address"]["last_name"] = last_name
    if line_2:
        payload["address"]["line_2"] = line_2
    if rubber_stamps:
        payload["rubber_stamps"] = rubber_stamps
    if return_address_name:
        payload["return_address_name"] = return_address_name
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key
    if metadata:
        payload["metadata"] = metadata

    return _post(f"/api/v1/letter_queues/instant/{queue_slug}", payload)


@mcp.tool()
def get_instant_queue_pending(queue_slug: str) -> dict:
    """Get pending/queued letters in an instant queue (requires PII key).

    Args:
        queue_slug: The instant queue's URL slug.
    """
    return _get(f"/api/v1/letter_queues/instant/{queue_slug}/queued")


# ---------------------------------------------------------------------------
# Tag tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_tags() -> dict:
    """List all available tags in Theseus."""
    return _get("/api/v1/tags")


@mcp.tool()
def get_tag_stats(tag_name: str, no_cache: bool = False) -> dict:
    """Get statistics for a specific tag (letter count, costs, warehouse order stats).

    Args:
        tag_name: The tag name to look up.
        no_cache: If true, bypass the 5-minute cache.
    """
    params = {"no_cache": "1"} if no_cache else None
    return _get(f"/api/v1/tags/{tag_name}", params=params)


@mcp.tool()
def get_tag_letters(tag_name: str) -> dict:
    """Get all letters associated with a specific tag.

    Args:
        tag_name: The tag name.
    """
    return _get(f"/api/v1/tags/{tag_name}/letters")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not THESEUS_API_KEY:
        import sys
        print("Set THESEUS_API_KEY environment variable before running.", file=sys.stderr)
        sys.exit(1)
    mcp.run()
