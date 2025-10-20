"""
FastAPI application exposing the EDL pipeline orchestration layer.

Modules in this package provide request/response schemas, an in-memory job
registry, and the ASGI app itself (`api.main:app`).
"""

__all__ = ["main", "schemas", "jobs"]
