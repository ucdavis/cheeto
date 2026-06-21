# Celery-based persistent service (`cheeto daemon worker|beat`) and the
# FastAPI REST API (`cheeto daemon api`).
#
# Import note: `from .app import app` triggers task registration; the api
# module is intentionally importable without celery configuration.
