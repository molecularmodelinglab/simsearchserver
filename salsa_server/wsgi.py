"""WSGI entry point.

Run from the repository root:

    gunicorn --workers 1 --threads 4 --bind 127.0.0.1:5000 salsa_server.wsgi:app

Each worker loads its own copy of the model, so prefer threads over workers.
"""

from salsa_server.main import app

if __name__ == "__main__":
    app.run()
