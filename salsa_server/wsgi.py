"""WSGI entry point.

Run from inside salsa_server/:

    gunicorn --workers 1 --threads 4 --bind 127.0.0.1:5000 wsgi:app

Each worker loads its own copy of the model, so prefer threads over workers.
"""

from main import app

if __name__ == "__main__":
    app.run()
