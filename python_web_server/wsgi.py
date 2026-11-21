"""WSGI entry point.

Run from the repository root, since main.py imports the package by name:

    gunicorn --workers 4 --bind 127.0.0.1:8000 python_web_server.wsgi:app
"""

from python_web_server.main import app

if __name__ == "__main__":
    app.run()
