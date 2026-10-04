"""Browser front end for the DAG generator.

`server.py` exposes the same `generate_dag` the CLI calls over HTTP; `static/`
holds the page, which is plain HTML, CSS and JavaScript with no build step and
no third-party runtime.

Started with `make web`, or:

    python -m dag_generator.cli --web
"""
