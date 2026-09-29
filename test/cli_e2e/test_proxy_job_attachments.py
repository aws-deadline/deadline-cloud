# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
End-to-end test that the job_attachments S3 traffic honors ``settings.https_proxy``
/ ``settings.ca_bundle`` (feature request aws-deadline/deadline-cloud#1046).

The companion ``test_proxy_config.py`` proves the *Deadline control-plane* client
routes through the proxy. This test proves the same for the **job_attachments**
library, whose S3 clients are built internally (``get_s3_client``) off the boto3
session the ``deadline`` CLI hands it -- so they must inherit the proxy / CA bundle
from that session.

It drives the real ``deadline attachment upload``/``download`` CLI as a subprocess
against a *realistic* ``https://s3.<region>.amazonaws.com`` endpoint:

    deadline attachment upload --s3-root-uri s3://<bucket>/<prefix> --profile default
      -> config: settings.https_proxy = http://127.0.0.1:<proxy>
                 settings.ca_bundle  = <throwaway CA>
      -> the job_attachments S3 client routes the call to the realistic
         s3.us-west-2.amazonaws.com endpoint THROUGH the proxy
      -> the TLS-intercepting proxy terminates TLS (serving a cert the CLI trusts
         only via settings.ca_bundle) and forwards plaintext to a moto S3 server
      -> the object lands in moto; download reads it back

Using ``--s3-root-uri`` + ``--profile`` keeps the command S3-only (no Deadline /
STS calls), so the single S3 endpoint is the only thing on the wire and the proxy
CONNECT log is unambiguous proof the job_attachments S3 client used the proxy. The
TLS-intercept (rather than a blind tunnel) is the realistic corporate-proxy case
that motivates ``ca_bundle`` -- moto speaks plain HTTP, and the proxy is what
presents the trusted ``amazonaws.com`` cert.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Iterator
from urllib.parse import urlparse

import boto3
import pytest

from _constants import ACCESS_KEY, BUCKET, REGION, ROOT_PREFIX, SECRET_KEY
from _proxy_helpers import (
    TLSInterceptConnectProxy,
    generate_ca_and_server_cert,
    server_ssl_context,
)

# Realistic S3 endpoint the CLI is pointed at (NOT localhost). botocore bypasses
# proxies for localhost/127.0.0.1 *targets*, so the endpoint must be a non-local
# host for the proxy to be exercised at all. With an explicit endpoint_url botocore
# uses path-style addressing, so the request host stays equal to this name (no
# bucket-name virtual-host prefix) and the served cert (issued for it) validates.
_S3_HOST = f"s3.{REGION}.amazonaws.com"
_S3_ENDPOINT = f"https://{_S3_HOST}"

skip_on_windows = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Threaded TLS-mock + CONNECT-proxy subprocess harness hangs on Windows CI; "
    "proxy/ca_bundle wiring is covered cross-platform by the unit tests.",
)


@pytest.fixture
def s3_proxy_setup(tmp_path: Path, moto_server: str) -> Iterator[tuple]:
    """
    Stand up a TLS-intercepting CONNECT proxy in front of a moto S3 server and an
    isolated CLI env that targets a realistic s3.<region>.amazonaws.com endpoint.

    A single throwaway CA signs the cert the proxy serves for the intercept; that
    same CA is handed to the CLI via ``settings.ca_bundle`` so the two stay in sync.

    Yields ``(env, proxy, ca_pem, s3_client)``.
    """
    ca_pem, server_cert, server_key = generate_ca_and_server_cert(tmp_path, [_S3_HOST])

    moto = urlparse(moto_server)
    ctx = server_ssl_context(server_cert, server_key)
    proxy = TLSInterceptConnectProxy(moto.hostname, moto.port, ctx).start()

    # Real boto3 S3 client (direct to moto) used to seed the bucket and assert state.
    s3_client = boto3.client(
        "s3",
        endpoint_url=moto_server,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )
    try:
        s3_client.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass
    # The moto server + bucket are session-scoped and shared across cli_e2e tests,
    # so wipe any objects left by a prior test on this xdist worker. Otherwise the
    # exact-count assertions below (e.g. 2 objects under Data/) can observe another
    # test's leftovers. (Mirrors the wipe in conftest's seeded_farm_queue fixture.)
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        if page.get("Contents"):
            s3_client.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in page["Contents"]]},
            )

    config_file = tmp_path / "deadline.config"
    config_file.write_text("")
    fake_home = tmp_path / "home"
    fake_home.mkdir()

    env = {
        **os.environ,
        "HOME": str(fake_home),
        "USERPROFILE": str(fake_home),
        # Point S3 at the realistic host so the only way there is via the proxy.
        "AWS_ENDPOINT_URL_S3": _S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
        "AWS_DEFAULT_REGION": REGION,
        "DEADLINE_CONFIG_FILE_PATH": str(config_file),
        # No ambient proxy / CA so the configured settings are the only influence.
        "NO_PROXY": "",
        "no_proxy": "",
        "HTTPS_PROXY": "",
        "https_proxy": "",
        "AWS_MAX_ATTEMPTS": "1",
        "AWS_RETRY_MODE": "standard",
    }
    # Remove rather than blank the CA bundle vars: botocore >= 1.43.54 rejects an
    # empty-string CA bundle with InvalidConfigError.
    for var in ("AWS_CA_BUNDLE", "REQUESTS_CA_BUNDLE"):
        env.pop(var, None)
    try:
        yield env, proxy, ca_pem, s3_client
    finally:
        proxy.stop()


def _run(env: dict, *args: str, timeout: int = 90) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["deadline", *args], env=env, capture_output=True, text=True, timeout=timeout
    )


def _config_set(env: dict, key: str, value: str) -> None:
    r = _run(env, "config", "set", key, value)
    assert r.returncode == 0, f"config set {key} failed: {r.stderr or r.stdout}"


@skip_on_windows
@pytest.mark.timeout(180)
def test_attachment_upload_download_routes_s3_through_configured_proxy(s3_proxy_setup, tmp_path):
    """
    With https_proxy + ca_bundle set, ``deadline attachment`` S3 traffic (built by
    the job_attachments library) reaches the (moto) S3 backend via the proxy, and a
    CONNECT to the realistic S3 host is recorded -- proof the job_attachments S3
    client honored both settings.
    """
    env, proxy, ca_pem, s3_client = s3_proxy_setup
    s3_root_uri = f"s3://{BUCKET}/{ROOT_PREFIX}"

    _config_set(env, "telemetry.opt_out", "true")
    _config_set(env, "settings.https_proxy", proxy.url)
    _config_set(env, "settings.ca_bundle", str(ca_pem))

    # --- snapshot a manifest of some local files ---
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("alpha")
    (src / "b.txt").write_text("bravo" * 100)
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    r = _run(env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests))
    assert r.returncode == 0, r.stderr or r.stdout
    manifest = next(manifests.glob("*.manifest"))

    # --- upload through the proxy (S3-only path: --s3-root-uri + --profile) ---
    r = _run(
        env,
        "attachment",
        "upload",
        "--manifests",
        str(manifest),
        "--root-dirs",
        str(src),
        "--s3-root-uri",
        s3_root_uri,
        "--profile",
        "default",
    )
    assert r.returncode == 0, f"upload failed: {r.stderr or r.stdout}"

    # The job_attachments S3 client genuinely traversed the proxy to the real host.
    assert proxy.connect_targets, "no CONNECT reached the proxy"
    assert any(_S3_HOST in t for t in proxy.connect_targets), proxy.connect_targets

    # Objects actually landed in the (moto) backend via the proxied tunnel.
    listing = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=f"{ROOT_PREFIX}/Data/")
    assert len(listing.get("Contents", [])) == 2, listing

    # --- download them back through the proxy ---
    dest = tmp_path / "out"
    dest.mkdir()
    r = _run(
        env,
        "attachment",
        "download",
        "--manifests",
        str(manifest),
        "--path-mapping-rules",
        _write_mapping(tmp_path, str(src), str(dest)),
        "--s3-root-uri",
        s3_root_uri,
        "--profile",
        "default",
    )
    assert r.returncode == 0, f"download failed: {r.stderr or r.stdout}"
    assert (dest / "a.txt").read_text() == "alpha"
    assert (dest / "b.txt").read_text() == "bravo" * 100


@skip_on_windows
@pytest.mark.timeout(180)
def test_attachment_upload_without_proxy_cannot_reach_s3(s3_proxy_setup, tmp_path):
    """
    Negative control: without the proxy the realistic S3 endpoint is unreachable.

    The CA bundle is still configured, isolating the proxy as the single variable.
    The S3 endpoint is overridden to an unresolvable ``.invalid`` host so the call
    fails at DNS resolution offline, and the proxy records no CONNECTs.
    """
    env, proxy, ca_pem, _s3_client = s3_proxy_setup
    env = {**env, "AWS_ENDPOINT_URL_S3": f"https://s3.{REGION}.amazonaws.invalid"}
    s3_root_uri = f"s3://{BUCKET}/{ROOT_PREFIX}"

    _config_set(env, "telemetry.opt_out", "true")
    _config_set(env, "settings.ca_bundle", str(ca_pem))
    # Deliberately do NOT set settings.https_proxy.

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("alpha")
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    r = _run(env, "manifest", "snapshot", "--root", str(src), "--destination", str(manifests))
    assert r.returncode == 0, r.stderr or r.stdout
    manifest = next(manifests.glob("*.manifest"))

    r = _run(
        env,
        "attachment",
        "upload",
        "--manifests",
        str(manifest),
        "--root-dirs",
        str(src),
        "--s3-root-uri",
        s3_root_uri,
        "--profile",
        "default",
    )
    assert r.returncode != 0, f"expected failure without proxy, got:\n{r.stdout}"
    assert not proxy.connect_targets, proxy.connect_targets


def _write_mapping(tmp_path: Path, source: str, destination: str) -> str:
    import json

    path = tmp_path / "mapping.json"
    path.write_text(
        json.dumps(
            [
                {
                    "source_path_format": "posix",
                    "source_path": source,
                    "destination_path": destination,
                }
            ]
        )
    )
    return str(path)
