#!/usr/bin/env python3
"""Is the object store actually slow right now?

The agent is rejecting acks=all produces with "record not durable in object
storage within the deadline", while its own successful durable waits all sit
under 2.5s. Those two facts only reconcile if some writes never complete rather
than completing slowly -- so this times writes directly rather than reading the
histogram, which only records the ones that finished.
"""
import os
import subprocess
import time
import uuid

ENDPOINT = "http://127.0.0.1:9000/kimistore/"
ENVFILE = os.environ.get("KIMISTORE_ENV", "/home/andy/kimistore/kimistore.env")


def creds():
    ak = sk = None
    for line in open(ENVFILE):
        if line.startswith("AWS_ACCESS_KEY_ID="):
            ak = line.split("=", 1)[1].strip()
        elif line.startswith("AWS_SECRET_ACCESS_KEY="):
            sk = line.split("=", 1)[1].strip()
    return ak, sk


def timed_write(ak, sk, key, size):
    payload = "/tmp/probe.bin"
    with open(payload, "wb") as fh:
        fh.write(os.urandom(size))
    t0 = time.time()
    p = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                        "-X", "PUT", "--aws-sigv4", "aws:amz:us-east-1:s3",
                        "-u", f"{ak}:{sk}", "--data-binary", f"@{payload}", ENDPOINT + key],
                       capture_output=True, text=True)
    dt = time.time() - t0
    subprocess.run(["curl", "-s", "-o", "/dev/null", "-X", "DELETE", "--aws-sigv4",
                    "aws:amz:us-east-1:s3", "-u", f"{ak}:{sk}", ENDPOINT + key],
                   capture_output=True)
    return dt, p.stdout.strip()


def main():
    ak, sk = creds()
    print(f"  {'size':>10}  {'seconds':>9}  http")
    for size in (64 * 1024, 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024):
        times = []
        code = ""
        for _ in range(3):
            dt, code = timed_write(ak, sk, f"_probe/{uuid.uuid4().hex}", size)
            times.append(dt)
        print(f"  {size:>10,}  {min(times):>9.2f}  {code}   (3 runs, slowest {max(times):.2f}s)")

    print()
    print("  --- what else is competing for the object store? ---")
    out = subprocess.run(
        ["docker", "logs", "--since", "20m", "mimir2-compactor"],
        capture_output=True, text=True).stdout
    jobs = out.count("job_type=merge") + out.count("job_type=vertical")
    print(f"    mimir2 compactor merge jobs in 20m : {jobs}")
    out2 = subprocess.run(
        ["docker", "logs", "--since", "20m", "mimir2-store-gateway"],
        capture_output=True, text=True).stdout
    errs = out2.count("level=error")
    print(f"    mimir2 store-gateway errors in 20m   : {errs}")


if __name__ == "__main__":
    main()
