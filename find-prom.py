#!/usr/bin/env python3
"""Find the Prometheus that is actually scraping this cluster.

kube-prometheus-stack's Prometheus is not in the prometheus namespace -- the
operator and the node-exporters are, but the server itself is somewhere else, and
the scrape data (kubelet, apiserver, cilium) says a full kube-prometheus scrape
set exists. So this asks the API server rather than guessing: every pod with a
container whose image looks like a Prometheus server.
"""
import json
import subprocess
import sys


def kubectl(*args):
    return subprocess.run(["kubectl", *args], capture_output=True, text=True).stdout


def main():
    d = json.loads(kubectl("get", "pods", "-A", "-o", "json"))
    if not d:
        print("  could not list pods")
        return
    found = False
    for it in d.get("items", []):
        meta = it["metadata"]
        for c in it["spec"].get("containers", []):
            img = c.get("image", "")
            low = img.lower()
            if "prometheus" in low and "operator" not in low and "node-exporter" not in low and "adapter" not in low:
                found = True
                node = it["spec"].get("nodeName")
                pod_ip = it["status"].get("podIP")
                phase = it["status"].get("phase")
                print(f"  {meta['namespace']}/{meta['name']}")
                print(f"      image={img}  node={node}  podIP={pod_ip}  phase={phase}")
                for p in it["spec"].get("containers", []):
                    for port in p.get("ports", []):
                        print(f"      port {port.get('name', '-')}:{port.get('containerPort')}")
    if not found:
        print("  no prometheus server pod found")
        print("  namespaces present:")
        seen = sorted({it["metadata"]["namespace"] for it in d["items"]})
        for n in seen:
            print("   ", n)


if __name__ == "__main__":
    main()
