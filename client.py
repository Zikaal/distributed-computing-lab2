import argparse
import urllib.request
import urllib.parse
import json

parser = argparse.ArgumentParser()
parser.add_argument("--node", required=True)
parser.add_argument("cmd", choices=["put", "get", "status", "sync"])
parser.add_argument("key", nargs="?")
parser.add_argument("value", nargs="?")
args = parser.parse_args()

base = args.node.rstrip("/")

def pretty_print(resp_bytes):
    text = resp_bytes.decode("utf-8") if resp_bytes else ""
    try:
        obj = json.loads(text) if text else {}
        print(json.dumps(obj, indent=2))
    except Exception:
        print(text)

if args.cmd == "put":
    if args.key is None or args.value is None:
        raise SystemExit("Usage: put <key> <value>")
    data = json.dumps({"key": args.key, "value": args.value}).encode("utf-8")
    req = urllib.request.Request(base + "/put", data=data, headers={"Content-Type": "application/json"}, method="POST")
    pretty_print(urllib.request.urlopen(req).read())

elif args.cmd == "get":
    if args.key is None:
        raise SystemExit("Usage: get <key>")
    url = base + "/get?key=" + urllib.parse.quote(args.key)
    pretty_print(urllib.request.urlopen(url).read())

elif args.cmd == "status":
    pretty_print(urllib.request.urlopen(base + "/status").read())

elif args.cmd == "sync":
    req = urllib.request.Request(base + "/sync", data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
    pretty_print(urllib.request.urlopen(req).read())