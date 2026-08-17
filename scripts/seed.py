"""Seed MOSAIC with a batch of contextualized events (run while the app is up)."""
import urllib.request, json
BASE = "http://localhost:8000"
def call(path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(BASE+path, data=data,
        headers={"Content-Type":"application/json"}, method="POST" if body else "GET")
    return json.loads(urllib.request.urlopen(req).read())
for _ in range(20):
    for rd in call("/api/emit")["readings"]:
        call("/api/contextualize", rd)
print("seeded:", call("/api/platform"))
