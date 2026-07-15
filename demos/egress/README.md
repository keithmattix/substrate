# Egress Demo

This demo runs an Actor that accepts `{"url":"..."}`, sends an HTTP GET to
that URL, and returns the upstream status and body. It is intended to exercise
Substrate's per-Actor egress allowlist.

Only Actors with an egress policy are redirected through the egress gateway.
Actors created without `--egress-host` use direct egress. A present policy with
an empty host list is an explicit deny-all policy.

Deploy the WorkerPool and ActorTemplate:

```bash
./hack/install-ate.sh --deploy-demo-egress
```

Create an Actor that may connect only to `example.com`:

```bash
kubectl ate create atespace demo
kubectl ate create actor egress-demo \
  --atespace demo \
  --template ate-demo-egress/egress \
  --egress-host example.com
kubectl port-forward -n ate-system service/ateway-ingress 8000:80
```

Fetch an allowed URL through the Actor:

```bash
curl -X POST http://localhost:8000/ \
  -H 'Host: egress-demo.demo.actors.resources.substrate.ate.dev' \
  -H 'Content-Type: application/json' \
  -d '{"url":"http://example.com/"}'
```

A request for another hostname, such as `http://example.org/`, should fail.

## E2E test

Deploy this demo, then run the networking suite:

```bash
./hack/run-e2e-kind.sh ./internal/e2e/suites/networking
```

Remove the demo resources with:

```bash
./hack/install-ate.sh --delete-demo-egress
```
