# Egress Demo

This demo runs an Actor that accepts `{"url":"..."}`, sends an HTTP GET to
that URL, and returns the upstream status and body. It is a fixture for future
end-to-end egress gateway tests. The gateway and tunnel plumbing are installed,
but the public Actor API does not yet provide a way to opt into tunneled egress.

Deploy the WorkerPool and ActorTemplate:

```bash
./hack/install-ate.sh --deploy-demo-egress
```

Create an Actor:

```bash
kubectl ate create atespace demo
kubectl ate create actor egress-demo \
  --atespace demo \
  --template ate-demo-egress/egress
kubectl port-forward -n ate-system service/ateway-ingress 8000:80
```

Fetch a URL through the Actor's current direct-egress path:

```bash
curl -X POST http://localhost:8000/ \
  -H 'Host: egress-demo.demo.actors.resources.substrate.ate.dev' \
  -H 'Content-Type: application/json' \
  -d '{"url":"http://example.com/"}'
```

## E2E test

Deploy this demo, then run the networking ingress suite:

```bash
./hack/run-e2e-kind.sh ./internal/e2e/suites/networking
```

Remove the demo resources with:

```bash
./hack/install-ate.sh --delete-demo-egress
```
