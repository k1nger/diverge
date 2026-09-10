# Fork ledger — k1nger/diverge

What this fork carries on top of `divergedev/diverge`, so a rebase never has
to re-derive it. Branch `local-dev-on-upstream` = `upstream/main` (e22c663,
2026-09-10, rebuilt after the lint fixes) + the commits below, in this order. Every fork commit lands with a
row here.

Verified by CONTENT against upstream, not by commit subject
(`git cherry` lies after a squash-merge).

| commit | fix | why it is still ours (upstream marker checked 2026-09-10) | PR |
|---|---|---|---|
| 9c980fa | `fix(transport)`: the tunnel speaks HTTP/2 to a plaintext server (h2c) | no `AllowHTTP`/`http2.Transport` in `internal/cli`; a plaintext server answers the bidi stream with 505 | [#281](https://github.com/divergedev/diverge/pull/281) |
| 9ffc120 | `fix(server)`: the tunnel stream is not bounded by `ReadTimeout` | `cmd/server/main.go` RPC server still `ReadTimeout: 30s`; the stream dies at 30s | [#282](https://github.com/divergedev/diverge/pull/282) |
| 2608f53 | `fix(server)`: the tunnel endpoint is marked Ready so its DNS resolves | upstream EndpointSlice has no `Conditions.Ready` | [#283](https://github.com/divergedev/diverge/pull/283) |
| d1f8d89 | `fix(server)`: the tunnel is resolvable under kube-dns too (core/v1 Endpoints) | no `corev1.Endpoints{` in `internal/server/tunnel.go` | [#283](https://github.com/divergedev/diverge/pull/283) |
| ddbb386 | `fix(server)`: the tunnel Service is a real ClusterIP that remaps the port | `tunnel.go` still `ClusterIP: "None"` | [#283](https://github.com/divergedev/diverge/pull/283) |
| fd30188 | `fix(server)`: a signed-out browser is sent to sign in, not handed a 401 | no `LoginURL` on `internal/server/auth/middleware.go` (the `loginUrl` in `oidc_handler.go` is dashboard config, not a redirect) | [#284](https://github.com/divergedev/diverge/pull/284) |
| 63b6801 | `fix(cli)`: `dev` survives a PreviewGroup the installed CRD refuses | no `IsInvalid` handling in `internal/cli` | [#285](https://github.com/divergedev/diverge/pull/285) |
| f765475 | `fix(cli)`: a missing PreviewGroup means no async routes to wait for | no `IsNotFound` branch on the async-route wait | [#285](https://github.com/divergedev/diverge/pull/285) |

Merged upstream and DROPPED from the fork on the 2026-09-10 rebase:
#243 (SAR against the API group), #244 (server image tag fallback),
#245 (secure cookies, revised), #246 (chart-installed server discovery),
#247 (istio provider RBAC), #248 (tunnel auth, revised), plus our
`--server/--token` docs which #246/#248 superseded.

## Rebase notes

- The h2c commit was re-expressed against upstream's
  `NewTunnelClientWithTokenSource`: a nil base transport still means
  `http.DefaultTransport` (two upstream tests drive the client with
  HTTP/1.1 stubs), and the two real constructors — `NewTunnelClient` and
  the `dev` command — pass `tunnelBaseTransport(serverAddr)`.
- h2c is done with `http.Server.Protocols` / `http.Transport.Protocols`
  (Go 1.24+), not `x/net/http2/h2c`, which staticcheck flags as deprecated.
- core/v1 Endpoints carries a justified `//nolint:staticcheck`: deprecated,
  and precisely what kube-dns still reads.

## Upstream changes since v0.10.0 that affect OUR deployment (not the code)

- #257 migrated the CRD API group and labels from `diverge.io` to
  `divergedev.com`. `environments/platform/base/core/diverge-server/dev-tunnel-rbac.yaml`
  grants on `diverge.io`; it must follow when the server is upgraded.
- #258 modernized the chart: `routingProvider` accepts `composite` and
  comma-separated providers; new `certManager:` and `rbac:` values blocks.
  Check keys before moving `targetRevision` off `v0.8.2`.
- #260 server discovery now uses `part-of` labels with an RBAC fallback.
- #266 `TokenSource` with dynamic reload, OpenBao and `KubeTokenSource` —
  a candidate replacement for the manual `kubectl create token` step.
- #273 multi-user conflict detection and session leasing for `diverge dev`.
