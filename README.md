# StatusRelay

[![CI](https://github.com/neisii/status-relay-rebuild/actions/workflows/ci.yml/badge.svg)](https://github.com/neisii/status-relay-rebuild/actions/workflows/ci.yml)

primary upstream의 상태를 주기적으로 점검하다가 장애로 판단되면 fallback upstream으로 요청을 돌리는 HTTP failover 프록시다. primary가 회복되면 다시 primary로 돌아간다.

## 개요

- 프록시가 요청을 현재 선택된 upstream(primary 또는 fallback)으로 그대로 전달한다. SSE와 Upgrade(WebSocket 등) 요청도 통과시킨다.
- 헬스 체커가 primary URL을 일정 간격으로 점검하고, 연속 실패 횟수가 기준에 닿으면 fallback으로 전환한다.
- `/metrics/json`으로 현재 상태와 요청, 점검 통계를 JSON으로 볼 수 있다.
- 외부 의존성이 없다(Go 표준 라이브러리만 사용).

## 요구 사항과 빌드

Go 1.22 이상이 필요하다.

```sh
go build -o status-relay .
```

빌드 결과물 `status-relay`는 `.gitignore`에 들어 있다.

## 실행

```sh
./status-relay start \
  --primary  https://api.primary.example.com \
  --fallback https://api.backup.example.com \
  --port     8080
```

`--primary`와 `--fallback`은 필수다. 서브커맨드는 `start` 하나이고, 모든 flag는 위치 인자보다 앞에 둬야 한다. 값 형식은 Go flag 규칙을 따른다(`-port 8080`과 `--port 8080` 모두 가능, 기간은 `200ms`, `1.5s` 같은 형식).

### flag

| flag | 기본값 | 허용 범위 | 설명 |
|---|---|---|---|
| `--primary` | 없음(필수) | 호스트가 있는 http 또는 https URL | primary upstream. 헬스 체크도 이 URL로 보낸다 |
| `--fallback` | 없음(필수) | 호스트가 있는 http 또는 https URL | fallback upstream |
| `--port` | `8080` | 0 이상 65535 이하 | 수신 포트. 0이면 OS가 포트를 고르며 로그에는 `:0`으로만 표시된다 |
| `--interval` | `200ms` | 10ms 이상 | 헬스 체크 간격 |
| `--probe-timeout` | `1.5s` | 10ms 이상 | 점검 요청 한 번의 시한 |
| `--fail-threshold` | `3` | 1 이상 | fallback으로 전환하기까지 연속 실패 횟수 |
| `--latency-threshold` | `1000` | 1 이상 9223372036854 이하(밀리초) | 이 값을 넘는 지연을 실패로 본다 |
| `--recover-threshold` | `2` | 1 이상 | primary로 복귀하기까지 연속 성공 횟수 |

범위를 벗어나거나 형식이 틀린 값이 있으면 문제마다 `Error:` 한 줄과 사용법을 stderr에 출력하고 종료한다.

## 엔드포인트

| 경로 | 동작 |
|---|---|
| `/metrics/json` | 지표 JSON. 최상위 키는 `version`, `timestamp`, `uptime_seconds`, `current_state`, `failover_events`, `request_stats`, `health_check_stats` |
| 그 밖의 모든 경로 | 현재 선택된 upstream으로 프록시 |

```sh
curl -s localhost:8080/metrics/json
```

`current_state.active_target`이 현재 upstream(`primary` 또는 `fallback`)이다. 지표는 메모리에만 있어 프로세스를 다시 시작하면 초기화되고, 지연 시간 통계는 primary, fallback, 점검 각각 최근 1000개 표본으로 계산한다.

## 종료 코드

| 코드 | 경우 |
|---|---|
| 0 | `start -h`, SIGINT 또는 SIGTERM으로 정상 종료 |
| 1 | 인자가 없거나 첫 인자가 `start`가 아님, `--primary` 또는 `--fallback` 누락, 포트 listen 실패, 서버 오류 |
| 2 | 알 수 없는 flag 등 flag 파싱 오류, 범위나 형식이 틀린 값, 위치 인자 |

## 동작 개요

점검과 전환 기준은 다음과 같다.

- 헬스 체커는 `--interval`마다 `--primary`에 준 URL을 GET한다(경로 포함). 시한은 `--probe-timeout`이고 리다이렉트는 따라가지 않는다. **fallback은 점검하지 않는다.**
- 다음 중 하나면 그 점검은 실패다: 요청 오류(시한 초과 포함), 상태 코드가 200 미만 또는 500 이상, 지연이 `--latency-threshold`ms 초과. 3xx와 4xx는 실패가 아니다.
- primary를 쓰는 중 연속 `--fail-threshold`회 실패하면 fallback으로 전환한다(failover).
- fallback을 쓰는 중 연속 `--recover-threshold`회 성공하면 primary로 복귀한다(recovery).
- 요청 통계의 성공은 응답 상태 코드가 200 이상 500 미만인 경우다.

## 테스트와 CI

```sh
go build ./...
gofmt -l .          # 출력이 없어야 한다
go vet ./...
go test -race ./...
```

GitHub Actions(`.github/workflows/ci.yml`)가 main 대상 PR과 main push마다 같은 검사를 Go 1.22와 최신 안정판으로 돌린다.

## 알려진 한계

- **긴 응답 시한**: 서버의 쓰기 시한이 30초다. 응답 `Content-Type`이 `text/event-stream`(SSE)이면 시한을 풀지만, 그 밖의 긴 응답(NDJSON, 장시간 다운로드, 30초 넘게 걸리는 upstream 응답)은 30초에 끊긴다.
- **느린 클라이언트**: 시한을 푼 SSE 연결은 클라이언트가 응답을 읽지 않으면 연결이 남을 수 있다.
- **종료 시 열린 SSE**: 종료 신호 뒤 열린 요청을 최대 5초 기다린다. SSE처럼 끝나지 않는 연결이 있으면 5초를 모두 쓰고 강제로 끊긴다.
- **끊긴 응답의 기록**: 시한에 걸려 끊긴 응답은 로그와 요청 통계에 남지 않는다. 일반 응답은 클라이언트가 중간에 끊겨도 성공으로 기록될 수 있다.
- **지연 통계**: 지연 시간은 응답이 끝날 때까지의 전체 시간이라 긴 SSE와 Upgrade 세션이 평균과 p95를 크게 올린다. 성공한 Upgrade의 기록 상태 코드는 101이 아니라 200이다.
- **프로토콜**: 서버는 평문 HTTP/1.1이다. TLS 종단과 HTTP/2는 지원하지 않는다.
- **fallback 점검 없음**: fallback의 상태는 보지 않는다. 연결할 수 없는 upstream으로 보낸 요청은 502(Bad Gateway)로 응답한다.

긴 응답 시한, 종료 시 SSE, 끊긴 응답의 기록, 지연 통계는 실제 바이너리로 시험한 결과다. 느린 클라이언트와 실제 브라우저 `EventSource` 동작은 시험하지 않았다.
