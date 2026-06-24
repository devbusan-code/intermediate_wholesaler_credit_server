# intermediate_wholesaler_credit_server

중도매인 여신 약정한도 조회, 홀드, 사용 차감, 사용 취소를 처리하는 FastAPI 기반 API 서버입니다.

이 서버는 MySQL에 저장된 중도매인 여신 한도를 기준으로 거래 가능 여부를 확인하고, 거래 단계에 따라 `hold_amount`, `use_amount`를 갱신하며, 요청/응답 및 금액 변경 이력을 로그 테이블에 기록합니다.

## 주요 기능

| 구분 | 엔드포인트 | 설명 |
| --- | --- | --- |
| 약정한도 조회 | `POST /WH_API_T01_01` | 중도매인별 사용 가능 한도를 조회하고 요청 금액 처리 가능 여부를 반환합니다. |
| 여신 홀드 | `POST /WH_API_T01_02` | 거래 품목별 주문 금액을 약정한도에서 홀드 처리하고 `BUND_EGM_ID`를 발급합니다. |
| 여신 사용 | `POST /WH_API_T01_03` | 홀드된 주문을 실제 사용 처리합니다. |
| 사용 취소 | `POST /WH_API_T01_04` | 홀드 취소 또는 사용 취소를 처리합니다. |

## 기술 스택

- Python `>=3.12`
- FastAPI
- Pydantic
- MySQL Connector/Python
- python-dotenv
- uv

## 프로젝트 구조

```text
.
|-- main.py          # FastAPI 앱, DB 처리, API 엔드포인트
|-- logger_kki.py    # 로그 설정
|-- pyproject.toml   # 프로젝트 메타데이터 및 의존성
|-- uv.lock          # uv lockfile
`-- README.md        # 프로젝트 문서
```

## 환경 변수

루트 디렉터리에 `.env` 파일을 생성하고 MySQL 접속 정보를 설정합니다.

```env
MYSQL_HOST=
MYSQL_PORT=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DATABASE=
```

`MYSQL_PORT`가 비어 있으면 코드상 기본값은 `3306`입니다.

## 설치

```bash
uv sync
```

의존성을 직접 추가해야 하는 경우:

```bash
uv add "fastapi[standard]" python-dotenv mysql-connector-python public_ip
```

## 실행

개발 실행:

```bash
uv run fastapi dev
```

Uvicorn으로 실행:

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8081
```

백그라운드 운영 실행 예시:

```bash
nohup uv run uvicorn main:app --host 0.0.0.0 --port 8081 > /dev/null 2>&1 &
```

실행 확인:

```bash
ps -aux | grep python
```

API 문서:

```bash
curl http://localhost:8081/docs
curl http://localhost:8081/redoc
```

브라우저에서는 다음 주소를 사용합니다.

- Swagger UI: `http://localhost:8081/docs`
- ReDoc: `http://localhost:8081/redoc`

## Linux systemd 서비스 등록

Linux 운영 환경에서는 `nohup`보다 `systemd` 서비스로 등록해 `systemctl`로 시작, 중지, 재시작, 자동 실행을 관리하는 방식을 권장합니다.

먼저 서버에서 `uv` 실행 경로를 확인합니다.

```bash
which uv
```

예를 들어 프로젝트 경로가 `/home/app/intermediate_wholesaler_credit_server`, 실행 계정이 `app`, `uv` 경로가 `/home/app/.local/bin/uv`라면 다음 서비스 파일을 생성합니다.

```bash
sudo vi /etc/systemd/system/intermediate-wholesaler-credit.service
```

```ini
[Unit]
Description=Intermediate Wholesaler Credit FastAPI Server
After=network.target

[Service]
Type=simple
User=app
Group=app
WorkingDirectory=/home/app/intermediate_wholesaler_credit_server
EnvironmentFile=/home/app/intermediate_wholesaler_credit_server/.env
ExecStart=/home/app/.local/bin/uv run uvicorn main:app --host 0.0.0.0 --port 8081
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

다음 항목은 실제 서버 환경에 맞게 수정해야 합니다.

- `User`: 서비스를 실행할 Linux 계정
- `Group`: 서비스를 실행할 Linux 그룹
- `WorkingDirectory`: 프로젝트 디렉터리 절대 경로
- `EnvironmentFile`: `.env` 파일 절대 경로
- `ExecStart`: `uv` 절대 경로와 실행 명령

서비스 파일을 저장한 뒤 `systemd`에 반영하고 실행합니다.

```bash
sudo systemctl daemon-reload
sudo systemctl enable intermediate-wholesaler-credit
sudo systemctl start intermediate-wholesaler-credit
```

상태 확인:

```bash
sudo systemctl status intermediate-wholesaler-credit
```

로그 확인:

```bash
sudo journalctl -u intermediate-wholesaler-credit -f
```

서비스 제어:

```bash
sudo systemctl restart intermediate-wholesaler-credit
sudo systemctl stop intermediate-wholesaler-credit
sudo systemctl start intermediate-wholesaler-credit
```

서비스 파일을 수정한 경우에는 반드시 다음 명령을 다시 실행한 뒤 재시작합니다.

```bash
sudo systemctl daemon-reload
sudo systemctl restart intermediate-wholesaler-credit
```

## 접근 제한

`main.py`의 HTTP 미들웨어에서 요청 클라이언트 IP를 검사합니다. 허용 목록에 없는 IP는 `403 Forbidden`으로 차단됩니다.

현재 허용 IP는 `main.py`의 `allowed_ips`에서 관리합니다.

```python
allowed_ips = [
    "127.0.0.1",
    "58.231.240.248",
    "180.210.117.190",
    "180.210.117.184",
]
```

운영 환경에서 프록시, 로드밸런서, 컨테이너 네트워크를 사용하는 경우 `request.client.host`가 실제 호출자 IP가 아닐 수 있으므로 배포 구조에 맞게 확인이 필요합니다.

## API 요약

### `POST /WH_API_T01_01`

약정한도 조회 API입니다.

주요 요청 필드:

- `CNTC_TYPE_CD`: `CHECK`
- `PRCHR_PRMS_NO`: 구매자 허가번호
- `TOT_TRNS_AMT`: 총 거래 금액
- `LOAN_DV_CD`: `HOLD` 또는 `USE`

주요 응답:

```json
{
  "RESULT": "true",
  "MESSAGE": "성공",
  "DATA": {
    "PRCS_STAT_CD": "0"
  }
}
```

처리 상태:

- `0`: 정상
- `1`: 거절 또는 한도 초과
- `2`: 오류

### `POST /WH_API_T01_02`

여신 홀드 API입니다.

주요 요청 필드:

- `CNTC_TYPE_CD`: `HOLD`
- `TRNS_ID`: 거래 ID
- `PRCHR_PRMS_NO`: 구매자 허가번호
- `TOT_TRNS_AMT`: 총 거래 금액
- `TRNS_ITEM`: 거래 품목 목록

각 품목 처리 시 `daily_sequence`를 사용해 `BUND_EGM_ID`를 생성하고, `intermediate_wholesaler_credit_order`에 주문을 저장한 뒤 `hold_amount`를 증가시킵니다.

주요 응답:

```json
{
  "RESULT": "true",
  "MESSAGE": "성공",
  "DATA": {
    "PRCS_STAT_CD": "0",
    "TRNS_ID": "거래ID",
    "TRNS_WAY_CD": "거래방식코드",
    "TRNS_ITEM": [
      {
        "BUND_EGM_ID": "약정한도ID",
        "TRNS_DETL_ID": "거래상세ID",
        "PRDCT_DETL_ID": "상품상세ID",
        "ORDR_AMT": "주문금액"
      }
    ]
  }
}
```

### `POST /WH_API_T01_03`

여신 사용 차감 API입니다.

주요 요청 필드:

- `CNTC_TYPE_CD`: `USE`
- `TRNS_ITEM_CNT`: 품목 수
- `TRNS_ITEM[].BUND_EGM_ID`: 홀드 API에서 발급된 약정한도 ID
- `TRNS_ITEM[].ORDR_AMT`: 주문 금액

홀드 상태의 주문을 조회한 뒤 `hold_amount`를 감소시키고 `use_amount`를 증가시킵니다.

### `POST /WH_API_T01_04`

약정한도 사용 취소 API입니다.

주요 요청 필드:

- `CNTC_TYPE_CD`: `CANCEL`
- `LOAN_USE_CD`: `01` 또는 `02`
- `TRNS_ITEM[].BUND_EGM_ID`: 약정한도 ID

`LOAN_USE_CD` 값:

- `01`: 홀드 취소
- `02`: 사용 취소

## DB 테이블

코드에서 사용하는 주요 테이블은 다음과 같습니다.

| 테이블 | 용도 |
| --- | --- |
| `intermediate_wholesaler_credit` | 중도매인별 여신 한도, 홀드 금액, 사용 금액 관리 |
| `intermediate_wholesaler_credit_order` | 발급된 약정한도 ID와 주문 상태 관리 |
| `intermediate_wholesaler_credit_log` | 금액 변경 이력 저장 |
| `intermediate_wholesaler_credit_api_log` | API 요청/응답 로그 저장 |
| `daily_sequence` | 일자별 `BUND_EGM_ID` 시퀀스 관리 |

## 거래 상태 구분

코드의 `flag_division` 의미는 다음과 같습니다.

| 값 | 의미 |
| --- | --- |
| `0` | 조회 |
| `1` | 홀드 |
| `2` | 사용 |
| `3` | 홀드 취소 |
| `4` | 사용 취소 |
| `9` | 기타 |

## 로그

로그 설정은 `logger_kki.py`에서 관리합니다. 런타임 로그 파일은 `logs/` 디렉터리에 생성되며, `.gitignore`에 의해 Git 추적 대상에서 제외됩니다.

## 운영 참고

- MySQL 커넥션 풀 크기는 `main.py`에서 `20`으로 설정되어 있습니다.
- 애플리케이션 시작 시 MySQL 커넥션 풀이 생성되므로 `.env` 값이 올바르지 않으면 서버 기동 단계에서 실패할 수 있습니다.
- 모든 API 요청은 `intermediate_wholesaler_credit_api_log`에 요청/응답 내용이 기록됩니다.
- 금액 처리는 `Decimal`을 사용합니다.
- 상세 요청 스키마와 필수 필드는 서버 실행 후 `/docs`에서 확인하는 것이 가장 정확합니다.
