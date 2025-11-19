# install dependencies:
# uv add "fastapi[standard]" python-dotenv mysql-connector-python public_ip

from logger_kki import LoggerKKI
import os
from dotenv import load_dotenv
import ipaddress
from decimal import Decimal
import mysql.connector
from mysql.connector import pooling
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import json
from datetime import datetime
import json # json 모듈 추가

# kki_logger.py에서 logger 객체를 가져옵니다.
logger = LoggerKKI().get_logger()

# .env 파일 로드
load_dotenv()

# MySQL 접속 정보 (환경변수에서 불러오기)
db_config = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),  # 기본 포트: 3306
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'charset': 'utf8'
}


####################################################################################################################
# 허용할 IP 또는 IP 대역 목록 (CIDR 표기법 지원)
####################################################################################################################
allowed_ips = [
    "127.0.0.1",
    "58.231.240.248",   # 작업실
    "180.210.117.190",  # 온라인도매시장 운영서버 #1
    "180.210.117.184",  # 온라인도매시장 운영서버 #2
]
allowed_networks = [ipaddress.ip_network(ip) for ip in allowed_ips]
####################################################################################################################


####################################################################################################################
# MySQL Connection Pool 생성 (앱 시작 시 1회만)
####################################################################################################################
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=20,
    pool_reset_session=True,
    **db_config
)
####################################################################################################################


####################################################################################################################
# MySQLKKI Class (커넥션 풀 기반)
####################################################################################################################
class MySQLKKI:
    ####################################################################################################################
    # 생성자
    ####################################################################################################################
    def __init__(self, pool):
        self.pool = pool


    ####################################################################################################################
    # execute_query 메서드
    ####################################################################################################################
    def execute_query(self, query, params=None, return_last_id=False):
        conn, cursor = None, None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor(buffered=True)
            if params:
                logger.info(f"Executing query: {query} | Params: {params}")
                cursor.execute(query, params)
            else:
                logger.info(f"Executing query: {query}")
                cursor.execute(query)
            conn.commit()
            rowcount = cursor.rowcount
            last_id = cursor.lastrowid
            # 마지막 ID를 반환할 경우
            if return_last_id and last_id is not None:
                # 커넥션을 close 하기 전에 마지막 ID를 반환한다
                return last_id
            # 영향을 받은 row 수 반환
            return rowcount
        except mysql.connector.Error as err:
            logger.error(f"MySQL Error: {err}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
        # 무조건 close 하도록 finally에 넣는다
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    ####################################################################################################################
    # execute_and_fetch_all 메서드
    ####################################################################################################################
    def execute_and_fetch_all(self, query, params=None):
        conn, cursor = None, None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor(buffered=True)
            if params:
                logger.info(f"Executing query: {query} | Params: {params}")
                cursor.execute(query, params)
            else:
                logger.info(f"Executing query: {query}")
                cursor.execute(query)
            result = cursor.fetchall()
            return result
        except mysql.connector.Error as err:
            logger.error(f"MySQL Error: {err}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
        # 무조건 close 하도록 finally에 넣는다
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    ####################################################################################################################
    # execute_and_fetch_one 메서드
    ####################################################################################################################
    def execute_and_fetch_one(self, query, params=None):
        conn, cursor = None, None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor(buffered=True)
            if params:
                logger.info(f"Executing query: {query} | Params: {params}")
                cursor.execute(query, params)
            else:
                logger.info(f"Executing query: {query}")
                cursor.execute(query)
            result = cursor.fetchone()
            return result
        except mysql.connector.Error as err:
            logger.error(f"MySQL Error: {err}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
        # 무조건 close 하도록 finally에 넣는다
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    ####################################################################################################################
    # execute_and_fetch_one_with_conn 메서드 (트랜잭션 내에서 단일 행 조회)
    ####################################################################################################################
    def execute_and_fetch_one_with_conn(self, conn, query, params=None):
        cursor = None
        try:
            cursor = conn.cursor(buffered=True)
            if params:
                logger.info(f"Executing query with conn: {query} | Params: {params}")
                cursor.execute(query, params)
            else:
                logger.info(f"Executing query with conn: {query}")
                cursor.execute(query)
            result = cursor.fetchone()
            return result
        except mysql.connector.Error as err:
            logger.error(f"MySQL Error during transaction: {err}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during transaction: {e}")
            raise
        finally:
            if cursor:
                cursor.close()


    ####################################################################################################################
    # 트랜잭션 시작
    ####################################################################################################################
    def start_transaction(self):
        conn = None
        try:
            conn = self.pool.get_connection()
            conn.autocommit = False  # 오토커밋 비활성화
            logger.info("Transaction started.")
            return conn
        except mysql.connector.Error as err:
            logger.error(f"Failed to start transaction: {err}")
            return None


    ####################################################################################################################
    # 트랜잭션 커밋
    ####################################################################################################################
    def commit_transaction(self, conn):
        if conn is None:
            logger.warning("Commit failed: connection is None.")
            return
        try:
            conn.commit()
            logger.info("Transaction committed.")
        except mysql.connector.Error as err:
            logger.error(f"Transaction commit failed: {err}")
        finally:
            conn.autocommit = True  # 오토커밋 다시 활성화
            conn.close()
            logger.info("Connection closed after commit.")


    ####################################################################################################################
    # 트랜잭션 롤백
    ####################################################################################################################
    def rollback_transaction(self, conn):
        if conn is None:
            logger.warning("Rollback skipped: connection is None.")
            return
        try:
            conn.rollback()
            logger.info("Transaction rolled back.")
        except mysql.connector.Error as err:
            logger.error(f"Transaction rollback failed: {err}")
        finally:
            conn.autocommit = True  # 오토커밋 다시 활성화
            conn.close()
            logger.info("Connection closed after rollback.")


    ####################################################################################################################
    # 트랜잭션 내에서 쿼리 실행
    ####################################################################################################################
    def execute_query_with_conn(self, conn, query, params=None, return_last_id=False):
        cursor = None
        try:
            cursor = conn.cursor(buffered=True)
            if params:
                logger.info(f"Executing query with conn: {query} | Params: {params}")
                cursor.execute(query, params)
            else:
                logger.info(f"Executing query with conn: {query}")
                cursor.execute(query)
            
            rowcount = cursor.rowcount
            last_id = cursor.lastrowid

            if return_last_id and last_id is not None:
                return last_id
            
            return rowcount
        except mysql.connector.Error as err:
            logger.error(f"MySQL Error during transaction: {err}")
            raise  # 오류를 다시 발생시켜 상위 except 블록에서 롤백 처리하도록 함
        except Exception as e:
            logger.error(f"Unexpected error during transaction: {e}")
            raise  # 오류를 다시 발생시켜 상위 except 블록에서 롤백 처리하도록 함
        finally:
            if cursor:
                cursor.close()
####################################################################################################################





####################################################################################################################
# FastAPI 시작
####################################################################################################################
# 데이터베이스 연결 객체 생성 (커넥션 풀 기반)
db = MySQLKKI(connection_pool)

app = FastAPI()
####################################################################################################################



####################################################################################################################
# Middleware 시작
####################################################################################################################
@app.middleware("http")
async def ip_allow_middleware(request: Request, call_next):
    client_ip = ipaddress.ip_address(request.client.host)
    if not any(client_ip in net for net in allowed_networks):
        return JSONResponse({"detail": "Forbidden"}, status_code=403)
    return await call_next(request)
####################################################################################################################


####################################################################################################################
# 여신 약정한도 조회
####################################################################################################################
def intermediate_wholesaler_credit_search(jumehuga: str):
    try:
        query = """
            SELECT 
                (credit_amount - hold_amount - use_amount) AS available_amount,
                1 AS exists_flag
            FROM intermediate_wholesaler_credit 
            WHERE jumehuga = %s
        """
        params = (jumehuga,)
        result = db.execute_and_fetch_one(query, params)
        # result가 None 이 아닐 경우
        if result:
            flag_success = True
            available_amount = result[0]
        else:
            flag_success = False
            available_amount = Decimal('0.0')

    except Exception as e:
        logger.error(f"DB 오류: {e}")
        flag_success = False
        available_amount = 0

    finally:
        return flag_success, available_amount
####################################################################################################################





####################################################################################################################
# 일자별 사용된 sequence 조회 및 sequence +1 증가
####################################################################################################################
def daily_sequence_search():
    flag_success = False
    try:
        query = (
            """
            SELECT sequence
            FROM daily_sequence
            WHERE basic_date = CURDATE()
            """
        )
        result = db.execute_and_fetch_one(query)

        # result가 None 이 아닐 경우
        if result is not None:
            flag_success = True
            sequence = result[0]

            # 오늘 날짜가 있다면 +1
            query = (
                """
                UPDATE daily_sequence
                SET sequence = sequence + 1
                WHERE basic_date = CURDATE()
                """
            )
            result = db.execute_query(query)
            # result가 None이 아니고, 0보다 커야 (실제 업데이트가 발생해야) 성공으로 간주
            if result is not None and result > 0:
                pass
            else:
                flag_success = False
                sequence = 0

        else:
            # 오늘 날짜의 sequence가 없으면 새로 생성
            query = (
                """
                INSERT INTO daily_sequence (basic_date, sequence)
                VALUES (CURDATE(), 1)
                """
            )
            result = db.execute_query(query)
            # result가 None이 아니고, 0보다 커야 (실제 업데이트가 발생해야) 성공으로 간주
            if result is not None and result > 0:
                flag_success = True
                sequence = 0
            else:
                flag_success = False
                sequence = 0

    except Exception as e:
        logger.error(f"DB 오류: {e}")
        flag_success = False
        sequence = 0
    return (flag_success, sequence)
####################################################################################################################





####################################################################################################################
# intermediate_wholesaler_credit Table 업데이트 (Transaction)
# flag_division
# 1: 약정한도 홀드
# 2: 약정한도 사용
# 3: 약정한도 홀드취소
# 4: 약정한도 사용취소
####################################################################################################################
def intermediate_wholesaler_credit_update_with_conn(conn, flag_division: int, jumehuga: str, amount: Decimal) -> int:
    query = ""
    params = ()

    try:
        ####################################################################################################################
        # 약정한도 홀드
        ####################################################################################################################
        if flag_division == 1:
            query = """
                UPDATE intermediate_wholesaler_credit
                SET hold_amount = hold_amount + %s
                WHERE jumehuga = %s
            """
            params = (amount, jumehuga)

        ####################################################################################################################
        # 약정한도 사용
        ####################################################################################################################
        elif flag_division == 2:
            query = """
                UPDATE intermediate_wholesaler_credit
                SET hold_amount = GREATEST(0, hold_amount - %s),
                    use_amount = use_amount + %s
                WHERE jumehuga = %s
            """
            params = (amount, amount, jumehuga)

        ####################################################################################################################
        # 약정한도 홀드취소
        ####################################################################################################################
        elif flag_division == 3:
            query = """
                UPDATE intermediate_wholesaler_credit
                SET hold_amount = GREATEST(0, hold_amount - %s)
                WHERE jumehuga = %s
            """
            params = (amount, jumehuga)

        ####################################################################################################################
        # 약정한도 사용취소
        ####################################################################################################################
        elif flag_division == 4:
            query = """
                UPDATE intermediate_wholesaler_credit
                SET use_amount = GREATEST(0, use_amount - %s)
                WHERE jumehuga = %s
            """
            params = (amount, jumehuga)

        ####################################################################################################################
        # 그 외 잘못된 분류값일 경우
        ####################################################################################################################
        else:
            raise ValueError("Invalid flag_division value")

        return db.execute_query_with_conn(conn, query, params)

    except Exception as e:
        logger.error(f"UPDATE (in transaction) 오류: {e}")
        raise # 예외를 다시 발생시켜 트랜잭션이 롤백되도록 함
####################################################################################################################





####################################################################################################################
# intermediate_wholesaler_credit_order Table 업데이트 (Transaction)
# flag_division
# 1: 약정한도 홀드
# 2: 약정한도 사용
# 3: 약정한도 홀드취소
# 4: 약정한도 사용취소
####################################################################################################################
def intermediate_wholesaler_credit_order_update_with_conn(conn, idx: str, flag_division: int) -> int:
    query = ""
    params = ()

    try:
        query = """
            UPDATE intermediate_wholesaler_credit_order
            SET flag_division = %s
            WHERE idx = %s
        """
        params = (flag_division, idx)

        return db.execute_query_with_conn(conn, query, params)

    except Exception as e:
        logger.error(f"UPDATE (in transaction) 오류: {e}")
        raise # 예외를 다시 발생시켜 트랜잭션이 롤백되도록 함
####################################################################################################################





####################################################################################################################
# intermediate_wholesaler_credit Table 업데이트 (Non-Transaction)
####################################################################################################################
def intermediate_wholesaler_credit_update(flag_division: int, jumehuga: str, amount: Decimal) -> bool:
    conn = None
    try:
        conn = db.start_transaction()
        intermediate_wholesaler_credit_update_with_conn(conn, flag_division, jumehuga, amount)
        db.commit_transaction(conn)
        return True
    except Exception as e:
        logger.error(f"UPDATE 오류: {e}")
        if conn:
            db.rollback_transaction(conn)
        return False
####################################################################################################################





####################################################################################################################
# intermediate_wholesaler_credit_order Table 에서 거래에서 사용된 허가번호 & 주문금액 조회
# 가차감시 ==> 조건이 없음 (사용하지 않음)
# 실차감시 ==> flag_division == 1 : 약정한도 홀드가 조건이 됨
# 가차감 취소시 ==> flag_division == 1 : 약정한도 홀드가 조건이 됨
# 실차감 취소시 ==> flag_division == 2 : 약정한도 사용이 조건이 됨
# flag_division
# 1: 약정한도 홀드
# 2: 약정한도 사용
####################################################################################################################
def intermediate_wholesaler_credit_order_search(idx: str, flag_division: int):
    flag_success, jumehuga, order_amount = False, "", 0
    try:
        query = (
            """
            SELECT jumehuga
                , ORDR_AMT
            FROM intermediate_wholesaler_credit_order
            WHERE idx = %s
                AND flag_division = %s
            """
        )
        params = (idx, flag_division)
        result = db.execute_and_fetch_one(query, params)

        # result가 None이 아닐 경우
        if result is not None:
            flag_success = True
            jumehuga = result[0]
            order_amount = result[1]
        else:
            flag_success = False
            jumehuga = ""
            order_amount = 0

    except Exception as e:
        logger.error(f"DB 오류: {e}")
        flag_success = False
        jumehuga = ""
        order_amount = 0
    return (flag_success, jumehuga, order_amount)
####################################################################################################################





####################################################################################################################
# 일자별 사용된 sequence 조회 및 sequence +1 증가 (Transaction)
####################################################################################################################
def daily_sequence_search_with_conn(conn):
    flag_success, sequence = False, 0
    try:
        # 오늘 날짜의 sequence가 없으면 새로 생성 (INSERT ... ON DUPLICATE KEY UPDATE)
        # 오늘 날짜의 row가 있으면 sequence를 1 증가시키고, 없으면 새로 1로 생성합니다.
        query_update = (
            """
            INSERT INTO daily_sequence (basic_date, sequence)
            VALUES (CURDATE(), 1)
            ON DUPLICATE KEY UPDATE sequence = sequence + 1
            """
        )
        db.execute_query_with_conn(conn, query_update)

        # 증가된 sequence 값을 조회합니다.
        query_select = (
            """
            SELECT sequence
            FROM daily_sequence
            WHERE basic_date = CURDATE()
            """
        )
        
        # execute_query_with_conn는 rowcount나 last_id를 반환하므로,
        # fetchone이 필요한 경우 커서를 직접 사용해야 합니다.
        # 여기서는 간단하게 별도의 fetch 함수를 만들거나 직접 로직을 구현합니다.
        cursor = None
        try:
            cursor = conn.cursor(buffered=True)
            cursor.execute(query_select)
            result = cursor.fetchone()
            
            if result:
                flag_success = True
                sequence = result[0]
            else:
                # 이 경우는 거의 발생하지 않아야 합니다 (위에서 INSERT/UPDATE 했으므로).
                flag_success = False
                sequence = 0

        finally:
            if cursor:
                cursor.close()

    except Exception as e:
        logger.error(f"DB 오류 (in transaction): {e}")
        flag_success = False
        sequence = 0
        raise # 트랜잭션 롤백을 위해 예외를 다시 발생시킴
    return flag_success, sequence
####################################################################################################################





####################################################################################################################
# intermediate_wholesaler_credit_log 삽입 (Transaction)
# flag_division
# 1: 약정한도 홀드
# 2: 약정한도 사용
# 3: 약정한도 홀드취소
# 4: 약정한도 사용취소
####################################################################################################################
def intermediate_wholesaler_credit_log_insert_with_conn(conn, jumehuga: str, flag_division: int, amount: Decimal):
    try:
        query = (
            """
            INSERT INTO intermediate_wholesaler_credit_log (
                log_date,
                log_time,
                jumehuga,
                flag_division,
                amount
            )
            VALUES (
                CURDATE(),
                CURTIME(),
                %s,
                %s,
                %s
            )
            """
        )
        params = (jumehuga, flag_division, amount)
        db.execute_query_with_conn(conn, query, params)

    except Exception as e:
        logger.error(f"로그 삽입 오류 (in transaction): {e}")
        raise # 예외를 다시 발생시켜 트랜잭션이 롤백되도록 함
####################################################################################################################





####################################################################################################################
# intermediate_wholesaler_credit_api_log 삽입
# flag_division
# 0: 약정한도 조회
# 1: 약정한도 홀드
# 2: 약정한도 사용
# 3: 약정한도 홀드취소
# 4: 약정한도 사용취소
####################################################################################################################
class intermediate_wholesaler_credit_api_log_insert_data(BaseModel):
    result: str
    jumehuga: str
    flag_division: int
    amount: Decimal
    request_content: str
    response_content: str
####################################################################################################################
def intermediate_wholesaler_credit_api_log_insert(r: intermediate_wholesaler_credit_api_log_insert_data):
    flag_success = False
    ####################################################################################################################
    # result 값이 String 이므로
    ####################################################################################################################
    if r.result == "true":
        flag_success = 1
    else:
        flag_success = 0
    ####################################################################################################################

    try:
        query = (
            """
            INSERT INTO intermediate_wholesaler_credit_api_log (
                log_date,
                log_time,
                flag_success,
                jumehuga,
                flag_division,
                amount,
                request_content,
                response_content
            )
            VALUES (
                CURDATE(),
                CURTIME(),
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """
        )
        params = (flag_success, r.jumehuga, r.flag_division, r.amount, r.request_content, r.response_content)
        result = db.execute_query(query, params)
        # result가 None이 아니고, 0보다 커야 (실제 업데이트가 발생해야) 성공으로 간주
        if result is not None and result > 0:
            flag_success = True
        else:
            flag_success = False

    except Exception as e:
        logger.error(f"로그 삽입 오류: {e}")
        flag_success = False
    return flag_success
####################################################################################################################





####################################################################################################################
# 도매법인 여신 약정한도 조회
# POST /WH_API_T01_01
####################################################################################################################
class WH_API_T01_01_Request(BaseModel):
    CNTC_TYPE_CD: str               # 연계유형코드
    SLER_BRNO: str
    WHMK_CD: str
    WHSL_CPR_CD: str                # 판매자 도매법인코드
    PRCHR_WHMK_CD: Optional[str] = None
    PRCHR_FIRM_NM: str
    PRCHR_PRMS_NO: Optional[str] = None
    PRCHR_BRNO: str
    ONLINE_PRCHR_PRMS_NO: str       # 온라인도매시장 구매자 허가번호
    PRCHR_LOGIN_ID: str
    TOT_TRNS_AMT: Decimal           # 총 거래 금액
    LOAN_DV_CD: str                 # 여신구분코드 (HOLD/USE)
####################################################################################################################
@app.post("/WH_API_T01_01", summary="도매법인 여신 약정한도 조회 V251104")
def wh_api_t01_01(r: WH_API_T01_01_Request):
    try:
        request_content = r.model_dump_json()

        if r.CNTC_TYPE_CD == "CHECK":
            if r.LOAN_DV_CD == "HOLD" or r.LOAN_DV_CD == "USE":
                flag_success, available_amount = intermediate_wholesaler_credit_search(r.PRCHR_PRMS_NO)
                if flag_success:
                    if available_amount - r.TOT_TRNS_AMT >= 0:
                        RESULT = "true"
                        MESSAGE = "성공"
                        PRCS_STAT_CD = "0"  # 정상
                    else:
                        RESULT = "false"
                        MESSAGE = "실패"                    
                        PRCS_STAT_CD = "1"  # 거절
                else:
                    RESULT = "false"
                    MESSAGE = "중도매인 존재하지 않음"
                    PRCS_STAT_CD = "2"  # 중도매인 존재하지 않음
            else:
                RESULT = "false"
                MESSAGE = "LOAN_DV_CD 오류"
                PRCS_STAT_CD = "2"  # LOAN_DV_CD 오류
        else:
            RESULT = "false"
            MESSAGE = "CNTC_TYPE_CD 오류"
            PRCS_STAT_CD = "2"  # CNTC_TYPE_CD 오류

    except Exception as e:
        logger.error(f"DB 오류: {e}")
        RESULT = "false"
        MESSAGE = "DB 오류"
        PRCS_STAT_CD = "2"

    finally:
        ##################################################################################################################
        # intermediate_wholesaler_credit_api_log Table Insert
        ##################################################################################################################        
        response_content = {"RESULT": RESULT, "MESSAGE": MESSAGE, "DATA": {"PRCS_STAT_CD": PRCS_STAT_CD}}
        log_data = intermediate_wholesaler_credit_api_log_insert_data(
            result = RESULT,
            flag_division = 0,                          # 구분 (0:조회, 1:홀드, 2:사용, 3:홀드취소, 4:사용취소, 9:기타)
            jumehuga = r.PRCHR_PRMS_NO,
            amount = r.TOT_TRNS_AMT, # Decimal 타입 그대로 전달
            request_content = request_content,
            response_content = json.dumps(response_content) # JSON 문자열로 변환
        )
        intermediate_wholesaler_credit_api_log_insert(log_data)
        ##################################################################################################################

        return response_content
####################################################################################################################





####################################################################################################################
# 도매법인 여신 사용(홀드) 요청
# POST /WH_API_T01_02
####################################################################################################################
class WH_API_T01_02_Trns_Item(BaseModel):
    ORDR_NO: Optional[str] = None
    TRNS_DETL_ID: str
    PRDCT_DETL_ID: str
    PRDCT_NM: str
    LCLS_CD: str
    LCLS_NM: str
    MCLS_CD: str
    MCLS_NM: str
    SCLS_CD: str
    SCLS_NM: str
    TRNS_UNIT_CD: str
    TRNS_UNIT_NM: str
    FRPRD_PACKN_CD: Optional[str] = None
    FRPRD_PACKN_NM: Optional[str] = None
    FRPRD_SIZE_CD: Optional[str] = None
    FRPRD_SIZE_NM: Optional[str] = None
    FRPRD_QLT_CD: Optional[str] = None
    FRPRD_QLT_NM: Optional[str] = None
    ORPLC_SIGNGU_CD: str
    ORPLC_EMD_CD: Optional[str] = None
    FIRM_PRDCT_NO: Optional[str] = None
    TRNS_PRUT: Decimal
    ORDR_UNPRC: Decimal
    ORDR_QNTT: int
    ORDR_AMT: Decimal           # 주문금액
    TRNS_DTM: str
    FRPRD_MAKER_NM: Optional[str] = None
    FRPRD_MAKER_NO: Optional[str] = None
    FWD_CMPNM: Optional[str] = None
    FWD_NM: Optional[str] = None
    FWD_DCLR_NO: Optional[str] = None
    FWD_TELNO: Optional[str] = None
    DPTRP_ZIP: str
    DPTRP_ADDR: Optional[str] = None
    DPTRP_LNNO_ADDR: Optional[str] = None
    DPTRP_DTADD: Optional[str] = None
    DTNT_ZIP: str
    DTNT_ADDR: Optional[str] = None
    DTNT_LNNO_ADDR: Optional[str] = None
    DTNT_DTADD: Optional[str] = None
    SHPMN_CNN_MNNMB: Optional[str] = None
    FWD_CNN_MNNMB: Optional[str] = None
    PRDCT_RGTR_ID: str
    AUCTER_NM: Optional[str] = None

class WH_API_T01_02_Request(BaseModel):
    CNTC_TYPE_CD: str                   # 연계유형코드
    SLER_BRNO: str
    WHMK_CD: str
    WHSL_CPR_CD: str                    # 판매자 도매법인코드
    TRNS_ID: str
    PRCHR_WHMK_CD: Optional[str] = None
    PRCHR_FIRM_NM: str
    PRCHR_PRMS_NO: Optional[str] = None
    PRCHR_BRNO: str
    ONLINE_PRCHR_PRMS_NO: str           # 온라인도매시장 구매자 허가번호
    PRCHR_LOGIN_ID: str
    TRNS_WAY_CD: str
    TOT_TRNS_AMT: Decimal               # 총 거래 금액
    TRNS_ITEM_CNT: Optional[int]
    TRNS_ITEM: List[WH_API_T01_02_Trns_Item]
####################################################################################################################
@app.post("/WH_API_T01_02", summary="도매법인 여신 사용(홀드) 요청 V251105")
def wh_api_t01_02(r: WH_API_T01_02_Request): # noqa: C901
    ####################################################################################################################
    # finally 블록에서 사용될 변수들을 미리 초기화합니다.
    RESULT, MESSAGE, PRCS_STAT_CD = "false", "", "2"
    TRNS_ITEM = []
    request_content = ""
    ####################################################################################################################

    try:
        request_content = r.model_dump_json()

        if r.CNTC_TYPE_CD == "HOLD":
            RESULT = "true"
            MESSAGE = "성공"
            PRCS_STAT_CD = "0"  # 정상

            # 1. 중도매인 존재 여부 및 약정한도 최초 조회 (반복문 밖에서 1회만)
            flag_success, current_available_amount = intermediate_wholesaler_credit_search(r.PRCHR_PRMS_NO)

            if not flag_success:
                RESULT, MESSAGE, PRCS_STAT_CD = "false", "중도매인 존재하지 않음", "2"
            elif current_available_amount < r.TOT_TRNS_AMT: # 총액으로 우선 검사
                RESULT, MESSAGE, PRCS_STAT_CD = "false", "한도 초과", "1"
            else:
                BUND_EGM_ID_BASE_CODE = datetime.now().strftime("%y%m%d") + r.WHSL_CPR_CD

                # 2. TRNS_ITEM 리스트를 돌면서 BUND_EGM_ID 생성 및 처리
                for item in r.TRNS_ITEM:
                    conn = None
                    try:
                        # 2-1. 메모리에서 한도 체크 (Decimal 연산)
                        if current_available_amount - item.ORDR_AMT < 0:
                            raise ValueError("한도 초과")

                        # 2-2. 트랜잭션 시작
                        conn = db.start_transaction()

                        # 2-3. 약정한도ID (BUND_EGM_ID) 생성
                        flag_seq_ok, sequence = daily_sequence_search_with_conn(conn) # 트랜잭션에 포함
                        if not flag_seq_ok:
                            raise Exception("sequence 조회 실패")
                        
                        BUND_EGM_ID = f"{BUND_EGM_ID_BASE_CODE}{sequence:06d}"

                        # 2-4. intermediate_wholesaler_credit_order Table INSERT (트랜잭션)
                        query_order = "INSERT INTO intermediate_wholesaler_credit_order (idx, trade_date, trade_time, jumehuga, flag_division, TRNS_DETL_ID, PRDCT_DETL_ID, ORDR_AMT) VALUES (%s, CURDATE(), CURTIME(), %s, 1, %s, %s, %s)"
                        params_order = (BUND_EGM_ID, r.PRCHR_PRMS_NO, item.TRNS_DETL_ID, item.PRDCT_DETL_ID, item.ORDR_AMT)
                        db.execute_query_with_conn(conn, query_order, params_order)

                        # 2-5. intermediate_wholesaler_credit Table UPDATE (트랜잭션)
                        intermediate_wholesaler_credit_update_with_conn(conn, 1, r.PRCHR_PRMS_NO, item.ORDR_AMT)

                        # 2-6. intermediate_wholesaler_credit_log Table INSERT (트랜잭션)
                        intermediate_wholesaler_credit_log_insert_with_conn(conn, r.PRCHR_PRMS_NO, 1, item.ORDR_AMT)

                        # 2-7. 모든 DB 작업 성공 시 커밋
                        db.commit_transaction(conn)

                        # 2-8. 성공적으로 처리된 항목을 응답 목록에 추가
                        TRNS_ITEM.append({
                            "BUND_EGM_ID": BUND_EGM_ID,
                            "TRNS_DETL_ID": item.TRNS_DETL_ID,
                            "PRDCT_DETL_ID": item.PRDCT_DETL_ID,
                            "ORDR_AMT": item.ORDR_AMT
                        })

                        # 2-9. 메모리의 사용 가능 한도 차감
                        current_available_amount -= item.ORDR_AMT

                    except ValueError as ve: # 한도 초과 예외 처리
                        db.rollback_transaction(conn)
                        RESULT, MESSAGE, PRCS_STAT_CD = "false", str(ve), "1"
                        break # 한도 초과 시 반복 중단
                    except Exception as e: # DB 및 기타 예외 처리
                        db.rollback_transaction(conn)
                        logger.error(f"Transaction failed for item {item.TRNS_DETL_ID}: {e}")
                        RESULT, MESSAGE, PRCS_STAT_CD = "false", "내부 처리 실패", "2"
                        break # 실패 시 반복 중단

                # for 루프가 break 없이 모두 완료되었을 때만 최종 성공 처리
                if PRCS_STAT_CD == "0":
                    RESULT, MESSAGE = "true", "성공"

        else:
            RESULT = "false"
            MESSAGE = "CNTC_TYPE_CD 오류"
            PRCS_STAT_CD = "2"  # CNTC_TYPE_CD 오류

    except Exception as e:
        logger.error(f"API Error: {e}")
        RESULT = "false"
        MESSAGE = "API 오류"
        PRCS_STAT_CD = "2"

    finally:
        ##################################################################################################################
        # intermediate_wholesaler_credit_api_log Table Insert
        ##################################################################################################################
        response_content = {
            "RESULT": RESULT,
            "MESSAGE": MESSAGE,
            "DATA": {
                "PRCS_STAT_CD": PRCS_STAT_CD,
                "TRNS_ID": r.TRNS_ID,
                "TRNS_WAY_CD": r.TRNS_WAY_CD if hasattr(r, 'TRNS_WAY_CD') else "",
                "TRNS_ITEM": TRNS_ITEM
            }
        }
        log_data = intermediate_wholesaler_credit_api_log_insert_data(
            result = RESULT,
            flag_division = 1,                        # 구분 (0:조회, 1:홀드, 2:사용, 3:홀드취소, 4:사용취소, 9:기타)
            jumehuga = r.PRCHR_PRMS_NO if hasattr(r, 'PRCHR_PRMS_NO') else "",
            amount = r.TOT_TRNS_AMT,
            request_content = request_content,
            response_content = json.dumps(response_content, default=str) # Decimal을 위해 default=str 추가
        )
        intermediate_wholesaler_credit_api_log_insert(log_data)
        ##################################################################################################################
        
        return response_content
####################################################################################################################





####################################################################################################################
# 도매법인 여신 사용(차감) 요청
# POST /WH_API_T01_03
####################################################################################################################
class WH_API_T01_03_Trns_Item(BaseModel):
    ORDR_NO: Optional[str] = None
    ORDR_UNPRC: Decimal
    ORDR_QNTT: int
    ORDR_AMT: Decimal
    RTGDS_QNTT: Optional[int] = None
    RTGDS_DTM: Optional[str] = None
    RTGDS_AMT: Optional[Decimal] = None
    PURC_CFMTN_QNTT: Optional[int] = None
    PURC_CFMTN_DTM: Optional[str] = None
    PURC_CFMTN_AMT: Optional[Decimal] = None
    BUND_EGM_ID: str        # 약정한도 아이디

class WH_API_T01_03_Request(BaseModel):
    CNTC_TYPE_CD: str
    TRNS_ITEM_CNT: int
    TRNS_ITEM: List[WH_API_T01_03_Trns_Item]
####################################################################################################################
@app.post("/WH_API_T01_03", summary="도매법인 여신 사용(차감) 요청 V251105")
def wh_api_t01_03(r: WH_API_T01_03_Request):
    RESULT, MESSAGE, PRCS_STAT_CD = "false", "", "2"
    TRNS_ITEM = []
    jumehuga = ""
    total_amount = Decimal('0.0')
    request_content = ""
    conn = None

    try:
        request_content = r.model_dump_json()

        if r.CNTC_TYPE_CD == "USE":
            conn = db.start_transaction()
            if not conn:
                raise Exception("데이터베이스 트랜잭션을 시작할 수 없습니다.")

            # 모든 아이템이 성공해야 커밋되므로, 미리 성공으로 가정
            RESULT, MESSAGE, PRCS_STAT_CD = "true", "성공", "0"

            for item in r.TRNS_ITEM:
                flag_success, temp_jumehuga, order_amount = intermediate_wholesaler_credit_order_search(item.BUND_EGM_ID, 1)
                jumehuga = temp_jumehuga if temp_jumehuga else jumehuga
                total_amount += item.ORDR_AMT

                if not flag_success:
                    RESULT, MESSAGE, PRCS_STAT_CD = "false", f"주문({item.BUND_EGM_ID})을 찾을 수 없습니다.", "2"
                    break

                # 1. intermediate_wholesaler_credit_order Table flag_division = 2 로 UPDATE (트랜잭션)
                intermediate_wholesaler_credit_order_update_with_conn(conn, item.BUND_EGM_ID, 2)

                # 2. intermediate_wholesaler_credit Table 업데이트 (트랜잭션)
                updated_rows = intermediate_wholesaler_credit_update_with_conn(conn, 2, jumehuga, item.ORDR_AMT)

                if updated_rows == 0:
                    RESULT, MESSAGE, PRCS_STAT_CD = "false", f"주문({item.BUND_EGM_ID})의 여신 사용 처리에 실패했습니다.", "2"
                    break

                # 3. intermediate_wholesaler_credit_log Table 삽입 (트랜잭션)
                intermediate_wholesaler_credit_log_insert_with_conn(conn, jumehuga, 2, item.ORDR_AMT)

                # 성공한 아이템 정보 추가
                TRNS_ITEM.append({
                    "PRCS_STAT_CD": "0",
                    "BUND_EGM_ID": item.BUND_EGM_ID
                })

            if RESULT == "true":
                db.commit_transaction(conn)
                conn = None # 커밋 후에는 conn을 None으로 설정하여 finally에서 롤백되지 않도록 함
            else:
                # 실패 시, 성공했던 아이템의 상태도 실패로 변경
                for res_item in TRNS_ITEM:
                    res_item["PRCS_STAT_CD"] = "2"
                # 실패한 아이템 정보도 추가
                if not any(d.get('BUND_EGM_ID') == item.BUND_EGM_ID for d in TRNS_ITEM):
                    TRNS_ITEM.append({
                        "PRCS_STAT_CD": "2",
                        "BUND_EGM_ID": item.BUND_EGM_ID
                    })

        else:
            RESULT, MESSAGE, PRCS_STAT_CD = "false", "CNTC_TYPE_CD 오류", "2"

    except Exception as e:
        logger.error(f"API 오류: {e}")
        RESULT, MESSAGE, PRCS_STAT_CD = "false", "API 처리 중 오류가 발생했습니다.", "2"

    finally:
        if conn: # 트랜잭션이 아직 끝나지 않았다면 (오류 발생 시) 롤백
            db.rollback_transaction(conn)

        response_content = {
            "RESULT": RESULT,
            "MESSAGE": MESSAGE,
            "DATA": {
                "PRCS_STAT_CD": PRCS_STAT_CD,
                "TRNS_ITEM": TRNS_ITEM
            }
        }
        log_data = intermediate_wholesaler_credit_api_log_insert_data(
            result = RESULT,
            flag_division = 2,
            jumehuga = jumehuga,
            amount = total_amount,
            request_content = request_content,
            response_content = json.dumps(response_content, default=str)
        )
        intermediate_wholesaler_credit_api_log_insert(log_data)

        return response_content
####################################################################################################################





####################################################################################################################
# 도매법인 약정한도 사용취소 요청
# POST /WH_API_T01_04
####################################################################################################################
class WH_API_T01_04_Trns_Item(BaseModel):
    BUND_EGM_ID: str        # 약정한도 아이디

class WH_API_T01_04_Request(BaseModel):
    CNTC_TYPE_CD: str
    LOAN_USE_CD: str
    TRNS_ITEM: List[WH_API_T01_04_Trns_Item]
####################################################################################################################
@app.post("/WH_API_T01_04", summary="도매법인 약정한도 사용취소 요청 V251106")
def wh_api_t01_04(r: WH_API_T01_04_Request):
    # finally 블록에서 사용될 변수들을 미리 초기화합니다.
    RESULT, MESSAGE, PRCS_STAT_CD = "true", "성공", "0"
    TRNS_ITEM = []
    jumehuga = ""
    total_amount = Decimal('0.0')
    request_content = ""
    flag_division_current = 0
    flag_division_next = 0

    try:
        request_content = r.model_dump_json()

        if r.CNTC_TYPE_CD == "CANCEL":
            # LOAN_USE_CD에 따라 쿼리와 flag_division_current & flag_division_next 결정
            if r.LOAN_USE_CD == "01":
                # 홀드
                flag_division_current = 1
                # 홀드 취소
                flag_division_next = 3
            elif r.LOAN_USE_CD == "02":
                # 사용
                flag_division_current = 2
                # 사용 취소
                flag_division_next = 4
            else:
                RESULT, MESSAGE = "false", "LOAN_USE_CD 오류"
                # 모든 아이템을 실패 처리
                for item in r.TRNS_ITEM:
                    TRNS_ITEM.append({"PRCS_STAT_CD": "2", "BUND_EGM_ID": item.BUND_EGM_ID, "MESSAGE": "LOAN_USE_CD 오류"})
                raise ValueError(MESSAGE)

            # 각 아이템을 순회하며 처리
            for item in r.TRNS_ITEM:
                conn = None
                item_prcs_stat_cd = "2" # 기본 실패
                item_message = ""

                try:
                    # 1. 주문 정보 조회
                    flag_success, temp_jumehuga, order_amount = intermediate_wholesaler_credit_order_search(item.BUND_EGM_ID, flag_division_current)
                    if not flag_success:
                        raise ValueError(f"주문({item.BUND_EGM_ID})을 찾을 수 없습니다.")
                    
                    jumehuga = temp_jumehuga
                    total_amount += order_amount

                    # 2. 트랜잭션 시작
                    conn = db.start_transaction()
                    if not conn:
                        raise Exception("데이터베이스 트랜잭션을 시작할 수 없습니다.")

                    # 3. 중도매인 존재 여부 확인 (트랜잭션)
                    check_query = "SELECT 1 FROM intermediate_wholesaler_credit WHERE jumehuga = %s"
                    check_params = (jumehuga,)
                    wholesaler_exists = db.execute_and_fetch_one_with_conn(conn, check_query, check_params)
                    if not wholesaler_exists:
                        raise ValueError(f"존재하지 않는 중도매인({jumehuga})입니다.")

                    # 4. intermediate_wholesaler_credit_order Table flag_division = 3 또는 4 로 UPDATE (트랜잭션)
                    intermediate_wholesaler_credit_order_update_with_conn(conn, item.BUND_EGM_ID, flag_division_next)

                    # 5. intermediate_wholesaler_credit Table 업데이트 (트랜잭션)
                    intermediate_wholesaler_credit_update_with_conn(conn, flag_division_next, jumehuga, order_amount)

                    # 6. intermediate_wholesaler_credit_log Table 삽입 (트랜잭션)
                    intermediate_wholesaler_credit_log_insert_with_conn(conn, jumehuga, flag_division_next, order_amount)

                    # 7. 모든 DB 작업 성공 시 커밋
                    db.commit_transaction(conn)

                    # 커밋 후에는 conn을 None으로 설정
                    conn = None 

                    item_prcs_stat_cd = "0" # 성공
                    item_message = "성공"

                except (ValueError, Exception) as e:
                    if conn:
                        db.rollback_transaction(conn)
                    logger.error(f"Item processing failed for {item.BUND_EGM_ID}: {e}")
                    item_message = str(e)
                    RESULT, MESSAGE = "false", "일부 항목 처리 실패" # 전체 결과는 실패로 설정

                finally:
                    TRNS_ITEM.append({
                        "PRCS_STAT_CD": item_prcs_stat_cd,
                        "BUND_EGM_ID": item.BUND_EGM_ID,
                        "MESSAGE": item_message
                    })
        else:
            RESULT, MESSAGE = "false", "CNTC_TYPE_CD 오류"

    except Exception as e:
        logger.error(f"API 오류: {e}")
        RESULT, MESSAGE = "false", "API 처리 중 오류가 발생했습니다."

    finally:
        response_content = {
            "RESULT": RESULT,
            "MESSAGE": MESSAGE,
            "DATA": {
                "TRNS_ITEM": TRNS_ITEM
            }
        }
        log_data = intermediate_wholesaler_credit_api_log_insert_data(
            result = RESULT,
            flag_division = flag_division_next if flag_division_next != 0 else 9,
            jumehuga = jumehuga,
            amount = total_amount,
            request_content = request_content,
            response_content = json.dumps(response_content, default=str)
        )
        intermediate_wholesaler_credit_api_log_insert(log_data)

        return response_content
####################################################################################################################