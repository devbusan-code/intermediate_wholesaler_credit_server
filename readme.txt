- intermediate_wholesaler_credit_server

nohup uv run uvicorn main:app --host 0.0.0.0 --port 8081 > /dev/null 2>&1 &

ps -aux | grep python

curl http://localhost:8081/docs

curl http://localhost:8081/redoc


uv run fastapi dev


# .env
MYSQL_HOST=
MYSQL_PORT=3336
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DATABASE=



@app.middleware("http")
async def ip_allow_middleware(request: Request, call_next):
    client_ip = ipaddress.ip_address(request.client.host)
    if not any(client_ip in net for net in allowed_networks):
        return JSONResponse({"detail": "Forbidden"}, status_code=403)
    return await call_next(request)