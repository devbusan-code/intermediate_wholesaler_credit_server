- intermediate_wholesaler_credit_server

nohup uv run uvicorn main:app --host 0.0.0.0 --port 8081 > /dev/null 2>&1 &

ps -aux | grep python

curl http://localhost:8081/docs

curl http://localhost:8081/redoc