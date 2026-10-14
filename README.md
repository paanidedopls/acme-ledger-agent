# Acme Appliances Ledger Agent

FastAPI service for the GA1 ledger-agent task. It loads the personalised source API at startup, caches its data, keeps only the latest `updated_at` row for each order ID, interprets order timestamps in `Asia/Kolkata`, and converts currency values to USD using the API's rates.

## Deploy on Render

- Runtime: Python
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Add environment variable `SOURCE_ROOT_URL` using the full personalized root URL from the assignment, including `questionId=q-ledger-agent-server&path=%2F`.
- Optional environment variable: `CACHE_TTL_SECONDS=1800`

Do not commit the personalized source URL to a public repository. Configure it in Render's Environment panel.

## API

`POST /` or `POST /ask`

Request:
```json
{"question":"What was the total revenue in USD from the North region in March 2026?"}
```

Response:
```json
{"answer": 123.45}
```

`GET /` and `GET /health` report service status.
