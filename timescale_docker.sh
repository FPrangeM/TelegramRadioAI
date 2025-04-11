docker run -d --name timescaledb \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=123 \
  -e POSTGRES_DB=radio_db \
  timescale/timescaledb:latest-pg16