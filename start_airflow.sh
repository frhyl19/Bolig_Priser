#!/bin/bash

echo "🧹 Stopping existing containers..."
docker compose down

echo "🧼 (Optional) Cleaning old state..."
docker compose down -v

echo "🚀 Starting Airflow..."
docker compose up -d

echo "⏳ Waiting for services..."
sleep 10

echo "📦 Showing running containers..."
docker ps

echo "✅ Airflow is starting..."
echo "👉 Open: http://localhost:8080"