# Multi-stage build для оптимизации размера образа
FROM python:3.9-slim as builder

# Установка переменных окружения
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Установка системных зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Установка рабочей директории
WORKDIR /app

# Копирование файлов зависимостей
COPY requirements.txt ./

# Установка Python зависимостей
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.9-slim

# Переменные окружения
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Установка runtime зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get autoremove -y \
    && apt-get clean

# Создание непривилегированного пользователя (с домашним каталогом:
# без -m playwright не может писать в /home/appuser и падает на EACCES)
RUN groupadd -r appuser && useradd -r -m -d /home/appuser -g appuser appuser

# Установка рабочей директории
WORKDIR /app

# Копирование Python зависимостей из builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Браузер для Домклика (Qrator отдаёт 401 без выполнения JS-челленджа).
# --with-deps сам ставит системные библиотеки Chromium; общий путь
# /ms-playwright читается юзером appuser (a+rX), поэтому установка
# живёт В ОБРАЗЕ и переживает пересоздание контейнера.
RUN PLAYWRIGHT_BROWSERS_PATH=/ms-playwright patchright install --with-deps chromium \
    && chmod -R a+rX /ms-playwright

# Копирование кода приложения
COPY --chown=appuser:appuser . .

# Создание необходимых директорий
RUN mkdir -p /app/logs /app/data && \
    chown -R appuser:appuser /app

# Переключение на непривилегированного пользователя
USER appuser

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose порт
EXPOSE 8000

# Запуск приложения
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
