from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    GATEWAY_HOST: str = "127.0.0.1"
    GATEWAY_PORT: int = 8000
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_POOL_SIZE: int = 200
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_HOST: str = "localhost"
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = "rootpassword"
    MYSQL_DATABASE: str = "flashsale"

    QUEUE_NAME: str = "checkout_queue"
    DLX_NAME: str = "checkout_dlx"
    DLQ_NAME: str = "checkout_dlq"
    WORKER_PREFETCH_COUNT: int = 20

    DEFAULT_PRODUCT_ID: int = 1
    DEFAULT_INITIAL_STOCK: int = 500
    RATE_LIMIT_MAX_REQUESTS: int = 5
    RATE_LIMIT_WINDOW_SECONDS: int = 10
    RESERVATION_TTL_SECONDS: int = 15
    REAPER_INTERVAL_SECONDS: int = 2

    LOADTEST_LEGITIMATE_USERS: int = 2500
    LOADTEST_BOT_REQUESTS: int = 200
    LOADTEST_CONCURRENCY: int = 150
    SIMULATED_PAYMENT_RATIO: float = 0.90

    @property
    def redis_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}"

    @property
    def gateway_url(self) -> str:
        return f"http://{self.GATEWAY_HOST}:{self.GATEWAY_PORT}"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()