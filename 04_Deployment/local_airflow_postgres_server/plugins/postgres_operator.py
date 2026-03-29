from airflow.sdk.bases.operator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # <- updated import

class MyPostgresOperator(BaseOperator):
    def __init__(
        self,
        sql: str,
        postgres_conn_id: str = "postgres_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        # Use PostgresHook from the provider package (Airflow 2.x)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        hook.run(self.sql)

