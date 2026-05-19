"""Register Postgres connection and datasets for traffic viz."""
import os

from superset.app import create_app

PG_URI = os.environ.get(
    "SUPERSET_PG_URI",
    "postgresql+psycopg2://superset:superset@postgres:5432/traffic_viz",
)
DB_NAME = "Traffic Postgres"
TABLES = ("tomtom_predictions", "tomtom_preprocessed")


def main():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        database = (
            db.session.query(Database).filter_by(database_name=DB_NAME).one_or_none()
        )
        if not database:
            database = Database(
                database_name=DB_NAME,
                sqlalchemy_uri=PG_URI,
                expose_in_sqllab=True,
                allow_run_async=True,
            )
            db.session.add(database)
            db.session.commit()
            print(f"Created database: {DB_NAME}")
        else:
            print(f"Database exists: {DB_NAME}")

        for table_name in TABLES:
            exists = (
                db.session.query(SqlaTable)
                .filter_by(table_name=table_name, database_id=database.id)
                .one_or_none()
            )
            if exists:
                print(f"Dataset exists: {table_name}")
                continue
            dataset = SqlaTable(
                table_name=table_name,
                schema="public",
                database=database,
            )
            db.session.add(dataset)
            db.session.commit()
            dataset.fetch_metadata()
            print(f"Created dataset: {table_name}")


if __name__ == "__main__":
    main()
