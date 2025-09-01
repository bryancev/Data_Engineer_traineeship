from sqlalchemy import create_engine
from contextlib import contextmanager

from .models.raw_products_model import VacanciesTable, Base

from itemadapter import ItemAdapter

from sqlalchemy.orm import scoped_session, sessionmaker
import logging 
from sqlalchemy.dialects.postgresql import insert

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from environs import Env
from pathlib import Path

def configure_db():
    env = Env()
    project_dir = Path(__file__).resolve().parent.parent
    env_file = project_dir / ".env"
    
    env.read_env(env_file)

    return {
        "user": env.str("POSTGRES_USER"),
        "password": env.str("POSTGRES_PASSWORD"),
        "db": env.str("POSTGRES_DB"),
        "host": env.str("POSTGRES_HOST"),
        "port": env.str("POSTGRES_PORT")
    }

def get_connection_string():
    config = configure_db()
    return f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}"

class Zolotoy585Pipeline:
    def __init__(self):
        self.engine = create_engine(
            get_connection_string(),
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        self.session_factory = scoped_session(
            sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False
            )
        )

    @contextmanager
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            session.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        product_data = {
            "category": adapter.get("category"),
            "subcategory": adapter.get("subcategory"),
            "name": adapter.get("name"),
            "sku": adapter.get("sku"),
            "price": adapter.get("price"),
            "old_price": adapter.get("old_price"),
            "discount": adapter.get("discount"),
            "rating": adapter.get("rating"),
            "reviews": adapter.get("reviews"),
            "parsed_date": adapter.get("parsed_date"),
            "product_url": adapter.get("product_url")
        }

        try:
            with self.session_scope() as session:
                stmt = insert(VacanciesTable).values(**product_data)
                stmt = stmt.on_conflict_do_nothing(index_elements=['sku'])  # Skip duplicates based on SKU
                session.execute(stmt)
                logger.debug(f"Processed product: {product_data['name']}")
        except Exception as e:
            logger.error(f"Error saving product {product_data.get('name')}: {e}")
            raise

        return item

    def close_spider(self, spider):
        self.session_factory.remove()
        self.engine.dispose()
        logger.info("Database connection closed")