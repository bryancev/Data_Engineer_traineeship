from sqlalchemy import Column, Text, Integer, Date
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class VacanciesTable(Base):
    __tablename__ = "zolotoy_raw_products"

    sku = Column(Integer, primary_key=True)
    category = Column(Text)
    subcategory = Column(Text)
    name = Column(Text)
    price = Column(Text)
    old_price = Column(Text)
    discount = Column(Text)
    rating = Column(Text)
    reviews = Column(Text)
    parsed_date = Column(Date)
    product_url = Column(Text)
