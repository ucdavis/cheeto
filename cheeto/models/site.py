import pymongo
from pymongo import IndexModel

from .base import BaseDocument


class Site(BaseDocument):
    name: str
    fqdn: str

    class Settings:
        name = 'sites'
        indexes = [
            IndexModel([('name', pymongo.ASCENDING)], unique=True),
        ]
