from beanie import Document

class Site(Document):
    name: str
    fqdn: str