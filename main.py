from typing import Dict, List, Tuple

from sqlalchemy.engine import create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.schema import ForeignKey
from strawberry import Schema, field, input, type
from strawberry.dataloader import DataLoader


################################### Database Layer #####################################


class CustomMappedAsDataclass(MappedAsDataclass, kw_only=True):
    pass


class Base(DeclarativeBase, CustomMappedAsDataclass):
    pass


class BookModel(Base):
    __tablename__ = "book"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    author_id: Mapped[int] = mapped_column(ForeignKey("author.id"), init=False)
    title: Mapped[str] = mapped_column(init=True)

    author: Mapped["AuthorModel"] = relationship("AuthorModel", init=True)


class AuthorModel(Base):
    __tablename__ = "author"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    name: Mapped[str] = mapped_column(init=True)


engine = create_engine("sqlite://")
Session = sessionmaker(engine)
Base.metadata.create_all(engine)

db_authors = [AuthorModel(name=f"Author {i + 1}") for i in range(2)]
db_books = [
    BookModel(title=f"Book {i + 1}", author=db_authors[i // 5]) for i in range(10)
]

with Session() as session:
    session.add_all(db_books)
    session.commit()

########################################################################################

##################################### Schema Layer #####################################


@type
class Book:
    id: int
    title: str


@input(frozen=True)
class BookFilter:
    title: str


def load_author_books_batch(
    where: BookFilter | None, ids: List[int]
) -> Dict[int, List[BookModel]]:
    statement = select(BookModel).where(BookModel.id.in_(ids))
    if where:
        statement = statement.where(BookModel.title.ilike(f"%{where.title}%"))
    with Session() as session:
        db_books = session.execute(statement).scalars().all()

    author_books_map: Dict[int, List[BookModel]] = {}
    for book in db_books:
        author_books = author_books_map.get(book.author_id, None)
        if author_books:
            author_books.append(book)
        else:
            author_books_map[book.author_id] = [book]

    return author_books_map


async def load_author_books(
    keys: List[Tuple[BookFilter | None, int]]
) -> List[List[Book]]:
    where_ids_map: Dict[BookFilter | None, List[int]] = {}
    for where, id in keys:
        ids = where_ids_map.get(where, None)
        if ids:
            ids.append(id)
        else:
            where_ids_map[where] = [id]

    where_author_books_map_map = {
        where: load_author_books_batch(where=where, ids=ids)
        for where, ids in where_ids_map.items()
    }

    db_book_batches = [
        where_author_books_map_map[where].get(id, []) for where, id in keys
    ]
    return [[Book(id=b.id, title=b.title) for b in batch] for batch in db_book_batches]


author_book_loader = DataLoader(load_fn=load_author_books)


@type
class Author:
    id: int
    name: str

    @field
    async def books(self, where: BookFilter | None = None) -> List[Book]:
        return await author_book_loader.load((where, self.id))


@type
class Query:
    @field
    def authors() -> List[Author]:
        statement = select(AuthorModel)
        with Session() as session:
            db_authors = session.execute(statement).scalars().all()
        return [Author(id=a.id, name=a.name) for a in db_authors]


schema = Schema(query=Query)

########################################################################################
