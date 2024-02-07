from typing import List

import strawberry
from fastapi import FastAPI
from functools import partial
from databases import Database
from strawberry.types import Info
from contextlib import asynccontextmanager
from strawberry.fastapi import BaseContext, GraphQLRouter

from settings import Settings


class Context(BaseContext):
    db: Database

    def __init__(
        self,
        db: Database,
    ) -> None:
        self.db = db


@strawberry.type
class Author:
    name: str


@strawberry.type
class Book:
    title: str
    author: Author


async def get_author_by_id(db, author_id: int) -> Author | None:
    query = "SELECT id, name FROM authors WHERE id = :author_id"
    values = {"author_id": author_id}
    row = await db.fetch_one(query, values=values)
    if row:
        # Создаем и возвращаем экземпляр Author на основе полученных данных
        return Author(name=row['name'])
    else:
        # Возвращаем None или выбрасываем исключение, если автор не найден
        return None


@strawberry.type
class Query:

    @strawberry.field
    async def books(
        self,
        info: Info,
        author_ids: List[int] | None = None,
        search: str | None = None,
        limit: int | None = None,
    ) -> List[Book]:
        """
        Получение списка книг
        :param info: Контекст запроса
        :param author_ids: ID авторов
        :param search: Поисковая строка
        :param limit: Ограничение на количество возвращаемых книг
        :return: Список книг
        """

        query_conditions = []
        values = {}

        if author_ids:
            query_conditions.append("author_id = ANY(:author_ids)")
            values["author_ids"] = author_ids

        if search:
            query_conditions.append("title ILIKE :search")
            values["search"] = f"%{search}%"

        query = "SELECT title, author_id FROM books"
        if query_conditions:
            query += " WHERE " + " AND ".join(query_conditions)

        if limit:
            query += " LIMIT :limit"
            values["limit"] = limit

        try:
            rows = await info.context.db.fetch_all(query, values=values)
        except Exception as e:
            raise e

        books = []
        for row in rows:
            author = await get_author_by_id(info.context.db, row['author_id'])
            book = Book(title=row['title'], author=author)
            books.append(book)
        return books


CONN_TEMPLATE = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}"
settings = Settings()  # type: ignore
db = Database(
    CONN_TEMPLATE.format(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        port=settings.DB_PORT,
        host=settings.DB_SERVER,
        name=settings.DB_NAME,
    ),
)

@asynccontextmanager
async def lifespan(
    app: FastAPI,
    db: Database,
):
    async with db:
        yield

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(  # type: ignore
    schema,
    context_getter=partial(Context, db),
)

app = FastAPI(lifespan=partial(lifespan, db=db))
app.include_router(graphql_app, prefix="/graphql")
