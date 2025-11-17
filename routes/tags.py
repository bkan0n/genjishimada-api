from asyncpg import Connection
from genjipk_sdk.tags import (
    TagRowDTO,
    TagsAutocompleteRequest,
    TagsAutocompleteResponse,
    TagsMutateRequest,
    TagsMutateResponse,
    TagsMutateResult,
    TagsSearchFilters,
    TagsSearchResponse,
)
from litestar import Controller, post


class TagsController(Controller):
    path = "/tags"

    @post(path="/search")
    async def search(self, conn: Connection, data: TagsSearchFilters) -> TagsSearchResponse:  # noqa: PLR0912
        """Search tags."""
        sort_col_map = {
            "name": "tag_lookup.name",
            "uses": "tags.uses",
            "created_at": "tag_lookup.created_at",
        }
        sort_dir = "ASC" if data.sort_dir.lower() == "asc" else "DESC"
        order_by_sql = sort_col_map[data.sort_by]

        params: list = [data.guild_id]
        idx = 2

        select_cols = [
            "tags.id",
            "tags.location_id AS guild_id",
            "tag_lookup.name",
            "tag_lookup.owner_id",
            "tags.uses",
            "LOWER(tag_lookup.name) <> LOWER(tags.name) AS is_alias",
        ]
        if data.include_content:
            select_cols.append("tags.content")
        if data.include_rank:
            select_cols.append(
                "(SELECT COUNT(*) FROM tags t2 "
                " WHERE (t2.uses, t2.id) >= (tags.uses, tags.id) "
                "   AND t2.location_id = tags.location_id) AS rank"
            )

        sql = [
            f"SELECT {', '.join(select_cols)}",
            "FROM tag_lookup",
            "INNER JOIN tags ON tag_lookup.tag_id = tags.id",
            "WHERE tag_lookup.location_id = $1",
        ]

        if data.only_aliases:
            sql.append("AND LOWER(tag_lookup.name) <> LOWER(tags.name)")
        elif not data.include_aliases:
            sql.append("AND LOWER(tag_lookup.name) = LOWER(tags.name)")

        if data.by_id is not None:
            sql.append(f"AND tags.id = ${idx}")
            params.append(data.by_id)
            idx += 1
        elif data.name:
            if data.fuzzy:
                sql.append(f"AND tag_lookup.name % ${idx}")
            else:
                sql.append(f"AND LOWER(tag_lookup.name) = LOWER(${idx})")
            params.append(data.name)
            idx += 1

        if data.owner_id is not None:
            sql.append(f"AND tag_lookup.owner_id = ${idx}")
            params.append(data.owner_id)
            idx += 1

        if data.random:
            sql.append("ORDER BY random()")
        elif data.fuzzy and data.name:
            sql.append(f"ORDER BY similarity(tag_lookup.name, ${idx - 1}) DESC")
        else:
            sql.append(f"ORDER BY {order_by_sql} {sort_dir}")

        sql.append(f"LIMIT {int(data.limit)} OFFSET {int(data.offset)}")

        rows = await conn.fetch("\n".join(sql), *params)

        if not rows and data.name and not data.fuzzy:
            suggest_q = """
                SELECT tag_lookup.name
                FROM tag_lookup
                WHERE tag_lookup.location_id=$1 AND tag_lookup.name % $2
                ORDER BY similarity(tag_lookup.name, $2) DESC
                LIMIT 5
            """
            suggestions = [r["name"] for r in await conn.fetch(suggest_q, data.guild_id, data.name)]
            return TagsSearchResponse(items=[], total=0, suggestions=suggestions)

        items = [
            TagRowDTO(
                id=r["id"],
                guild_id=r["guild_id"],
                name=r["name"],
                owner_id=r["owner_id"],
                is_alias=r["is_alias"],
                uses=r.get("uses"),
                content=r.get("content"),
                rank=r.get("rank"),
            )
            for r in rows
        ]

        return TagsSearchResponse(items=items, total=len(items))

    @post(path="/mutate")
    async def mutate(self, conn: Connection, data: TagsMutateRequest) -> TagsMutateResponse:  # noqa: PLR0912, PLR0915
        """Mutate a tag."""
        results: list[TagsMutateResult] = []

        for op in data.ops:
            try:
                if op.op == "create":
                    q = """
                        WITH ins AS (
                            INSERT INTO tags (name, content, owner_id, location_id)
                            VALUES ($1,$2,$3,$4)
                            RETURNING id
                        )
                        INSERT INTO tag_lookup (name, owner_id, location_id, tag_id)
                        VALUES ($1,$3,$4,(SELECT id FROM ins))
                        RETURNING (SELECT id FROM ins);
                    """
                    tag_id = await conn.fetchval(q, op.name, op.content, op.owner_id, op.guild_id)  # type: ignore
                    results.append(TagsMutateResult(ok=True, tag_id=tag_id, message="Tag created"))
                    continue

                if op.op == "alias":
                    q = """
                        INSERT INTO tag_lookup (name, owner_id, location_id, tag_id)
                        SELECT $1,$4,tag_lookup.location_id,tag_lookup.tag_id
                        FROM tag_lookup
                        WHERE tag_lookup.location_id=$3 AND LOWER(tag_lookup.name)=LOWER($2);
                    """
                    res = await conn.execute(q, op.new_name, op.old_name, op.guild_id, op.owner_id)  # type: ignore
                    results.append(TagsMutateResult(ok=True, affected=int(res.split()[-1]), message="Alias created"))
                    continue

                if op.op == "edit":
                    q = """
                        UPDATE tags
                        SET content=$1
                        WHERE LOWER(name)=LOWER($2) AND location_id=$3 AND owner_id=$4;
                    """
                    res = await conn.execute(q, op.new_content, op.name, op.guild_id, op.owner_id)  # type: ignore
                    results.append(TagsMutateResult(ok=True, affected=int(res.split()[-1]), message="Tag edited"))
                    continue

                if op.op == "remove":
                    async with conn.transaction():
                        del_lookup = await conn.fetchrow(
                            "DELETE FROM tag_lookup WHERE LOWER(name)=LOWER($1) AND location_id=$2 RETURNING tag_id;",
                            op.name,  # type: ignore
                            op.guild_id,
                        )
                        if not del_lookup:
                            results.append(TagsMutateResult(ok=False, message="Tag not found"))
                            continue
                        tag_id = del_lookup["tag_id"]
                        await conn.execute("DELETE FROM tags WHERE id=$1;", tag_id)
                        results.append(TagsMutateResult(ok=True, message="Tag deleted"))
                    continue

                if op.op == "remove_by_id":
                    async with conn.transaction():
                        await conn.execute(
                            "DELETE FROM tag_lookup WHERE tag_id=$1 AND location_id=$2;",
                            op.tag_id,  # type: ignore
                            op.guild_id,
                        )
                        res = await conn.execute(
                            "DELETE FROM tags WHERE id=$1 AND location_id=$2;",
                            op.tag_id,  # type: ignore
                            op.guild_id,
                        )
                        results.append(TagsMutateResult(ok=True, affected=int(res.split()[-1]), message="Tag deleted"))
                    continue

                if op.op == "increment_usage":
                    await conn.execute(
                        "UPDATE tags SET uses = uses + 1 WHERE LOWER(name)=LOWER($1) AND location_id=$2;",
                        op.name,  # pyright: ignore[reportAttributeAccessIssue]
                        op.guild_id,
                    )
                    results.append(TagsMutateResult(ok=True, message="Usage incremented"))
                    continue

                if op.op == "transfer":
                    async with conn.transaction():
                        q1 = "SELECT id FROM tags WHERE LOWER(name)=LOWER($1) AND location_id=$2 AND owner_id=$3;"
                        row = await conn.fetchrow(q1, op.name, op.guild_id, op.requester_id)  # type: ignore
                        if not row:
                            results.append(TagsMutateResult(ok=False, message="No permission or tag not found"))
                            continue
                        tag_id = row["id"]
                        await conn.execute("UPDATE tags SET owner_id=$1 WHERE id=$2;", op.new_owner_id, tag_id)  # type: ignore
                        await conn.execute(
                            "UPDATE tag_lookup SET owner_id=$1 WHERE tag_id=$2;",
                            op.new_owner_id,  # type: ignore
                            tag_id,
                        )
                        results.append(TagsMutateResult(ok=True, message="Ownership transferred"))
                    continue

                if op.op == "purge":
                    res = await conn.execute(
                        "DELETE FROM tags WHERE location_id=$1 AND owner_id=$2;",
                        op.guild_id,
                        op.owner_id,  # type: ignore
                    )
                    results.append(TagsMutateResult(ok=True, affected=int(res.split()[-1]), message="User purged"))
                    continue

                if op.op == "claim":
                    async with conn.transaction():
                        row = await conn.fetchrow(
                            "SELECT id FROM tags WHERE location_id=$1 AND LOWER(name)=LOWER($2);",
                            op.guild_id,
                            op.name,  # type: ignore
                        )
                        if not row:
                            results.append(TagsMutateResult(ok=False, message="Tag not found"))
                            continue
                        tag_id = row["id"]
                        await conn.execute("UPDATE tags SET owner_id=$1 WHERE id=$2;", op.requester_id, tag_id)  # type: ignore
                        await conn.execute(
                            "UPDATE tag_lookup SET owner_id=$1 WHERE tag_id=$2;",
                            op.requester_id,  # type: ignore
                            tag_id,
                        )
                        results.append(TagsMutateResult(ok=True, message="Tag claimed"))
                    continue

                results.append(TagsMutateResult(ok=False, message=f"Unknown op {op.op}"))
            except Exception as e:
                results.append(TagsMutateResult(ok=False, message=str(e)))

        return TagsMutateResponse(results=results)

    @post(path="/autocomplete")
    async def autocomplete(self, conn: Connection, data: TagsAutocompleteRequest) -> TagsAutocompleteResponse:
        """Autocomplete route for tags."""
        if not data.q.strip():
            return TagsAutocompleteResponse(items=[])
        table = "tag_lookup" if "aliased" in data.mode else "tags"
        clauses = ["location_id=$1"]
        params: list[int | str] = [data.guild_id]
        idx = 2

        if "owned" in data.mode and data.owner_id is not None:
            clauses.append(f"owner_id=${idx}")
            params.append(data.owner_id)
            idx += 1

        clauses.append(f"LOWER(name) % LOWER(${idx})")
        params.append(data.q)

        sql = f"""
            SELECT name
            FROM {table}
            WHERE {" AND ".join(clauses)}
            ORDER BY similarity(name, ${idx}) DESC
            LIMIT {int(data.limit)};
        """
        rows = await conn.fetch(sql, *params)
        return TagsAutocompleteResponse(items=[r["name"] for r in rows])
