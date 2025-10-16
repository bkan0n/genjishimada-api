import pytest
from litestar import Litestar
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_204_NO_CONTENT, HTTP_400_BAD_REQUEST
from litestar.testing import AsyncTestClient
from asyncpg import Connection
# ruff: noqa: D102, D103, ANN001, ANN201


class TestMapsEndpoints:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "code,expected,http_status",
        [
            ("AAAAA", False, 200),
            ("BBBBB", False, 200),
            ("1EASY", True, 200),
            ("BAD", False, 400),
            ("BADAGAIN", False, 400),
        ],
    )
    async def test_check_code_exists(
        self,
        test_client: AsyncTestClient[Litestar],
        code: str,
        expected: bool,
        http_status: int
    ) -> None:
        response = await test_client.get(f"/api/v3/maps/{code}/exists/")
        assert response.status_code == http_status
        if http_status != 400:
            assert response.json() == expected

    @pytest.mark.asyncio
    async def test_get_guides(self, test_client):
        response = await test_client.get(f"/api/v3/maps/2GUIDE/guides/")
        assert response.status_code == HTTP_200_OK
        assert len(response.json()) == 2
        for x in response.json():
            assert x["url"] is not None
            assert x["user_id"] is not None
            assert x["usernames"] is not None

    @pytest.mark.asyncio
    async def test_create_guides(self, test_client):
        response = await test_client.get(f"/api/v3/maps/GUIDE/guides/")
        assert response.status_code == HTTP_200_OK
        assert not response.json()


        new_data = {
            "user_id": 53,
            "url": "https://www.youtube.com/watch?v=ri76tCrDjXw"
        }
        response = await test_client.post(f"/api/v3/maps/GUIDE/guides/", json=new_data)
        assert response.status_code == HTTP_201_CREATED

        data = response.json()
        assert data["user_id"] == 53
        assert data["url"] == "https://www.youtube.com/watch?v=ri76tCrDjXw"

        response = await test_client.get(f"/api/v3/maps/GUIDE/guides/")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data[0]["user_id"] == 53
        assert data[0]["url"] == "https://www.youtube.com/watch?v=ri76tCrDjXw"
        assert data[0]["usernames"] == ['GuideMaker', 'GuideMaker']

    @pytest.mark.asyncio
    async def test_delete_guides(self, test_client):
        response = await test_client.get(f"/api/v3/maps/1GUIDE/guides/")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert len(data) == 1
        assert data[0]["url"] == 'https://www.youtube.com/watch?v=FJs41oeAnHU'
        assert data[0]["user_id"] == 53
        assert data[0]["usernames"] is not None

        response = await test_client.delete(f"/api/v3/maps/1GUIDE/guides/53")
        assert response.status_code == HTTP_204_NO_CONTENT

        response = await test_client.get(f"/api/v3/maps/1GUIDE/guides/")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert len(data) == 0

    @pytest.mark.asyncio
    async def test_edit_guides(self, test_client):
        response = await test_client.get(f"/api/v3/maps/3GUIDE/guides/")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert len(data) == 1
        assert data[0]["url"] == 'https://www.youtube.com/watch?v=GU8htjxY6ro'
        assert data[0]["user_id"] == 54
        assert data[0]["usernames"] is not None

        response = await test_client.patch(f"/api/v3/maps/3GUIDE/guides/54?url=https://www.youtube.com/watch?v=FJs41oeAnHU")
        assert response.status_code == HTTP_200_OK

        response = await test_client.get(f"/api/v3/maps/3GUIDE/guides/")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert len(data) == 1
        assert data[0]["url"] == 'https://www.youtube.com/watch?v=FJs41oeAnHU'
        assert data[0]["user_id"] == 54
        assert data[0]["usernames"] is not None
