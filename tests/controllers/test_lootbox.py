from logging import getLogger
import pytest
from litestar import Litestar
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_400_BAD_REQUEST
from litestar.testing import AsyncTestClient
from asyncpg import Connection
# ruff: noqa: D102, D103, ANN001, ANN201

log = getLogger(__name__)
class TestLootboxEndpoints:
    @pytest.mark.asyncio
    async def test_grant_reward_debug(self, test_client):
        response = await test_client.get("/api/v3/lootbox/users/50/keys")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data[0]["amount"] == 1
        assert data[1]["amount"] == 1
        response = await test_client.post("/api/v3/lootbox/users/debug/50/Classic/skin/Malachite")
        assert response.status_code == HTTP_201_CREATED
        response = await test_client.get("/api/v3/lootbox/users/50/keys")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data[0]["amount"] == 1
        assert data[1]["amount"] == 1

    @pytest.mark.asyncio
    async def test_draw_random_rewards(self, test_client):
        response = await test_client.get("/api/v3/lootbox/users/50/keys/Classic")
        assert response.status_code == HTTP_200_OK
        data = response.json()

    @pytest.mark.asyncio
    async def test_grant_key_to_user(self, test_client):
        response = await test_client.get("/api/v3/lootbox/users/52/keys")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert not data
        response = await test_client.post("/api/v3/lootbox/users/52/keys/Classic")
        assert response.status_code == HTTP_201_CREATED
        response = await test_client.get("/api/v3/lootbox/users/52/keys")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data[0]["amount"] == 1


    @pytest.mark.asyncio
    async def test_get_user_coin_balance(self, test_client):
        response = await test_client.get("/api/v3/lootbox/users/52/coins")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data == 696969
