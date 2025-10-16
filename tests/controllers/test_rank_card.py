from logging import getLogger
import pytest
from litestar import Litestar
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_400_BAD_REQUEST
from litestar.testing import AsyncTestClient
from asyncpg import Connection
# ruff: noqa: D102, D103, ANN001, ANN201

log = getLogger(__name__)
class TestRankCardEndpoints:
    @pytest.mark.asyncio
    async def test_get_avatar_pose_good(self, test_client):
        response = await test_client.get("/api/v3/users/50/rank-card/avatar/pose")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["pose"] == "Heroic"
        assert data["skin"] == "Overwatch 1"
        assert data["url"] == "assets/rank_card/avatar/overwatch_1/heroic.webp"

    @pytest.mark.asyncio
    async def test_set_avatar_pose(self, test_client):
        response = await test_client.get("/api/v3/users/51/rank-card/avatar/pose")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["pose"] == "Heroic"
        assert data["skin"] == "Overwatch 1"
        assert data["url"] == "assets/rank_card/avatar/overwatch_1/heroic.webp"

        response = await test_client.put("/api/v3/users/51/rank-card/avatar/pose", json={"pose": "Medal"})
        assert response.status_code == HTTP_200_OK


        response = await test_client.get("/api/v3/users/51/rank-card/avatar/pose")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["pose"] == "Medal"
        assert data["skin"] == "Overwatch 1"
        assert data["url"] == "assets/rank_card/avatar/overwatch_1/medal.webp"

    @pytest.mark.asyncio
    async def test_get_avatar_skin_good(self, test_client):
        response = await test_client.get("/api/v3/users/52/rank-card/avatar/skin")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["pose"] == "Heroic"
        assert data["skin"] == "Overwatch 1"
        assert data["url"] == "assets/rank_card/avatar/overwatch_1/heroic.webp"

    @pytest.mark.asyncio
    async def test_set_avatar_skin(self, test_client):
        response = await test_client.get("/api/v3/users/52/rank-card/avatar/skin")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["pose"] == "Heroic"
        assert data["skin"] == "Overwatch 1"
        assert data["url"] == "assets/rank_card/avatar/overwatch_1/heroic.webp"

        response = await test_client.put("/api/v3/users/52/rank-card/avatar/skin", json={"skin": "Nihon"})
        assert response.status_code == HTTP_200_OK


        response = await test_client.get("/api/v3/users/52/rank-card/avatar/skin")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["pose"] == "Heroic"
        assert data["skin"] == "Nihon"
        assert data["url"] == "assets/rank_card/avatar/nihon/heroic.webp"

    @pytest.mark.asyncio
    async def test_get_background_good(self, test_client):
        response = await test_client.get("/api/v3/users/50/rank-card/background")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["name"] == "placeholder"
        assert data["url"] == "assets/rank_card/background/placeholder.webp"

    @pytest.mark.asyncio
    async def test_set_background(self, test_client):
        response = await test_client.get("/api/v3/users/52/rank-card/background")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["name"] == "placeholder"
        assert data["url"] == "assets/rank_card/background/placeholder.webp"

        response = await test_client.put("/api/v3/users/52/rank-card/background", json={"name": "Ayutthaya"})
        assert response.status_code == HTTP_200_OK


        response = await test_client.get("/api/v3/users/52/rank-card/background")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["name"] == "Ayutthaya"
        assert data["url"] == "assets/rank_card/background/ayutthaya.webp"


    @pytest.mark.asyncio
    async def test_get_badges(self, test_client):
        response = await test_client.get("/api/v3/users/50/rank-card/badges")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["badge_name1"] == None
        assert data["badge_name2"] == None
        assert data["badge_name3"] == None
        assert data["badge_name4"] == None
        assert data["badge_name5"] == None
        assert data["badge_name6"] == None
        assert data["badge_type1"] == None
        assert data["badge_type2"] == None
        assert data["badge_type3"] == None
        assert data["badge_type4"] == None
        assert data["badge_type5"] == None
        assert data["badge_type6"] == None
        assert data["badge_url1"] == None
        assert data["badge_url2"] == None
        assert data["badge_url3"] == None
        assert data["badge_url4"] == None
        assert data["badge_url5"] == None
        assert data["badge_url6"] == None

    @pytest.mark.asyncio
    async def test_set_badges(self, test_client):
        response = await test_client.get("/api/v3/users/52/rank-card/badges")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["badge_name1"] == None
        assert data["badge_name2"] == None
        assert data["badge_name3"] == None
        assert data["badge_name4"] == None
        assert data["badge_name5"] == None
        assert data["badge_name6"] == None
        assert data["badge_type1"] == None
        assert data["badge_type2"] == None
        assert data["badge_type3"] == None
        assert data["badge_type4"] == None
        assert data["badge_type5"] == None
        assert data["badge_type6"] == None
        assert data["badge_url1"] == None
        assert data["badge_url2"] == None
        assert data["badge_url3"] == None
        assert data["badge_url4"] == None
        assert data["badge_url5"] == None
        assert data["badge_url6"] == None


        new_data = {
            "badge_name1": "string",
            "badge_name2": "string",
            "badge_name3": "string",
            "badge_name4": "string",
            "badge_name5": "string",
            "badge_name6": "string",
            "badge_type1": "string",
            "badge_type2": "string",
            "badge_type3": "string",
            "badge_type4": "string",
            "badge_type5": "string",
            "badge_type6": "string",
        }
        response = await test_client.put("/api/v3/users/52/rank-card/badges", json=new_data)
        assert response.status_code == HTTP_200_OK


        response = await test_client.get("/api/v3/users/52/rank-card/badges")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["badge_name1"] == "string"
        assert data["badge_name2"] == "string"
        assert data["badge_name3"] == "string"
        assert data["badge_name4"] == "string"
        assert data["badge_name5"] == "string"
        assert data["badge_name6"] == "string"
        assert data["badge_type1"] == "string"
        assert data["badge_type2"] == "string"
        assert data["badge_type3"] == "string"
        assert data["badge_type4"] == "string"
        assert data["badge_type5"] == "string"
        assert data["badge_type6"] == "string"
        assert data["badge_url1"] == None
        assert data["badge_url2"] == None
        assert data["badge_url3"] == None
        assert data["badge_url4"] == None
        assert data["badge_url5"] == None
        assert data["badge_url6"] == None
