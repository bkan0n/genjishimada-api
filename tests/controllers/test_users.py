from logging import getLogger
import pytest
from litestar import Litestar
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_400_BAD_REQUEST
from litestar.testing import AsyncTestClient
from asyncpg import Connection
# ruff: noqa: D102, D103, ANN001, ANN201

log = getLogger(__name__)
class TestUsersEndpoints:
    @pytest.mark.asyncio
    async def test_create_user_real(
        self,
        test_client: AsyncTestClient[Litestar],
    ) -> None:
        user_id, global_name, nickname = (12345678912345678, "Fake1", "AA")
        response = await test_client.post(f"/api/v3/users", json={
            "id": user_id,
            "global_name": global_name,
            "nickname": nickname,
        })
        assert response.status_code == HTTP_201_CREATED
        return_data = response.json()
        assert return_data["id"] == user_id
        assert return_data["global_name"] == global_name
        assert return_data["nickname"] == nickname
        assert return_data["coalesced_name"] == nickname
        assert return_data["coins"] == 0

    @pytest.mark.asyncio
    async def test_create_user_fake_fail(
        self,
        test_client: AsyncTestClient[Litestar],
    ) -> None:
        user_id, global_name, nickname = (9999, "Fake1", "AA")
        response = await test_client.post(f"/api/v3/users", json={
            "id": user_id,
            "global_name": global_name,
            "nickname": nickname,
        })
        assert response.status_code == HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_create_user_duplicate(self, test_client: AsyncTestClient[Litestar]):
        user_id, global_name, nickname = (12345678912345670, "NoDupe", "NoDupe")
        response = await test_client.post(f"/api/v3/users", json={
            "id": user_id,
            "global_name": global_name,
            "nickname": nickname,
        })
        assert response.status_code == HTTP_201_CREATED
        response = await test_client.post(f"/api/v3/users", json={
            "id": user_id,
            "global_name": global_name,
            "nickname": nickname,
        })
        assert response.status_code == HTTP_400_BAD_REQUEST


    @pytest.mark.asyncio
    async def test_list_users(self, test_client: AsyncTestClient[Litestar]):
        response = await test_client.get(f"/api/v3/users")
        assert response.status_code == HTTP_200_OK

    @pytest.mark.asyncio
    async def test_get_user(self, test_client: AsyncTestClient[Litestar]):
        response = await test_client.get(f"/api/v3/users/100000000000000000")
        assert response.status_code == HTTP_200_OK
        data = response.json()

        assert data["id"] == 100000000000000000
        assert data["global_name"] == "ShadowSlayerGlobal"
        assert data["nickname"] == "ShadowSlayerNick"
        assert data["coalesced_name"] == "ShadowSlayer#1001"
        assert data["coins"] == 50
        assert data["overwatch_usernames"] == ["ShadowSlayer#1001", "ShadowSlayerAlt#1001"]

    @pytest.mark.asyncio
    async def test_get_notification_settings(self, test_client: AsyncTestClient[Litestar]):
        response = await test_client.get(f"/api/v3/users/23/notifications")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["notifications"] == ['DM_ON_VERIFICATION', 'DM_ON_SKILL_ROLE_UPDATE', 'DM_ON_LOOTBOX_GAIN', 'PING_ON_XP_GAIN', 'PING_ON_MASTERY', 'PING_ON_COMMUNITY_RANK_UPDATE']
        assert data["user_id"] == 23
        response = await test_client.get(f"/api/v3/users/23/notifications?to_bitmask=true")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["bitmask"] == 231
        assert data["user_id"] == 23

    @pytest.mark.asyncio
    async def test_bulk_update_notification(self, test_client: AsyncTestClient[Litestar]):
        response = await test_client.get(f"/api/v3/users/24/notifications")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["notifications"] == []
        assert data["user_id"] == 24
        response = await test_client.put(f"/api/v3/users/24/notifications", json={"notifications": ['DM_ON_VERIFICATION', 'DM_ON_SKILL_ROLE_UPDATE']})
        assert response.status_code == HTTP_200_OK
        response = await test_client.get(f"/api/v3/users/24/notifications")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["notifications"] == ['DM_ON_VERIFICATION', 'DM_ON_SKILL_ROLE_UPDATE']

    @pytest.mark.asyncio
    async def test_get_overwatch_names(self, test_client):
        response = await test_client.get(f"/api/v3/users/100000000000000000/overwatch")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 100000000000000000
        assert data["primary"] == "ShadowSlayer#1001"
        assert data["secondary"] == "ShadowSlayerAlt#1001"
        assert data["tertiary"] == None

    @pytest.mark.asyncio
    async def test_get_overwatch_names_array_size(self, test_client):
        response = await test_client.get(f"/api/v3/users/100000000000000001/overwatch")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 100000000000000001
        assert data["primary"] == "PixelMage#2002"
        assert data["secondary"] == None
        assert data["tertiary"] == None

    @pytest.mark.asyncio
    async def test_replace_overwatch_usernames(self, test_client):
        response = await test_client.get(f"/api/v3/users/100000000000000002/overwatch")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data["user_id"] == 100000000000000002
        assert data["primary"] == "NovaKnightOW1"
        assert data["secondary"] == "NovaKnightShadowOW2"
        assert data["tertiary"] == "NovaKnightShadowOW3"

        new_data = {"usernames": [{"username": "1", "is_primary": True},{"username": "2", "is_primary": False},{"username": "3", "is_primary": False}]}

        response = await test_client.put(f"/api/v3/users/100000000000000002/overwatch", json=new_data)
        assert response.status_code == HTTP_200_OK

        response = await test_client.get(f"/api/v3/users/100000000000000002/overwatch")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data["user_id"] == 100000000000000002
        assert data["primary"] == "1"
        assert data["secondary"] == "2"
        assert data["tertiary"] == "3"

    @pytest.mark.asyncio
    async def test_update_usernames(self, test_client):
        response = await test_client.get(f"/api/v3/users/100000000000000005")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data["id"] == 100000000000000005
        assert data["nickname"] == "PreUpdateNick1"
        assert data["global_name"] == "PreUpdateGlobal1"

        new_data = {"nickname": "PostUpdateNick1", "global_name": "PostUpdateGlobal1"}

        response = await test_client.patch(f"/api/v3/users/100000000000000005", json=new_data)
        assert response.status_code == HTTP_200_OK

        response = await test_client.get(f"/api/v3/users/100000000000000005")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data["id"] == 100000000000000005
        assert data["nickname"] == "PostUpdateNick1"
        assert data["global_name"] == "PostUpdateGlobal1"

    @pytest.mark.asyncio
    async def test_update_usernames_only_nick(self, test_client):
        response = await test_client.get(f"/api/v3/users/100000000000000007")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data["id"] == 100000000000000007
        assert data["nickname"] == "PreUpdateNick2"
        assert data["global_name"] == "PreUpdateGlobal2"

        new_data = {"nickname": "PostUpdateNick2"}

        response = await test_client.patch(f"/api/v3/users/100000000000000007", json=new_data)
        assert response.status_code == HTTP_200_OK

        response = await test_client.get(f"/api/v3/users/100000000000000007")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data["id"] == 100000000000000007
        assert data["nickname"] == "PostUpdateNick2"
        assert data["global_name"] == "PreUpdateGlobal2"

    @pytest.mark.asyncio
    async def test_update_usernames_only_global(self, test_client):
        response = await test_client.get(f"/api/v3/users/100000000000000008")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data["id"] == 100000000000000008
        assert data["nickname"] == "PreUpdateNick3"
        assert data["global_name"] == "PreUpdateGlobal3"

        new_data = {"global_name": "PostUpdateGlobal3"}

        response = await test_client.patch(f"/api/v3/users/100000000000000008", json=new_data)
        assert response.status_code == HTTP_200_OK

        response = await test_client.get(f"/api/v3/users/100000000000000008")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data["id"] == 100000000000000008
        assert data["nickname"] == "PreUpdateNick3"
        assert data["global_name"] == "PostUpdateGlobal3"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "notification_type",
        [
            "DM_ON_VERIFICATION",
            'DM_ON_SKILL_ROLE_UPDATE',
            'DM_ON_LOOTBOX_GAIN',
            'PING_ON_XP_GAIN',
            'PING_ON_MASTERY',
            'PING_ON_COMMUNITY_RANK_UPDATE',
        ],
    )
    async def test_toggle_single_notification(self, test_client, notification_type):
        response = await test_client.get(f"/api/v3/users/25/notifications")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["notifications"] == []
        assert data["user_id"] == 25

        response = await test_client.patch(f"/api/v3/users/25/notifications/{notification_type}", content="true")
        assert response.status_code == HTTP_200_OK

        response = await test_client.get(f"/api/v3/users/25/notifications")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["notifications"] == [notification_type]
        assert data["user_id"] == 25

        response = await test_client.patch(f"/api/v3/users/25/notifications/{notification_type}", content="false")
        assert response.status_code == HTTP_200_OK

        response = await test_client.get(f"/api/v3/users/25/notifications")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["notifications"] == []
        assert data["user_id"] == 25

    @pytest.mark.asyncio
    async def test_check_if_creator(self, test_client):
        response = await test_client.get("/api/v3/users/2/creator")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data == True

    @pytest.mark.asyncio
    async def test_check_if_not_creator(self, test_client):
        response = await test_client.get("/api/v3/users/100000000000000006/creator")
        assert response.status_code == HTTP_200_OK

        data = response.json()
        assert data == False
