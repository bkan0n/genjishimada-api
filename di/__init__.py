from .autocomplete import AutocompleteService, provide_autocomplete_service
from .change_requests import ChangeRequestsService, provide_change_requests_service
from .community import CommunityService, provide_community_service
from .completions import CompletionsService, provide_completions_service
from .image_storage import ImageStorageService, provide_image_storage_service
from .lootbox import LootboxService, provide_lootbox_service
from .lust import LustService, provide_lust_service
from .maps import MapService, provide_map_service
from .newsfeed import NewsfeedService, provide_newsfeed_service
from .playtests import PlaytestService, provide_playtest_service
from .rank_card import RankCardService, provide_rank_card_service
from .users import UserService, provide_user_service

__all__ = (
    "AutocompleteService",
    "ChangeRequestsService",
    "CommunityService",
    "CompletionsService",
    "ImageStorageService",
    "LootboxService",
    "LustService",
    "MapService",
    "NewsfeedService",
    "PlaytestService",
    "RankCardService",
    "UserService",
    "provide_autocomplete_service",
    "provide_change_requests_service",
    "provide_community_service",
    "provide_completions_service",
    "provide_image_storage_service",
    "provide_lootbox_service",
    "provide_lust_service",
    "provide_map_service",
    "provide_newsfeed_service",
    "provide_playtest_service",
    "provide_rank_card_service",
    "provide_user_service",
)
