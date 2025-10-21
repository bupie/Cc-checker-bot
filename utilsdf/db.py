import datetime
import random
import string
from typing import Union, Optional, Dict
from os import getenv

# 1. Import the async driver
from motor.motor_asyncio import AsyncIOMotorClient

# Define Database and Collection names
MONGO_URI = getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "bot_database"

class AsyncMongoDatabase:
    # Collections (replacing SQL Tables)
    USERS_COLLECTION = "users"
    KEYS_COLLECTION = "bot_keys"
    GROUPS_COLLECTION = "groups"
    ID_OWNER = '6937607934'
    
    # Removed Singleton pattern (__new__, _instance)

    async def __init__(self):
        # 2. Asynchronous Connection
        self.client = AsyncIOMotorClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        
        # 3. Initialize Collections
        self.users = self.db[self.USERS_COLLECTION]
        self.keys = self.db[self.KEYS_COLLECTION]
        self.groups = self.db[self.GROUPS_COLLECTION]
        
        # 4. Create Indexes (replaces PRIMARY KEY/UNIQUE)
        await self.users.create_index("ID", unique=True)
        await self.keys.create_index("BOT_KEY", unique=True)
        await self.groups.create_index("ID", unique=True)
        
        await self.__initialize_owner()

    # NOTE: All methods are now 'async def' and use 'await' before Motor calls

    async def __initialize_owner(self) -> None:
        if not await self.is_seller_or_admin(self.ID_OWNER):
            # Using update_one with upsert=True to create if not exists
            await self.users.update_one(
                {"ID": self.ID_OWNER},
                {"$set": {
                    "USERNAME": "owner",
                    "NICK": "owner",
                    "RANK": "admin",
                    "STATE": "free",
                    "MEMBERSHIP": "Premium",
                    "EXPIRATION": None,
                    "ANTISPAM": 40,
                    "CREDITS": 300,
                    "REGISTERED": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "CHECKS": 0,
                }},
                upsert=True 
            )
            await self.add_premium_membership(int(self.ID_OWNER), 30, 300)

    async def add_premium_membership(
        self, user_id: int, days: int, credits: int
    ) -> Optional[str]:
        user_id = str(user_id)
        
        user_data = await self.users.find_one({"ID": user_id})
        if user_data is None:
            return None

        expiration_time = (
            datetime.datetime.now() + datetime.timedelta(days=days)
        ).strftime("%Y-%m-%d %H:%M:%S")
        
        await self.users.update_one(
            {"ID": user_id},
            {"$set": {
                "MEMBERSHIP": "Premium", 
                "ANTISPAM": 40, 
                "CREDITS": credits, 
                "EXPIRATION": expiration_time
            }},
        )
        return expiration_time

    async def is_premium(self, user_id: int) -> bool:
        user_id = str(user_id)
        user_data = await self.users.find_one({"ID": user_id})
        return user_data and user_data.get("MEMBERSHIP", "").lower() == "premium"

    async def register_user(self, user_id: int, username: str) -> None:
        user_id = str(user_id)
        time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Using update_one with $setOnInsert for safe, non-duplicating insertion
        await self.users.update_one(
            {"ID": user_id},
            {"$setOnInsert": {
                "ID": user_id, 
                "USERNAME": username, 
                "NICK": '¿?',
                "RANK": 'user',
                "STATE": 'free',
                "MEMBERSHIP": 'free user',
                "EXPIRATION": None,
                "ANTISPAM": 60,
                "CREDITS": 0,
                "REGISTERED": time,
                "CHECKS": 0,
            }},
            upsert=True
        )

    async def gen_key(self, days: int) -> tuple:
        expire_day = (
            datetime.datetime.now() + datetime.timedelta(days=int(days))
        ).strftime("%Y-%m-%d %H:%M:%S")
        key = "key-aktz" + "".join(
            random.choice(string.ascii_letters) for _ in range(8)
        )
        
        await self.keys.insert_one(
            {"BOT_KEY": key, "EXPIRATION": expire_day}
        )
        return key, expire_day

    async def rename_premium(self, user_id: int) -> Optional[int]:
        user_id = str(user_id)
        
        result = await self.users.update_one(
            {"ID": user_id, "MEMBERSHIP": "Premium"},
            {"$set": {
                "MEMBERSHIP": "free user", 
                "RANK": "user", 
                "ANTISPAM": 60, 
                "EXPIRATION": None
            }}
        )
        return result.modified_count if result.modified_count > 0 else None

    async def remove_group(self, chat_id: str) -> Optional[int]:
        result = await self.groups.delete_one({"ID": chat_id})
        return result.deleted_count if result.deleted_count > 0 else None

    async def unban_or_ban_user(self, user_id: int, ban: bool = True) -> Optional[int]:
        user_id = str(user_id)
        status = "ban" if ban else "free"
        
        result = await self.users.update_one(
            {"ID": user_id},
            {"$set": {
                "RANK": "user", 
                "MEMBERSHIP": "free user", 
                "ANTISPAM": 60, 
                "CREDITS": 0, 
                "EXPIRATION": None, 
                "STATE": status
            }}
        )
        return result.modified_count if result.modified_count > 0 else None

    async def is_ban(self, user_id: int) -> bool:
        user_id = str(user_id)
        user_data = await self.users.find_one({"ID": user_id})
        return user_data and user_data.get("STATE", "").lower() == "ban"

    async def claim_key(self, key: str, user_id: int) -> Optional[str]:
        user_id = str(user_id)
        
        key_data = await self.keys.find_one({"BOT_KEY": key})
        if key_data is None:
            return None
            
        expiration_time = key_data["EXPIRATION"]
        
        await self.users.update_one(
            {"ID": user_id},
            {"$set": {
                "MEMBERSHIP": "Premium", 
                "ANTISPAM": 40, 
                "EXPIRATION": expiration_time
            }}
        )
        
        await self.keys.delete_one({"BOT_KEY": key})
        return expiration_time

    async def __is_rank(self, user_id: Union[int, str], rank: str) -> bool:
        user_id = str(user_id)
        user_data = await self.users.find_one({"ID": user_id}, {"RANK": 1}) # Project to get only RANK field
        return user_data and user_data.get("RANK", "").lower() == rank

    async def is_admin(self, user_id: int) -> bool:
        return await self.__is_rank(user_id, "admin")

    async def is_seller(self, user_id: int) -> bool:
        return await self.__is_rank(user_id, "seller")

    async def is_seller_or_admin(self, user_id) -> bool:
        return await self.is_admin(user_id) or await self.is_seller(user_id)

    async def __get_info(self, ID: Union[int, str], group: bool = False) -> Dict[str, Union[str, int]] | None:
        ID = str(ID)
        collection = self.groups if group else self.users
        return await collection.find_one({"ID": ID})

    async def get_info_user(self, user_id: int) -> Dict[str, Union[str, int]] | None:
        # MongoDB returns a dictionary directly, which is what your function needed
        return await self.__get_info(user_id, group=False)

    async def get_info_group(self, chat_id: int) -> Dict[str, Union[str, int]] | None:
        group_data = await self.__get_info(chat_id, group=True)
        # We manually map the MongoDB document keys to your expected dictionary format
        return {
            "ID": group_data["ID"],
            "EXPIRATION": group_data["EXPIRATION"],
        } if group_data else None

    async def get_chats_ids(self) -> list:
        # Use find({}, {"ID": 1}) to get only the ID field, and to_list() to await the results
        users_cursor = self.users.find({}, {"ID": 1, "_id": 0})
        users_data = await users_cursor.to_list(length=None)
        
        groups_cursor = self.groups.find({}, {"ID": 1, "_id": 0})
        groups_data = await groups_cursor.to_list(length=None)
        
        # Combine and extract IDs
        all_ids = [doc["ID"] for doc in users_data]
        all_ids.extend([doc["ID"] for doc in groups_data])
        return all_ids

    async def group_authorized(self, chat_id: int) -> bool:
        chat_id = str(chat_id)
        data = await self.groups.find_one({"ID": chat_id}, {"EXPIRATION": 1})
        return bool(data) # True if data exists, False otherwise

    async def user_has_credits(self, user_id: int) -> bool:
        user_id = str(user_id)
        user_data = await self.users.find_one({"ID": user_id}, {"CREDITS": 1})
        return user_data and user_data.get("CREDITS", 0) > 0

    async def remove_credits(self, user_id: int, credits: int) -> None:
        if credits <= 0:
            return
        
        user_id = str(user_id)
        
        # Atomically decrease credits using $inc and ensure it doesn't drop below 0
        await self.users.update_one(
            {"ID": user_id, "CREDITS": {"$gt": 0}}, # Only update if credits > 0
            {"$inc": {"CREDITS": -credits}}
        )

    async def add_group(self, chat_id: int, days: int, username: str) -> Union[str, bool]:
        chat_id = str(chat_id)
        expiration_time = (
            datetime.datetime.now() + datetime.timedelta(days=days)
        ).strftime("%Y-%m-%d %H:%M:%S")
        
        # Use upsert to handle both INSERT and UPDATE (like your try/except block)
        await self.groups.update_one(
            {"ID": chat_id},
            {"$set": {
                "EXPIRATION": expiration_time, 
                "PROVIDER": username
            }},
            upsert=True
        )
        return expiration_time

    async def is_authorized(self, user_id: int, chat_id: int) -> bool:
        return await self.is_premium(user_id) or await self.group_authorized(chat_id)

    async def remove_expireds_users(self) -> None:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 1. Reset expired premium users to 'free user' status
        await self.users.update_many(
            {"EXPIRATION": {"$lt": now}, "MEMBERSHIP": "Premium"},
            {"$set": {
                "MEMBERSHIP": "free user", 
                "RANK": "user", 
                "ANTISPAM": 60, 
                "EXPIRATION": None,
            }}
        )
        
        # 2. Delete expired keys
        await self.keys.delete_many(
            {"EXPIRATION": {"$lt": now}}
        )
        
        # 3. Delete expired groups
        await self.groups.delete_many(
            {"EXPIRATION": {"$lt": now}}
        )

    async def increase_checks(self, user_id: int, quantity: int = 1) -> bool | None:
        user_id, quantity = str(user_id), int(quantity)

        result = await self.users.update_one(
            {"ID": user_id},
            {"$inc": {"CHECKS": quantity}} # $inc is atomic and safe
        )
        return result.modified_count > 0

    async def update_colum(self, user_id: int, column: str, value) -> bool | None:
        user_id = str(user_id)
        
        # Safety check: Prevent updating ID or _id
        if column in ["ID", "_id", "REGISTERED"]: 
            return False

        result = await self.users.update_one(
            {"ID": user_id},
            {"$set": {column: value}}
        )
        return result.modified_count > 0

    async def __promote(self, user_id: int, rank: str) -> bool | None:
        user_id = str(user_id)
        
        result = await self.users.update_one(
            {"ID": user_id},
            {"$set": {"RANK": rank}}
        )
        return result.modified_count > 0

    async def set_nick(self, user_id: int, nick: str) -> bool | None:
        return await self.update_colum(user_id, "NICK", nick)

    async def set_antispam(self, user_id: int, antispam: int) -> bool | None:
        return await self.update_colum(user_id, "ANTISPAM", int(antispam))

    async def promote_to_seller(self, user_id: int) -> bool | None:
        return await self.__promote(user_id, "seller")

    async def promote_to_admin(self, user_id: int) -> bool | None:
        return await self.__promote(user_id, "admin")
        
    def close(self):
        """Closes the MongoDB connection."""
        self.client.close()

    # Removed __enter__ and __exit__ as they are not needed with persistent async connections
