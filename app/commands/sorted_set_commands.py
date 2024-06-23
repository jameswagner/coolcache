
from typing import TYPE_CHECKING, List, Tuple
from app.commands.commands import RedisCommand
from sortedcontainers import SortedSet

from app.utils import encoding_utils
from app.utils.constants import FLOAT_ERROR_MESSAGE, NOT_FOUND_RESPONSE, WRONG_TYPE_RESPONSE

if TYPE_CHECKING:
    from app.AsyncHandler import AsyncRequestHandler

class CoolCacheSortedSet:
    def __init__(self):
        self.data = SortedSet()
        self.scores = {}
    
    def __str__(self):
        string_rep = str(self.data)
        #if prefix "SortedSet(" and suffix ")" are present, remove them
        if string_rep.startswith("SortedSet(") and string_rep.endswith(")"):
            string_rep = string_rep[10:-1]
        return string_rep
    
    def as_list(self) -> List [Tuple[float, str]]:
        return [item for item in self.data]

    def check_conditions(self, score, member, only_if_not_exists, only_if_exists, only_if_greater, only_if_less, incr) -> bool:
        if (member not in self.scores and only_if_exists) or (member in self.scores and only_if_not_exists):
            return False
        if only_if_greater and (member in self.scores and self.scores[member] >= score):
            return False
        if only_if_less and (member in self.scores and self.scores[member] <= score):
            return False
        if incr and score > 0 and member in self.scores and only_if_less:
            return False
        if incr and score < 0 and member in self.scores and only_if_greater:
            return False
        return True


    def zadd(self, score, member, only_if_not_exists=False, only_if_exists=False, only_if_greater=False, only_if_less=False, incr=False, count_changed=False) -> bool:

        if not self.check_conditions(score, member, only_if_not_exists, only_if_exists, only_if_greater, only_if_less, incr):
            return False
        print(f"score: {score}, member: {member}, only_if_not_exists: {only_if_not_exists}, only_if_exists: {only_if_exists}, only_if_greater: {only_if_greater}, only_if_less: {only_if_less}, incr: {incr}, count_changed: {count_changed}")
        changed = False
        existed = False
        if member in self.scores:
            # Remove the existing entry from the sorted set
            self.data.remove((self.scores[member], member))
            changed = (not incr and self.scores[member] != score) or (incr and score != 0)
            existed = True   
            if incr:
                score += self.scores[member]
            
        # Update the score in the dictionary
        self.scores[member] = score
        # Add the member with the updated score to the sorted set
        self.data.add((score, member))
        return (not existed) or (existed and changed and count_changed)

    def zrem(self, member) -> bool:
        if member in self.scores:
            score = self.scores[member]
            self.data.remove((score, member))
            del self.scores[member]
            return True
        return False

    def zrange(self, start, end):
        if start < 0:
            start = len(self.data) + start
        if end < 0:
            end = len(self.data) + end
        ret_list = []
        for i in range(start, end+1):
            print(i, len(self.data))
            if i >= len(self.data):
                break
            ret_list.append(self.data[i][1])
        print(ret_list)
        return ret_list
    
    def zrangebyscore(self, min_score, max_score):
        return [item[1] for item in self.data.irange(min_score, max_score)]

    def zrank(self, member):
        if member in self.scores:
            score = self.scores[member]
            return self.data.index((score, member))
        return None

    def zrevrank(self, member):
        if member in self.scores:
            score = self.scores[member]
            return len(self.data) - 1 - self.data.index((score, member))
        return None

    def zscore(self, member):
        return self.scores.get(member)

    def zcard(self):
        return len(self.data)
    
    def zcount(self, min_score, max_score):
        return len(list(self.data.irange(min_score, max_score)))


def get_sorted_set_from_memory(handler: 'AsyncRequestHandler', key: str) -> CoolCacheSortedSet:
    if key not in handler.memory:
        return CoolCacheSortedSet()
    elif not isinstance(handler.memory[key], CoolCacheSortedSet):
        return WRONG_TYPE_RESPONSE
    return handler.memory[key]


class ZAddCommand(RedisCommand):
    
    def parse_options(self, command: List[str]) -> None|Tuple[bool, bool, bool, bool, bool, bool, int]:
        nx = False
        xx = False
        incr = False
        gt = False
        ch = False
        lt = False
        first_num = 2

        for i in range(2, len(command)):
            if command[i] == "NX":
                nx = True
            elif command[i] == "XX":
                xx = True
            elif command[i] == "INCR":
                incr = True
            elif command[i] == "GT":
                gt = True
            elif command[i] == "CH":
                ch = True
            elif command[i] == "LT":
                lt = True
            else:
                try:
                    float(command[i])
                    first_num = i
                    break
                except ValueError:
                    return None

        if (xx and nx) or (gt and lt) or (gt and nx) or (lt and nx):
            return None

        return nx, xx, incr, gt, ch, lt, first_num

    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        options = self.parse_options(command)
        nx, xx, incr, gt, ch, lt, first_num = options
        if options is None:
            return "Invalid options"
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        added = 0
        for i in range(first_num, len(command), 2):
            try:
                score = float(command[i])
            except ValueError:
                return FLOAT_ERROR_MESSAGE
            member = command[i+1]
            if existing_set.zadd(score, member, only_if_exists=xx, only_if_not_exists=nx, only_if_greater=gt, only_if_less=lt, incr=incr, count_changed=ch):
                added += 1
                handler.memory[key] = existing_set
        return f":{added}\r\n"
    
class ZRemCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        removed = 0
        for member in command[2:]:
            if existing_set.zrem(member):
                removed += 1
                handler.memory[key] = existing_set
        return f":{removed}\r\n"
    
class ZRangeCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        try:
            start = int(command[2])
            stop = int(command[3])
        except ValueError:
            return "Invalid start or stop index"
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        members = existing_set.zrange(start, stop)
        return encoding_utils.generate_redis_array(lst = members)
    
class ZRangeByScoreCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        try:
            min_score = float(command[2])
            max_score = float(command[3])
        except ValueError:
            return FLOAT_ERROR_MESSAGE
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        
        #put small epsilon so we query by tuples
        members = existing_set.zrangebyscore((min_score-1e-5, "0"), (max_score+1e-5, "0"))
        
        #we must filter members whose score is less than min_score or greater than max_score
        members = [member for member in members if existing_set.scores[member] >= min_score and existing_set.scores[member] <= max_score]
        
        return encoding_utils.generate_redis_array(members)

class ZRankCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        member = command[2]
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        rank = existing_set.zrank(member)
        if rank is None:
            return NOT_FOUND_RESPONSE
        return f":{rank}\r\n"

class ZRevRankCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        member = command[2]
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        rank = existing_set.zrevrank(member)
        if rank is None:
            return NOT_FOUND_RESPONSE
        return f":{rank}\r\n"
    
class ZScoreCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        member = command[2]
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        score = existing_set.zscore(member)
        if score is None:
            return NOT_FOUND_RESPONSE
        return f":{score}\r\n"

class ZCardCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
        return f":{existing_set.zcard()}\r\n"
    
class ZCountCommand(RedisCommand):
    async def execute(self, handler: 'AsyncRequestHandler', command: List[str]) -> str:
        key = command[1]
        try:
            min_score = float(command[2])
            max_score = float(command[3])
        except ValueError:
            return FLOAT_ERROR_MESSAGE
        existing_set = get_sorted_set_from_memory(handler, key)
        if existing_set == WRONG_TYPE_RESPONSE:
            return WRONG_TYPE_RESPONSE
                #put small epsilon so we query by tuples
        members = existing_set.zrangebyscore((min_score-1e-5, "0"), (max_score+1e-5, "0"))
        #we must filter members whose score is less than min_score or greater than max_score
        members = [member for member in members if existing_set.scores[member] >= min_score and existing_set.scores[member] <= max_score]
        
        return f":{len(members)}\r\n"