
# strings.py
import threading
from typing import List

# --- Stubs for external Go types ---
class YaraMatch:
    def Length(self):
        return 0
    def Offset(self):
        return 0

class YaraPattern:
    def Identifier(self):
        return ""
    def Matches(self):
        return []

# --- StringPool for interning ---
class StringPool:
    def __init__(self, length: int):
        self.lock = threading.RLock()
        self.strings = {}

    def intern(self, s: str) -> str:
        with self.lock:
            if s in self.strings:
                return self.strings[s]
            self.strings[s] = s
            return s

# --- MatchProcessor ---
class MatchProcessor:
    def __init__(self, fc: bytes, matches: List[YaraMatch], patterns: List[YaraPattern]):
        self.fc = fc
        self.pool = StringPool(len(matches))
        self.matches = matches
        self.patterns = patterns
        self.lock = threading.Lock()

    def process(self) -> List[str]:
        if not self.matches:
            return []
        result = []
        buffer = bytearray(8)
        patterns_cap = len(self.patterns)
        patterns = None
        for match in self.matches:
            l = int(match.Length())
            o = int(match.Offset())
            if o < 0 or o + l > len(self.fc):
                continue
            match_bytes = self.fc[o:o+l]
            if not contains_unprintable(match_bytes):
                if l <= len(buffer):
                    buffer[:l] = match_bytes
                    result.append(self.pool.intern(buffer[:l].decode(errors='ignore')))
                else:
                    result.append(self.pool.intern(match_bytes.decode(errors='ignore')))
            else:
                if patterns is None or len(patterns) < patterns_cap:
                    patterns = [p.Identifier() for p in self.patterns]
                result.extend(list(set(patterns)))
        return result

def contains_unprintable(b: bytes) -> bool:
    for c in b:
        if c < 32 or c > 126:
            return True
    return False
