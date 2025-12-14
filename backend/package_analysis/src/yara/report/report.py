import hashlib
import re
import os
import threading
from typing import List, Dict, Optional, Any

# --- Stubs for external Go types ---
class YaraScanResults:
    def MatchingRules(self):
        return []

class YaraRule:
    def Identifier(self):
        return ""
    def Namespace(self):
        return ""
    def Tags(self):
        return []
    def Patterns(self):
        return []
    def Metadata(self):
        return []

class YaraPattern:
    def Identifier(self):
        return ""
    def Matches(self):
        return []

class YaraMatch:
    def Length(self):
        return 0
    def Offset(self):
        return 0

class Logger:
    def debug(self, *args, **kwargs):
        pass
    def error(self, *args, **kwargs):
        pass

# --- Data classes ---
class Config:
    def __init__(self):
        self.Concurrency = 1
        self.ExitFirstHit = False
        self.ExitFirstMiss = False
        self.FileRiskChange = False
        self.FileRiskIncrease = False
        self.IgnoreSelf = False
        self.IgnoreTags = []
        self.IncludeDataFiles = False
        self.MinFileRisk = 0
        self.MinRisk = 0
        self.OCI = False
        self.Output = None
        self.Processes = False
        self.QuantityIncreasesRisk = False
        self.Renderer = None
        self.RuleFS = []
        self.Rules = None
        self.Scan = False
        self.ScanPaths = []
        self.Stats = False
        self.TrimPrefixes = []

class Behavior:
    def __init__(self):
        self.Description = ""
        self.MatchStrings = []
        self.RiskScore = 0
        self.RiskLevel = ""
        self.RuleURL = ""
        self.ReferenceURL = ""
        self.RuleAuthor = ""
        self.RuleAuthorURL = ""
        self.RuleLicense = ""
        self.RuleLicenseURL = ""
        self.DiffAdded = False
        self.DiffRemoved = False
        self.ID = ""
        self.RuleName = ""
        self.Override = []

class FileReport:
    def __init__(self):
        self.Path = ""
        self.SHA256 = ""
        self.Size = 0
        self.Skipped = ""
        self.Meta = {}
        self.Syscalls = []
        self.Pledge = []
        self.Capabilities = []
        self.Behaviors = []
        self.FilteredBehaviors = 0
        self.PreviousPath = ""
        self.PreviousRelPath = ""
        self.PreviousRelPathScore = 0.0
        self.PreviousRiskScore = 0
        self.PreviousRiskLevel = ""
        self.RiskScore = 0
        self.RiskLevel = ""
        self.IsMalcontent = False
        self.Overrides = []
        self.ArchiveRoot = ""
        self.FullPath = ""

# --- Constants and mappings ---
NAME = "malcontent"
HARMLESS, LOW, MEDIUM, HIGH, CRITICAL = range(5)
RiskLevels = {
    0: "NONE",
    1: "LOW",
    2: "MEDIUM",
    3: "HIGH",
    4: "CRITICAL",
}
Levels = {
    "ignore": -1,
    "none": -1,
    "harmless": 0,
    "low": 1,
    "notable": 2,
    "medium": 2,
    "suspicious": 3,
    "weird": 3,
    "high": 3,
    "crit": 4,
    "critical": 4,
}
yaraForgeJunkWords = set([
    "0", "1", "2", "apt", "artefacts", "artifacts", "base", "big", "controller", "dynamic", "encoded", "exe", "forensic", "forensicartifacts", "generic", "greyware", "hunting", "indicator", "keyword", "linux", "lnx", "m", "mac", "macos", "mal", "malware", "offensive", "osx", "small", "suspicious", "tool", "trojan", "unix", "YARAForge"
])

authorWithURLRe = re.compile(r"(.*?) \((http.*)\)")
threatHuntingKeywordRe = re.compile(r"Detection patterns for the tool '(.*)' taken from the ThreatHunting-Keywords github project")
dateRe = re.compile(r"[a-z]{3}\d{1,2}")

# --- Utility functions ---
def third_party_key(path: str, rule: str) -> str:
    yara_index = path.find("yara/")
    if yara_index == -1:
        return ""
    sub_dir = path[yara_index+5:path.find('/', yara_index+5)]
    words = [sub_dir] + rule.lower().split('_')
    last_word = words[-1]
    try:
        int(last_word, 16)
        words = words[:-1]
    except ValueError:
        pass
    keep_words = []
    for x, w in enumerate(words):
        if x == len(words)-1 and dateRe.match(w):
            continue
        if not w:
            continue
        if w not in yaraForgeJunkWords:
            keep_words.append(w)
    if len(keep_words) > 4:
        keep_words = keep_words[:4]
    src = keep_words[0]
    if src == "signature":
        src = "sig_base"
    rulename = keep_words[1:]
    key = f"3P/{src}/{'_'.join(rulename)}"
    return key

def third_party(src: str) -> bool:
    return "yara/" in src

def is_valid_url(s: str) -> bool:
    try:
        from urllib.parse import urlparse
        urlparse(s)
        return True
    except Exception:
        return False

def generate_key(src: str, rule: str) -> str:
    if third_party(src):
        return third_party_key(src, rule)
    key = src.replace("-", "_").replace(".yara", "")
    dir_parts = key.split("/")
    ns = dir_parts[0].replace("_", "-")
    rsrc = dir_parts[-2]
    tech = dir_parts[-1].replace(rsrc, "").replace("__", "_").strip("_")
    dir_parts[0] = ns
    dir_parts[-1] = tech
    return "/".join(dir_parts).rstrip("/")

def generate_rule_url(src: str, rule: str) -> str:
    return f"https://github.com/pakaremon/rust-mal/tree/master/web/package-analysis-web/package_analysis/src/yara/rules/{src}/{rule}"

def ignore_match(tags: List[str], ignore_tags: Dict[str, bool]) -> bool:
    return any(t in ignore_tags for t in tags)

def behavior_risk(ns: str, rule: str, tags: List[str]) -> int:
    risk = 1
    if third_party(ns):
        risk = 3
        src = ns.split("/")[1] if "/" in ns else ns
        if src in ["JPCERT", "YARAForge", "bartblaze", "huntress", "elastic"]:
            risk = 4
            if "generic" in ns.lower() or "generic" in rule.lower():
                risk = 3
        if "keyword" in ns.lower() or "keyword" in rule.lower():
            risk = 2
    if "combo/" in ns:
        risk = 2
    for tag in tags:
        if tag in Levels:
            return Levels[tag]
    return risk

def longest_unique(raw: List[str]) -> List[str]:
    if len(raw) <= 1:
        return raw
    safe = list(raw)
    safe.sort(key=len, reverse=True)
    longest = []
    seen = set()
    for s in safe:
        if not s or s in seen:
            continue
        is_longest = True
        for o in longest:
            if o.find(s) != -1:
                is_longest = False
                break
        if is_longest:
            longest.append(s)
            seen.add(s)
    return longest

def match_to_string(rule_name: str, m: str) -> str:
    if contains_unprintable(m.encode()):
        return rule_name
    if "base64" in rule_name or "xor" in rule_name:
        return f"{rule_name}::{m}"
    if "xml_key_val" in rule_name:
        return m.replace("<key>", "").replace("</key>", "").strip()
    return m.strip()

def match_strings(rule_name: str, ms: List[str]) -> List[str]:
    if not ms:
        return []
    raw = [match_to_string(rule_name, m) for m in ms if match_to_string(rule_name, m)]
    return longest_unique(raw)

def size_and_checksum(fc: bytes) -> tuple[int, str]:
    if fc:
        size = len(fc)
        checksum = hashlib.sha256(fc).hexdigest()
        return size, checksum
    return 0, ""

def fix_url(s: str) -> str:
    return s.replace(" ", "%20")

def munge_description(s: str) -> str:
    m = threatHuntingKeywordRe.match(s)
    if m:
        return f"references '{m.group(1)}' tool"
    return s

def trim_prefixes(path: str, prefixes: List[str]) -> str:
    for prefix in prefixes:
        if not prefix:
            continue
        prefix = prefix.lstrip("./")
        if path.startswith(prefix):
            trimmed = path[len(prefix):]
            return trimmed.lstrip(os.sep)
    return path

def all_true(*conditions) -> bool:
    return all(conditions)

def contains_unprintable(b: bytes) -> bool:
    return any(c < 32 or c > 126 for c in b)

# --- Main functions ---
def generate(ctx, path: str, mrs: YaraScanResults, c: Config, expath: str, logger: Logger, fc: bytes) -> FileReport:
    """Main function to generate a file report from YARA scan results."""
    if ctx and hasattr(ctx, 'err') and ctx.err():
        return FileReport()
    
    if mrs is None:
        raise ValueError("scan failed")
    
    ignore_tags = c.IgnoreTags
    min_score = c.MinRisk
    ignore_self = c.IgnoreSelf
    
    ignore = {t: True for t in ignore_tags}
    
    size, checksum = size_and_checksum(fc)
    
    display_path = path
    if c.OCI:
        display_path = path[len(expath):] if path.startswith(expath) else path
    if c.TrimPrefixes:
        display_path = trim_prefixes(display_path, c.TrimPrefixes)
    
    match_count = len(mrs.MatchingRules())
    fr = FileReport()
    fr.Path = display_path
    fr.SHA256 = checksum
    fr.Size = size
    fr.Meta = {}
    fr.Behaviors = []
    fr.Overrides = []
    
    pledges = []
    caps = []
    syscalls = []
    
    ignore_malcontent = False
    key = ""
    overall_risk_score = 0
    risk = 0
    risk_counts = {}
    
    highest_risk = highest_match_risk(mrs)
    # Store match rules in a map for future override operations
    mrs_map = {m.Identifier(): m for m in mrs.MatchingRules()}
    
    for m in mrs.MatchingRules():
        if all_true(m.Identifier() == NAME, ignore_self):
            ignore_malcontent = True
        
        override = "override" in m.Tags()
        
        risk = behavior_risk(m.Namespace(), m.Identifier(), m.Tags())
        overall_risk_score = max(overall_risk_score, risk)
        risk_counts[risk] = risk_counts.get(risk, 0) + 1
        
        # The malcontent rule is classified as harmless
        # A !ignoreMalcontent condition will prevent the rule from being filtered
        # If running a scan as opposed to an analyze,
        # drop any matches that fall below the highest risk
        if risk == -1:
            continue
        if risk < min_score and not ignore_malcontent and not override:
            continue
        if c.Scan and risk < highest_risk and not ignore_malcontent and not override:
            continue
        
        key = generate_key(m.Namespace(), m.Identifier())
        rule_url = generate_rule_url(m.Namespace(), m.Identifier())
        
        # Process matched strings
        matched_strings = []
        total_matches = sum(len(p.Matches()) for p in m.Patterns())
        matches = []
        for p in m.Patterns():
            matches.extend(p.Matches())
        
        # Use the match processor from strings.py
        from strings import MatchProcessor
        processor = MatchProcessor(fc, matches, m.Patterns())
        matched_strings = processor.process()
        
        b = Behavior()
        b.ID = key
        b.MatchStrings = match_strings(m.Identifier(), matched_strings)
        b.RiskLevel = RiskLevels[risk]
        b.RiskScore = risk
        b.RuleName = m.Identifier()
        b.RuleURL = rule_url
        
        k = ""
        v = ""
        
        for meta in m.Metadata():
            k = meta.Identifier()
            v = str(meta.Value())
            # Empty data is unusual, so just ignore it.
            if not k or not v:
                continue
            
            # If we find a match in the map for the metadata key, that's the rule to override
            # Store this rule (the override) in the fr.Overrides behavior slice
            # If an override rule is not overriding a valid rule, log an error
            exists = k in mrs_map
            if exists and override:
                override_sev = Levels.get(v, 0)
                b.RiskLevel = RiskLevels[override_sev]
                b.RiskScore = override_sev
                b.Override.append(k)
                fr.Overrides.append(b)
            elif not exists and override:
                # TODO: return error if override references an unknown rule name
                continue
            
            if k == "author":
                b.RuleAuthor = v
                m = authorWithURLRe.match(v)
                if m and is_valid_url(m.group(2)):
                    b.RuleAuthor = m.group(1)
                    b.RuleAuthorURL = m.group(2)
                # If author is in @username format, strip @ to avoid constantly pinging them on GitHub
                if b.RuleAuthor.startswith("@"):
                    b.RuleAuthor = b.RuleAuthor.replace("@", "", 1)
            elif k == "author_url":
                b.RuleAuthorURL = v
            elif k == f"__{NAME}__":
                if v == "true":
                    fr.IsMalcontent = True
            elif k == "license":
                b.RuleLicense = v
            elif k == "license_url":
                b.RuleLicenseURL = v
            elif k in ["description", "threat_name", "name"]:
                desc = munge_description(v)
                if len(desc) > len(b.Description):
                    b.Description = desc
            elif k in ["ref", "reference"]:
                u = fix_url(v)
                if is_valid_url(u):
                    b.ReferenceURL = u
            elif k == "source_url":
                # YARAforge forgets to encode spaces
                b.RuleURL = fix_url(v)
            elif k == "pledge":
                pledges.append(v)
            elif k == "syscall":
                sy = v.split(",")
                syscalls.extend(sy)
            elif k == "cap":
                caps.append(v)
        
        # Fix YARA Forge rules that record their author URL as reference URLs
        if b.RuleURL.startswith(b.ReferenceURL):
            b.RuleAuthorURL = b.ReferenceURL
            b.ReferenceURL = ""
        
        # Meta names are weird and unfortunate, depending on whether they hold a value
        if key.startswith("meta/"):
            k = os.path.dirname(key).replace("meta/", "")
            v = os.path.basename(key)
            fr.Meta[k] = v
            continue
        
        if ignore_match(m.Tags(), ignore):
            fr.FilteredBehaviors += 1
            continue
        
        # If the rule does not have a description, make one up based on the rule name
        if not b.Description:
            b.Description = m.Identifier().replace("_", " ")
        
        existing_index = -1
        for i, existing in enumerate(fr.Behaviors):
            if existing.ID == key:
                existing_index = i
                break
        
        # If the existing description is longer and the priority is the same or lower
        if existing_index != -1:
            if fr.Behaviors[existing_index].RiskScore < b.RiskScore:
                fr.Behaviors[existing_index:existing_index+1] = [b]
            if (len(fr.Behaviors[existing_index].Description) < len(b.Description) and 
                fr.Behaviors[existing_index].RiskScore <= b.RiskScore):
                fr.Behaviors[existing_index].Description = b.Description
        else:
            fr.Behaviors.append(b)
    
    # Update the behaviors to account for overrides
    fr.Behaviors = handle_overrides(fr.Behaviors, fr.Overrides, min_score)
    
    # Adjust the overall risk if we deviated from overall_risk_score
    # Scans will still need to drop <= medium results
    new_risk = highest_behavior_risk(fr)
    if overall_risk_score != new_risk:
        overall_risk_score = new_risk
    
    if c.Scan and overall_risk_score < HIGH:
        fr.Skipped = "overall risk too low for scan"
    
    # Check for both the full and shortened variants of malcontent
    is_mal_binary = (os.path.basename(path) == NAME or os.path.basename(path) == "mal")
    
    if all_true(ignore_self, fr.IsMalcontent, ignore_malcontent, is_mal_binary):
        fr.Skipped = "ignoring malcontent binary"
    
    # If something has a lot of high, it's probably critical
    if c.QuantityIncreasesRisk and upgrade_risk(ctx, overall_risk_score, risk_counts, size):
        overall_risk_score = 4
    
    pledges.sort()
    syscalls.sort()
    caps.sort()
    fr.Pledge = list(dict.fromkeys(pledges))  # Remove duplicates while preserving order
    fr.Syscalls = list(dict.fromkeys(syscalls))
    fr.Capabilities = list(dict.fromkeys(caps))
    fr.RiskScore = overall_risk_score
    fr.RiskLevel = RiskLevels[fr.RiskScore]
    
    # Ensure that the behaviors are consistently sorted by ID
    fr.Behaviors.sort(key=lambda x: x.ID)
    
    return fr

def upgrade_risk(ctx, risk_score: int, risk_counts: Dict[int, int], size: int) -> bool:
    """Determine whether to upgrade risk based on finding density."""
    if risk_score != 3:
        return False
    high_count = risk_counts.get(3, 0)
    size_mb = size // 1024 // 1024
    upgrade = False
    
    if size < 1024 and high_count > 1:
        upgrade = True
    elif size_mb < 2 and high_count > 2:
        upgrade = True
    elif size_mb < 4 and high_count > 3:
        upgrade = True
    elif size_mb < 10 and high_count > 4:
        upgrade = True
    elif size_mb < 20 and high_count > 5:
        upgrade = True
    elif high_count > 6:
        upgrade = True
    else:
        upgrade = False
    
    if logger:
        logger.debug(f"upgrading risk: high={high_count}, size={size}")
    return upgrade

def highest_match_risk(mrs: YaraScanResults) -> int:
    """Return the highest risk score from a slice of MatchRules."""
    if not mrs.MatchingRules():
        return 0
    
    highest_risk = 0
    for m in mrs.MatchingRules():
        risk = behavior_risk(m.Namespace(), m.Identifier(), m.Tags())
        highest_risk = max(highest_risk, risk)
    return highest_risk

def highest_behavior_risk(fr: FileReport) -> int:
    """Return the highest risk score from a slice of FileReport Behaviors."""
    if fr is None or not fr.Behaviors:
        return 0
    
    highest_risk = 0
    for b in fr.Behaviors:
        highest_risk = max(highest_risk, b.RiskScore)
    
    return highest_risk

def handle_overrides(original: List[Behavior], override: List[Behavior], min_score: int) -> List[Behavior]:
    """Modify the behavior slice based on the contents of the override slice."""
    behavior_map = {b.RuleName: b for b in original}
    
    for o in override:
        for ob in o.Override:
            if ob in behavior_map:
                behavior_map[ob].RiskLevel = o.RiskLevel
                behavior_map[ob].RiskScore = o.RiskScore
        # Delete the override rule from the behavior map
        if o.RuleName in behavior_map:
            del behavior_map[o.RuleName]
    
    modified = [b for b in behavior_map.values() if b.RiskScore >= min_score]
    return modified