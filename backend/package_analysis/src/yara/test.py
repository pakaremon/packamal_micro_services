import yara

# Assume 'rules' object from 3.1 is available, or re-compile
yara_rule_string = """
rule basic_string_match {
    strings:
        $a = "This is a test string for Yara." ascii
        $b = "SECRET_PHRASE" nocase
    condition:
        $a and $b
}
rule hex_pattern_match {
    strings:
        $h = { 4D 5A 90 00 03 00 00 00 } // Typical PE header bytes
    condition:
        $h
}
"""
rules = yara.compile(source=yara_rule_string)

# Data as bytes
data_to_scan = b"Some preamble.This is a test string for Yara.\x00\x00SECRET_PHRASE. More data."
data_with_hex = b"\x4D\x5A\x90\x00\x03\x00\x00\x00 This is a Windows executable header."

print("\nScanning in-memory data (bytes):")
matches_data = rules.match(data=data_to_scan)


if matches_data:
    print("Matches found in first data block:")
    for match in matches_data:
        print(f"  Rule: {match.rule}")
        for s in match.strings:
            # print data structure of s
            # print(f"  Data structure of s: {dir(s)}")
            # print(f"  identifier: {s.identifier}")
            # print(f"instance of s: {s.instances}")
            # s.data is bytes, decode it for printing if it's text, or use .hex() for binary
            display_data = s.data.decode('latin-1', errors='ignore') if s.data.isascii() else s.data.hex()
            print(f"    - Offset: {s.offset}, Identifier: {s.identifier}, Data: {display_data}")
else:
    print("No matches found in first data block.")

matches_hex_data = rules.match(data=data_with_hex)
if matches_hex_data:
    print("\nMatches found in data block with hex pattern:")
    for match in matches_hex_data:
        print(f"  Rule: {match.rule}")
        for s in match.strings:
            display_data = s.data.decode('latin-1', errors='ignore') if s.data.isascii() else s.data.hex()
            print(f"    - Offset: {s.offset}, Identifier: {s.identifier}, Data: {display_data}")
else:
    print("No matches found in data block with hex pattern.")
