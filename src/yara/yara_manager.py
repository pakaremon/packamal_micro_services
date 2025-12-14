import os
import yara
from pathlib import Path
import string

class YaraRuleManager:
    def __init__(self):
        self.rules = None
        self.load_rules()
    
    def load_rules(self):
        rules_dir = os.path.join(os.path.dirname(__file__), 'rules')
        filepaths = {}
        for sub_dir in Path(rules_dir).glob('*'):
            for rule_file in sub_dir.glob('*.yar'):
                namespace = rule_file.stem  # or use rule_file.name for uniqueness
                # print(f"namespace: {namespace}")
                # print(f"rule_file: {rule_file}")
                # namespace is the first part folder @rules/namespace/rule.yar
                folder_name = sub_dir.name
                filepaths[f"{folder_name}@{namespace}"] = str(rule_file)
        if filepaths:
            try:
                self.rules = yara.compile(filepaths=filepaths)
            except yara.SyntaxError as e:
                print(f"YARA syntax error: {e}")
            except Exception as e:
                print(f"Error loading rules: {e}")
        else:
            print("No YARA rule files found.")
    
    def analyze_behavior(self, analysis_data):
        if not self.rules:
            return []
        try:
            # print("success load rules")
            return self.rules.match(data=analysis_data)
        except Exception as e:
            print(f"Error matching rules: {e}")
            return []
        
class ReportYara:
    @staticmethod
    def is_printable(s):
        """Check if a string contains only printable characters."""
        return all(c in string.printable for c in s)

    @staticmethod
    def extract_evidence(match, data):
        # https://github.com/VirusTotal/yara-python

        '''
        >>> import yara
        >>> rule = yara.compile(source='rule foo: bar {strings: $a = "lmn" condition: $a}')
        >>> matches = rule.match(data='abcdefgjiklmnoprstuvwxyz')
        >>> print(matches)
        [foo]
        >>> print(matches[0].rule)
        foo
        >>> print(matches[0].tags)
        ['bar']
        >>> print(matches[0].strings)
        [$a]
        >>> print(matches[0].strings[0].identifier)
        $a
        >>> print(matches[0].strings[0].instances)
        [lmn]
        >>> print(matches[0].strings[0].instances[0].offset)
        10
        >>> print(matches[0].strings[0].instances[0].matched_length)
        '''
        evidences = []
        for string in match.strings:
            identifier = string.identifier
            instances = string.instances
            # print(f"len instances: {len(instances)}")
            for instance in instances[:3]:
                offset = instance.offset
                matched_length = instance.matched_length
                # get data from offset to offset + matched_length
                evidence = data[offset:offset+matched_length]
                # check if evidence is printable
                if ReportYara.is_printable(evidence):
                    # evidence = evidence.decode('utf-8')
                    # DECODE STRING Yara analysis error: 'str' object has no attribute 'decode'
                    # check if bytes
                    if isinstance(evidence, bytes):
                        evidence = evidence.decode('utf-8')
                        evidences.append(evidence)
                    else:
                        evidences.append(evidence)
                


        return list(set(evidences))
    
    @staticmethod
    def generate_rule_url(src: str, rule: str) -> str:
        parts = src.split("@")
        folder_name = parts[0]
        file_name = parts[1]
        return f"https://github.com/pakaremon/rust-mal/tree/master/web/package-analysis-web/package_analysis/src/yara/rules/{folder_name}/{file_name}.yar"


