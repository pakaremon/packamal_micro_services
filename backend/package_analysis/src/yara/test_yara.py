# from ..src.yara.yara_manager import YaraRuleManager
# /web/package-analysis-web/package_analysis/test/test_yara.py", line 1, in <module>
#     from ..src.yara.yara_manager import YaraRuleManager
# ImportError: attempted relative import with no known parent package



import json
import os
import re

from typing import List, Dict, Optional
from dataclasses import dataclass, field

file_path = "solana-web3.js.json"
from yara_manager import YaraRuleManager

import string
import yara
import codecs

def is_printable(s):
    """Check if a string contains only printable characters."""
    return all(c in string.printable for c in s)


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
        for instance in instances[:5]:
            offset = instance.offset
            matched_length = instance.matched_length
            # get data from offset to offset + matched_length
            evidence = data[offset:offset+matched_length]
            # check if evidence is printable
            if is_printable(evidence):
                # evidence = evidence.decode('utf-8')
                # DECODE STRING Yara analysis error: 'str' object has no attribute 'decode'
                # check if bytes
                if isinstance(evidence, bytes):
                    evidence = evidence.decode('utf-8')
                    evidences.append(evidence)
                else:
                    evidences.append(evidence)
            


    return list(set(evidences))

def generate_rule_url(src: str, rule: str) -> str:
    parts = src.split("@")
    folder_name = parts[0]
    file_name = parts[1]
    return f"https://github.com/pakaremon/rust-mal/tree/master/web/package-analysis-web/package_analysis/src/yara/rules/{folder_name}/{file_name}.yar"


with open(file_path, 'r') as f:
    data = json.load(f)

class Report:

    @staticmethod
    def generate_report(json_data):
        # Initialize lists for commands, domains, and system calls
        commands = []
        domains = []
        system_calls = []

        # Process install phase
        install_phase = json_data.get('Analysis', {}).get('install', {})
        
        # Process commands
        for command in install_phase.get('Commands', []) or []:
            if command is not None:
                cmd = command.get('Command')
                if cmd:
                    # If cmd is a list, join it with spaces
                    if isinstance(cmd, list):
                        cmd = ' '.join(cmd)
                    commands.append({
                        'command': cmd,
                        'rules': []  # Will be populated by Yara analysis
                    })

        # Process DNS entries
        for dns in install_phase.get('DNS', []) or []:
            if dns is not None:
                for query in dns.get('Queries', []):
                    hostname = query.get('Hostname')
                    if hostname:
                        domains.append({
                            'domain': hostname,
                            'rules': []  # Will be populated by Yara analysis
                        })

        # Process system calls
        pattern = re.compile(r'^Enter:\s*(.*)')
        for syscall in install_phase.get('Syscalls', []):
            if syscall is not None:
                match = pattern.match(syscall)
                if match:
                    syscall_name = match.group(1)
                    system_calls.append({
                        'system_call': syscall_name,
                        'rules': []  # Will be populated by Yara analysis
                    })

        # Process execution phase
        execution_phase = json_data.get('Analysis', {}).get('execute', {})
        if not execution_phase:
            execution_phase = json_data.get('Analysis', {}).get('import', {})

        # Process commands from execution phase
        for command in execution_phase.get('Commands', []) or []:
            if command is not None:
                cmd = command.get('Command')
                if cmd:
                    # If cmd is a list, join it with spaces
                    if isinstance(cmd, list):
                        cmd = ' '.join(cmd)
                    commands.append({
                        'command': cmd,
                        'rules': []  # Will be populated by Yara analysis
                    })

        # Process DNS entries from execution phase
        for dns in execution_phase.get('DNS') or []:
            if dns is not None:
                for query in dns.get('Queries', []):
                    hostname = query.get('Hostname')
                    if hostname:
                        domains.append({
                            'domain': hostname,
                            'rules': []  # Will be populated by Yara analysis
                        })

        # Process system calls from execution phase
        for syscall in execution_phase.get('Syscalls', []):
            if syscall is not None:
                match = pattern.match(syscall)
                if match:
                    syscall_name = match.group(1)
                    system_calls.append({
                        'system_call': syscall_name,
                        'rules': []  # Will be populated by Yara analysis
                    })

        # Add Yara analysis
        try:
            yara_manager = YaraRuleManager()
           
        
            
            # Analyze commands
            command_text = '\n'.join([cmd['command'] for cmd in commands])
            command_matches = yara_manager.analyze_behavior(command_text)
            
            # Analyze domains
            domain_text = '\n'.join([domain['domain'] for domain in domains])
            network_matches = yara_manager.analyze_behavior(domain_text)
            
            # Analyze system calls
            syscall_text = '\n'.join([syscall['system_call'] for syscall in system_calls])
            syscall_matches = yara_manager.analyze_behavior(syscall_text)
            
            # analyze files
            files_text = '\n'.join([file for file in install_phase.get('files', {}).get('read', []) + execution_phase.get('files', {}).get('read', []) + install_phase.get('files', {}).get('write', []) + execution_phase.get('files', {}).get('write', []) + install_phase.get('files', {}).get('delete', []) + execution_phase.get('files', {}).get('delete', [])])
            files_matches = yara_manager.analyze_behavior(files_text)

            # Add Yara results to commands
            for match in command_matches:
                rule = {
                    'name': match.rule,
                    'description': match.meta['description'],
                    'severity': 'high',  # You might want to add severity from your Yara rules
                    'strings': [str(s) for s in match.strings],
                    'evidence': extract_evidence(match, command_text),
                    'url': generate_rule_url(match.namespace, match.rule),
                }
               
                print(rule)
                for cmd in commands:
                    if any(str(s) in cmd['command'] for s in match.strings):
                        cmd['rules'].append(rule)

            # Add Yara results to domains
            for match in network_matches:
                rule = {
                    'name': match.rule,
                    'description': match.meta['description'],
                    'severity': 'high',  # You might want to add severity from your Yara rules
                    'strings': [str(s) for s in match.strings],
                    'evidence': extract_evidence(match, domain_text),
                    'url': generate_rule_url(match.namespace, match.rule)
                }
                print(rule)
                for domain in domains:
                    if any(str(s) in domain['domain'] for s in match.strings):
                        domain['rules'].append(rule)

            # Add Yara results to system calls
            for match in syscall_matches:
                rule = {
                    'name': match.rule,
                    'description': match.meta['description'],
                    'severity': 'high',  # You might want to add severity from your Yara rules
                    'strings': [str(s) for s in match.strings],
                    'evidence': extract_evidence(match, syscall_text),
                    'url': generate_rule_url(match.namespace, match.rule)
                }
                # print(rule)
                for syscall in system_calls:
                    if any(str(s) in syscall['system_call'] for s in match.strings):
                        syscall['rules'].append(rule)

            # Add Yara results to files
            for match in files_matches:
                rule = {
                    'name': match.rule,
                    'description': match.meta['description'],
                    'severity': 'high',  # You might want to add severity from your Yara rules
                    'strings': [str(s) for s in match.strings],
                    'evidence': extract_evidence(match, files_text),
                    'url': generate_rule_url(match.namespace, match.rule)
                }
                # print(rule)
                for file in files_text:
                    if any(str(s) in file['file'] for s in match.strings):
                        file['rules'].append(rule)

        except Exception as e:
            print(f"Yara analysis error: {e}")

        # Return data in the format that matches the Report class
        return {
            'commands': commands,
            'domains': domains,
            'system_calls': system_calls
        }
    
    


# only print the commands that have rules
report = Report.generate_report(data)
# print evidence for each command
# print(report['commands'])


# create a class report
# in each command, domain, system call, there is a list of matching rules
# each rule has a name, description, severity, and a list of strings that matched

@dataclass
class Rule:
    name: str
    description: str
    severity: str
    strings: List[str]

@dataclass
class Command:
    command: str
    rules: List[Rule]

@dataclass
class Domain:
    domain: str
    rules: List[Rule]

@dataclass
class SystemCall:
    system_call: str
    rules: List[Rule]

@dataclass
class AnalysisReport:
    commands: List[Command]
    domains: List[Domain]
    system_calls: List[SystemCall]

    def __init__(self, data: Dict):
        self.commands = []
        self.domains = []
        self.system_calls = []

        for command in data['commands']:
            command_obj = Command(command['command'], [])
            for rule in command['rules']:
                rule_obj = Rule(rule['name'], rule['description'], rule['severity'], rule['strings'])
                command_obj.rules.append(rule_obj)
            self.commands.append(command_obj)

        for domain in data['domains']:
            domain_obj = Domain(domain['domain'], [])
            for rule in domain['rules']:
                rule_obj = Rule(rule['name'], rule['description'], rule['severity'], rule['strings'])
                domain_obj.rules.append(rule_obj)
            self.domains.append(domain_obj)

        for system_call in data['system_calls']:
            system_call_obj = SystemCall(system_call['system_call'], [])
            for rule in system_call['rules']:
                rule_obj = Rule(rule['name'], rule['description'], rule['severity'], rule['strings'])
                system_call_obj.rules.append(rule_obj)
            self.system_calls.append(system_call_obj)

        # calculate the overall severity
        self.overall_severity = self.calculate_overall_severity()

    def calculate_overall_severity(self):
        # calculate the overall severity
        # the overall severity is the highest severity of all the rules
        return max(rule.severity for command in self.commands for rule in command.rules)




