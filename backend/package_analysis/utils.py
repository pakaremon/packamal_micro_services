"""
Utility functions for package analysis
"""
import re
from urllib.parse import unquote
from typing import Dict, Optional, Tuple


class PURLParser:
    """
    Parser for Package URLs (PURL) as defined in the purl-spec
    Reference: https://github.com/package-url/purl-spec/tree/505dca561f6d6f1f1f0ebb6b5c36c6aa2516d98d
    """
    
    SUPPORTED_ECOSYSTEMS = {
        'pypi': 'pypi',
        'npm': 'npm', 
        'gem': 'rubygems',
        'maven': 'maven',
        'packagist': 'packagist'
    }
    
    @classmethod
    def parse_purl(cls, purl: str) -> Dict[str, Optional[str]]:
        """
        Parse a Package URL string and extract components
        
        Args:
            purl: Package URL string (e.g., "pkg:pypi/django@1.11.1")
            
        Returns:
            Dict with keys: ecosystem, namespace, name, version, qualifiers
        """
        if not purl or not purl.startswith('pkg:'):
            raise ValueError("Invalid PURL: must start with 'pkg:'")
        
        # Remove 'pkg:' prefix
        purl_part = purl[4:]
        
        # Split ecosystem and rest
        if '/' not in purl_part:
            raise ValueError("Invalid PURL: missing ecosystem separator")
        
        ecosystem_part, rest = purl_part.split('/', 1)
        
        # Parse ecosystem
        ecosystem = cls.SUPPORTED_ECOSYSTEMS.get(ecosystem_part)
        if not ecosystem:
            raise ValueError(f"Unsupported ecosystem: {ecosystem_part}")
        
        # Parse namespace/name@version?qualifiers
        namespace = None
        name = None
        version = None
        qualifiers = {}
        
        # Check for qualifiers (after ?)
        if '?' in rest:
            rest, qualifiers_part = rest.split('?', 1)
            qualifiers = cls._parse_qualifiers(qualifiers_part)
        
        # Check for version (after @)
        if '@' in rest:
            name_part, version = rest.split('@', 1)
            version = unquote(version)
        else:
            name_part = rest
        
        # Parse namespace/name
        if '/' in name_part:
            namespace, name = name_part.split('/', 1)
            namespace = unquote(namespace)
            name = unquote(name)
        else:
            name = unquote(name_part)
        
        # Handle special cases for different ecosystems
        if ecosystem == 'maven':
            # For Maven, namespace is groupId, name is artifactId
            if namespace and name:
                # If both exist, combine them for the package name
                package_name = f"{namespace}:{name}"
            else:
                package_name = name or namespace
            name = package_name
            namespace = None
        
        return {
            'ecosystem': ecosystem,
            'namespace': namespace,
            'name': name,
            'version': version,
            'qualifiers': qualifiers,
            'original_purl': purl
        }
    
    @classmethod
    def _parse_qualifiers(cls, qualifiers_str: str) -> Dict[str, str]:
        """Parse qualifiers string into a dictionary"""
        qualifiers = {}
        if qualifiers_str:
            for pair in qualifiers_str.split('&'):
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    qualifiers[unquote(key)] = unquote(value)
        return qualifiers
    
    @classmethod
    def extract_package_info(cls, purl: str) -> Tuple[str, str, str]:
        """
        Extract package name, version, and ecosystem from PURL
        
        Returns:
            Tuple of (package_name, package_version, ecosystem)
        """
        parsed = cls.parse_purl(purl)
        
        package_name = parsed['name']
        if parsed['namespace']:
            # For ecosystems that use namespace, include it in package name
            if parsed['ecosystem'] == 'npm':
                # For npm, namespace is scope (e.g., @angular/core)
                package_name = f"{parsed['namespace']}/{package_name}"
        
        return (
            package_name or '',
            parsed['version'] or '',
            parsed['ecosystem']
        )


def validate_purl_format(purl: str) -> bool:
    """
    Validate if a string is a properly formatted PURL
    """
    try:
        PURLParser.parse_purl(purl)
        return True
    except (ValueError, IndexError):
        return False


def get_ecosystem_from_purl(purl: str) -> Optional[str]:
    """
    Get ecosystem from PURL without full parsing
    """
    if not purl or not purl.startswith('pkg:'):
        return None
    
    try:
        ecosystem_part = purl[4:].split('/')[0]
        return PURLParser.SUPPORTED_ECOSYSTEMS.get(ecosystem_part)
    except (IndexError, ValueError):
        return None



