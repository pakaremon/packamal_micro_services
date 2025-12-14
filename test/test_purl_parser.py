#!/usr/bin/env python3
"""
Test script for PURL parser functionality
"""

import sys
import os

# Add the package_analysis directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'package_analysis'))

from package_analysis.utils import PURLParser, validate_purl_format


def test_purl_examples():
    """Test PURL parsing with the provided examples"""
    
    test_cases = [
        # PyPI
        ("pkg:pypi/django@1.11.1", "django", "1.11.1", "pypi"),
        
        # npm
        ("pkg:npm/%40angular/animation@12.3.1", "@angular/animation", "12.3.1", "npm"),
        ("pkg:npm/foobar@12.3.1", "foobar", "12.3.1", "npm"),
        
        # RubyGems
        ("pkg:gem/jruby-launcher@1.1.2?platform=java", "jruby-launcher", "1.1.2", "rubygems"),
        ("pkg:gem/ruby-advisory-db-check@0.12.4", "ruby-advisory-db-check", "0.12.4", "rubygems"),
        
        # Maven
        ("pkg:maven/org.apache.xmlgraphics/batik-anim@1.9.1?packaging=sources", 
         "org.apache.xmlgraphics:batik-anim", "1.9.1", "maven"),
        ("pkg:maven/org.apache.xmlgraphics/batik-anim@1.9.1?repository_url=repo.spring.io/release",
         "org.apache.xmlgraphics:batik-anim", "1.9.1", "maven"),
    ]
    
    print("Testing PURL Parser")
    print("=" * 50)
    
    all_passed = True
    
    for purl, expected_name, expected_version, expected_ecosystem in test_cases:
        print(f"\nTesting: {purl}")
        
        try:
            # Test validation
            is_valid = validate_purl_format(purl)
            print(f"  Validation: {'✓' if is_valid else '✗'}")
            
            if not is_valid:
                print(f"  ERROR: PURL validation failed")
                all_passed = False
                continue
            
            # Test parsing
            package_name, package_version, ecosystem = PURLParser.extract_package_info(purl)
            
            # Check results
            name_ok = package_name == expected_name
            version_ok = package_version == expected_version
            ecosystem_ok = ecosystem == expected_ecosystem
            
            print(f"  Package Name: {package_name} {'✓' if name_ok else '✗'} (expected: {expected_name})")
            print(f"  Version: {package_version} {'✓' if version_ok else '✗'} (expected: {expected_version})")
            print(f"  Ecosystem: {ecosystem} {'✓' if ecosystem_ok else '✗'} (expected: {expected_ecosystem})")
            
            if not (name_ok and version_ok and ecosystem_ok):
                all_passed = False
            
            # Test full parsing
            parsed = PURLParser.parse_purl(purl)
            print(f"  Full parsing: ✓")
            print(f"    Namespace: {parsed.get('namespace', 'None')}")
            print(f"    Qualifiers: {parsed.get('qualifiers', {})}")
            
        except Exception as e:
            print(f"  ERROR: {e}")
            all_passed = False
    
    print("\n" + "=" * 50)
    print(f"Test Results: {'ALL PASSED' if all_passed else 'SOME FAILED'}")
    
    return all_passed


def test_invalid_purls():
    """Test invalid PURL formats"""
    
    invalid_purls = [
        "not-a-purl",
        "pkg:",
        "pkg:invalid-ecosystem/package@1.0.0",
        "pkg:pypi/",  # Missing package name
        "pkg:pypi/package@",  # Missing version
    ]
    
    print("\nTesting Invalid PURLs")
    print("=" * 30)
    
    all_passed = True
    
    for purl in invalid_purls:
        print(f"\nTesting invalid PURL: {purl}")
        
        try:
            is_valid = validate_purl_format(purl)
            if is_valid:
                print(f"  ERROR: Should be invalid but validation passed")
                all_passed = False
            else:
                print(f"  ✓ Correctly identified as invalid")
                
        except Exception as e:
            print(f"  ✓ Correctly threw exception: {e}")
    
    return all_passed


if __name__ == "__main__":
    print("PURL Parser Test Suite")
    print("=" * 50)
    
    # Test valid PURLs
    valid_test_passed = test_purl_examples()
    
    # Test invalid PURLs
    invalid_test_passed = test_invalid_purls()
    
    print("\n" + "=" * 50)
    print("FINAL RESULTS:")
    print(f"Valid PURL tests: {'PASSED' if valid_test_passed else 'FAILED'}")
    print(f"Invalid PURL tests: {'PASSED' if invalid_test_passed else 'FAILED'}")
    
    if valid_test_passed and invalid_test_passed:
        print("\n🎉 All tests passed! PURL parser is working correctly.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
        sys.exit(1)



