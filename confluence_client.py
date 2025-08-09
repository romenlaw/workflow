#!/usr/bin/env python3
"""
Confluence API Client
A comprehensive Python client for accessing Confluence pages using Atlassian API key authentication.
"""

# Import required libraries
import urllib3
import time
from typing import Optional, Dict, Any, List, Union
import logging
from dataclasses import dataclass
from bs4 import BeautifulSoup

# Disable SSL warnings for corporate environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from enum import Enum
import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
cookies = {
    'JSESSIONID': os.getenv('JSESSIONID'),
    'atl-sticky-version': os.getenv('ATL_STICKY_VERSION'),
    'atl.xsrf.token': os.getenv('ATL_XSRF_TOKEN'),
    'atlassian.xsrf.token': os.getenv('ATLASSIAN_XSRF_TOKEN'),
    'tenant.session.token': os.getenv('TENANT_SESSION_TOKEN'),
    'ajs_anonymous_id': os.getenv('AJS_ANONYMOUS_ID')
}
base_url = "https://commbank.atlassian.net/wiki"
username = os.getenv("ATLASSIAN_LOGIN")
api_token = os.getenv("ATLASSIAN_API_KEY")

class ConnectionMethod(Enum):
    LOGIN = 1
    COOKIES = 2


@dataclass
class ConfluencePage:
    """Data class for Confluence page information"""
    id: str
    title: str
    space_key: str
    version: int
    content: Optional[str] = None
    url: Optional[str] = None
    created_date: Optional[str] = None
    modified_date: Optional[str] = None
    author: Optional[str] = None

class ConfluenceClient:
    """Comprehensive Confluence API client"""
    
    def __init__(self, base_url: str="https://commbank.atlassian.net/wiki", verify_ssl: bool = False,
                 connection_method=ConnectionMethod.COOKIES):
        print(f"got {connection_method=}")
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/rest/api"
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.verify = verify_ssl
        # Set proper headers for Atlassian Cloud API
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'ConfluenceClient/1.0',
            'X-Atlassian-Token': 'no-check'  # Helps avoid XSRF issues
        })

        if connection_method == ConnectionMethod.LOGIN:
            self.session.auth = (username, api_token)
        else: # use cookies
            cookies = {
                'JSESSIONID': os.getenv('JSESSIONID'),
                'atl-sticky-version': os.getenv('ATL_STICKY_VERSION'),
                'atl.xsrf.token': os.getenv('ATL_XSRF_TOKEN'),
                'atlassian.xsrf.token': os.getenv('ATLASSIAN_XSRF_TOKEN'),
                'tenant.session.token': os.getenv('TENANT_SESSION_TOKEN'),
                'ajs_anonymous_id': os.getenv('AJS_ANONYMOUS_ID')
            }
            self.session.cookies.update(cookies)
        
        # Test connection
        self._test_connection()
    
    def _test_connection(self) -> bool:
        """Test the API connection"""
        try:
            # response = self.session.get(f"{self.base_url}")
            response = self.session.get(f"{self.api_url}/space")
            response.raise_for_status()
            logger.info("✓ Successfully connected to Confluence API")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to connect to Confluence API: {e}")
            raise
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make API request with error handling and rate limiting"""
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            print(response.content)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                return self._make_request(method, endpoint, **kwargs)
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {method} {url} - {e}")
            raise
    
    def get_spaces(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get all spaces"""
        params = {'limit': limit, 'expand': 'description,homepage'}
        response = self._make_request('GET', 'space', params=params)
        return response.json().get('results', [])
    
    def get_space_content(self, space_key: str, content_type: str = 'page', limit: int = 100) -> List[Dict[str, Any]]:
        """Get content from a specific space"""
        params = {
            'spaceKey': space_key,
            'type': content_type,
            'limit': limit,
            'expand': 'version,space,body.storage,history.lastUpdated'
        }
        response = self._make_request('GET', 'content', params=params)
        return response.json().get('results', [])
    
    def get_page_by_id(self, page_id: str, expand: str = 'body.storage,version,space,history') -> Dict[str, Any]:
        """Get a specific page by ID"""
        params = {'expand': expand}
        response = self._make_request('GET', f'content/{page_id}', params=params)
        return response.json()
    
    def get_page_by_title(self, space_key: str, title: str) -> Optional[Dict[str, Any]]:
        """Get a page by title within a space"""
        params = {
            'spaceKey': space_key,
            'title': title,
            'expand': 'body.storage,version,space,history'
        }
        response = self._make_request('GET', 'content', params=params)
        results = response.json().get('results', [])
        return results[0] if results else None
    
    def search_title(self, query: str, limit: int = 50, content_type: str = 'page') -> List[Dict[str, Any]]:
        """Search for content using CQL (Confluence Query Language)"""
        cql = f'type="{content_type}" and (title ~ "{query}")'
        params = {
            'cql': cql,
            'limit': limit,
            'expand': 'content.body.storage,content.version,content.space'
        }
        response = self._make_request('GET', 'content/search', params=params)
        return response.json().get('results', [])
    
    def search_content(self, query: str, limit: int = 50, content_type: str = 'page') -> List[Dict[str, Any]]:
        """Search for content using CQL (Confluence Query Language)"""
        cql = f'type="{content_type}" and (text ~ "{query}")'
        params = {
            'cql': cql,
            'limit': limit,
            'expand': 'content.body.storage,content.version,content.space'
        }
        response = self._make_request('GET', 'content/search', params=params)
        return response.json().get('results', [])
    
    def get_page_children(self, page_id: str, content_type: str = 'page', limit: int=99) -> List[Dict[str, Any]]:
        """Get child pages of a specific page"""
        params = {
            'type': content_type,
            'expand': 'version,space,body.storage',
            'limit': limit
        }
        response = self._make_request('GET', f'content/{page_id}/child/{content_type}', params=params)
        return response.json().get('results', [])
    
    def extract_text_from_storage(self, storage_content: str) -> str:
        """Extract plain text from Confluence storage format"""
        if not storage_content:
            return ""
        
        # Parse HTML and extract text
        soup = BeautifulSoup(storage_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text and clean it up
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def get_page_as_object(self, page_id: str) -> ConfluencePage:
        """Get page data as a structured object"""
        page_data = self.get_page_by_id(page_id)
        
        # Extract content if available
        content = None
        if 'body' in page_data and 'storage' in page_data['body']:
            storage_content = page_data['body']['storage']['value']
            content = self.extract_text_from_storage(storage_content)
        
        # Extract dates
        created_date = None
        modified_date = None
        author = None
        
        if 'history' in page_data:
            history = page_data['history']
            if 'createdDate' in history:
                created_date = history['createdDate']

        if 'version' in page_data:
            version = page_data['version']
            if 'when' in version:
                modified_date = version['when']
            if 'by' in version:
                author = version['by']['displayName']
        
        return ConfluencePage(
            id=page_data['id'],
            title=page_data['title'],
            space_key=page_data['space']['key'],
            version=page_data['version']['number'],
            content=content,
            url=f"{self.base_url}{page_data['_links']['webui']}",
            created_date=created_date,
            modified_date=modified_date,
            author=author
        )
    
    def export_pages_to_text(self, pages: List[Dict[str, Any]], output_file: str = None) -> str:
        """Export multiple pages to a text format"""
        content_parts = []
        
        for page in pages:
            page_obj = self.get_page_as_object(page['id'])
            
            content_parts.append(f"{'='*80}")
            content_parts.append(f"Title: {page_obj.title}")
            content_parts.append(f"Space: {page_obj.space_key}")
            content_parts.append(f"URL: {page_obj.url}")
            content_parts.append(f"Last Modified: {page_obj.modified_date}")
            content_parts.append(f"Author: {page_obj.author}")
            content_parts.append(f"{'='*80}")
            content_parts.append(page_obj.content or "No content available")
            content_parts.append("\n")
        
        full_content = "\n".join(content_parts)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(full_content)
            logger.info(f"Content exported to {output_file}")
        
        return full_content
    
    def update_page(self, page_id: str, title: str = None, content: str = None,
                   content_format: str = 'storage', minor_edit: bool = False) -> Dict[str, Any]:
        """
        Update a Confluence page by page ID
        
        Args:
            page_id: The ID of the page to update
            title: New title for the page (optional, keeps existing if not provided)
            content: New content for the page (optional, keeps existing if not provided)
            content_format: Format of the content ('storage' or 'wiki')
            minor_edit: Whether this is a minor edit (doesn't send notifications)
            
        Returns:
            Dict containing updated page information
            
        Raises:
            Exception: If the page cannot be found or updated
        """
        try:
            # First, get the current page to retrieve version and other details
            current_page = self.get_page_by_id(page_id, expand='body.storage,version,space')
            
            # Prepare the update payload
            update_data = {
                'id': page_id,
                'type': 'page',
                'title': title or current_page['title'],
                'space': {
                    'key': current_page['space']['key']
                },
                'version': {
                    'number': current_page['version']['number'] + 1,
                    'minorEdit': minor_edit
                }
            }
            
            # Add content if provided
            if content is not None:
                update_data['body'] = {
                    content_format: {
                        'value': content,
                        'representation': content_format
                    }
                }
            elif 'body' in current_page and content_format in current_page['body']:
                # Keep existing content if no new content provided
                update_data['body'] = current_page['body']
            
            # Make the update request
            response = self._make_request('PUT', f'content/{page_id}', json=update_data)
            
            logger.info(f"✓ Successfully updated page '{update_data['title']}' (ID: {page_id})")
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise Exception(f"Page with ID {page_id} not found")
            elif e.response.status_code == 409:
                raise Exception(f"Version conflict when updating page {page_id}. Page may have been modified by another user.")
            else:
                raise Exception(f"Failed to update page {page_id}: {e}")
        except Exception as e:
            logger.error(f"✗ Failed to update page {page_id}: {e}")
            raise
    
    def update_page_content_html(self, page_id: str, html_content: str, title: str = None,
                                minor_edit: bool = False) -> Dict[str, Any]:
        """
        Update a Confluence page with HTML content
        
        Args:
            page_id: The ID of the page to update
            html_content: HTML content to set on the page
            title: New title for the page (optional)
            minor_edit: Whether this is a minor edit
            
        Returns:
            Dict containing updated page information
        """
        return self.update_page(
            page_id=page_id,
            title=title,
            content=html_content,
            content_format='storage',
            minor_edit=minor_edit
        )
    
    def append_to_page(self, page_id: str, additional_content: str,
                      content_format: str = 'storage') -> Dict[str, Any]:
        """
        Append content to an existing Confluence page
        
        Args:
            page_id: The ID of the page to update
            additional_content: Content to append to the page
            content_format: Format of the content ('storage' or 'wiki')
            
        Returns:
            Dict containing updated page information
        """
        try:
            # Get current page content
            current_page = self.get_page_by_id(page_id, expand='body.storage')
            
            # Get existing content
            existing_content = ""
            if 'body' in current_page and content_format in current_page['body']:
                existing_content = current_page['body'][content_format]['value']
            
            # Combine existing and new content
            combined_content = existing_content + "\n" + additional_content
            
            # Update the page
            return self.update_page(
                page_id=page_id,
                content=combined_content,
                content_format=content_format,
                minor_edit=True
            )
            
        except Exception as e:
            logger.error(f"✗ Failed to append to page {page_id}: {e}")
            raise


# utilities
confluence = ConfluenceClient()
def list_recent_pages(space_key: str = None, days: int = 30) -> List[Dict[str, Any]]:
    """List recently updated pages"""
    from datetime import datetime, timedelta
    
    # Calculate date threshold
    threshold_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    # Build CQL query
    cql = f'type="page" and lastModified >= "{threshold_date}"'
    if space_key:
        cql += f' and space="{space_key}"'
    
    try:
        params = {
            'cql': cql,
            'limit': 50,
            'expand': 'content.version,content.space,content.history.lastUpdated'
        }
        response = confluence._make_request('GET', 'content/search', params=params)
        return response.json().get('results', [])
    except Exception as e:
        logger.error(f"Error getting recent pages: {e}")
        return []

def get_page_tree(root_page_id: str, max_depth: int = 3) -> Dict[str, Any]:
    """Get a hierarchical tree of pages starting from a root page"""
    def build_tree(page_id: str, current_depth: int = 0) -> Dict[str, Any]:
        if current_depth >= max_depth:
            return {}
        
        try:
            page = confluence.get_page_by_id(page_id, expand='version,space')
            children = confluence.get_page_children(page_id)
            
            tree = {
                'id': page['id'],
                'title': page['title'],
                'space': page['space']['key'],
                'url': f"{base_url}{page['_links']['webui']}",
                'children': []
            }
            
            for child in children:
                child_tree = build_tree(child['id'], current_depth + 1)
                if child_tree:
                    tree['children'].append(child_tree)
            
            return tree
            
        except Exception as e:
            logger.error(f"Error building tree for page {page_id}: {e}")
            return {}
    
    return build_tree(root_page_id)

def print_page_tree(tree: Dict[str, Any], indent: int = 0) -> None:
    """Print a page tree in a readable format"""
    if not tree:
        return
    
    prefix = "  " * indent + ("├─ " if indent > 0 else "")
    print(f"{prefix}{tree['title']} ({tree['space']})")
    
    for child in tree.get('children', []):
        print_page_tree(child, indent + 1)

