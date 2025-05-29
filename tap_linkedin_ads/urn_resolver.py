import logging
from abc import ABC, abstractmethod
from typing import Dict, Set, Optional, Any

LOGGER = logging.getLogger(__name__)

class URNResolver(ABC):
    """Base class for URN resolvers."""
    
    def __init__(self, client: Any, locale: Optional[str] = None):
        self.client = client
        self.locale = locale
        self._cache: Dict[str, str] = {}
    
    @abstractmethod
    def resolve(self, urns: Set[str]) -> Dict[str, str]:
        """Resolve URNs to their names."""
        pass
    
    def _extract_code(self, urn: str) -> str:
        """Extract the code from a URN."""
        return urn.split(':')[-1]
    
    def _get_cached_value(self, code: str) -> Optional[str]:
        """Get a value from cache if it exists."""
        return self._cache.get(code)
    
    def _add_to_cache(self, code: str, name: str) -> None:
        """Add a value to the cache."""
        self._cache[code] = name

class FunctionsResolver(URNResolver):
    """Resolver for job function URNs."""
    
    def resolve(self, urns: Set[str]) -> Dict[str, str]:
        """Resolve a set of function URNs to their names."""
        if not urns:
            return {}
            
        codes = {self._extract_code(urn) for urn in urns}
        return self._batch_resolve(codes)
        
    def _batch_resolve(self, codes: Set[str]) -> Dict[str, str]:
        """Resolve function codes in batches."""
        resolved = {}
        chunk_size = 150  # LinkedIn's batch limit
        
        # Split codes into chunks
        code_chunks = [list(codes)[i:i + chunk_size] for i in range(0, len(codes), chunk_size)]
        
        for chunk in code_chunks:
            try:
                # For functions, we need to use a different URL format
                locale_param = f"?locale={self.locale}" if self.locale else ""
                url = f"https://api.linkedin.com/v2/functions{locale_param}"
                
                LOGGER.info(f"Making request to URL: {url}")
                
                headers = {'X-Restli-Protocol-Version': '2.0.0'}
                response = self.client.get(url=url, endpoint="functions", headers=headers)
                
                if response and 'elements' in response:
                    for element in response['elements']:
                        if isinstance(element, dict):
                            code = str(element.get('id'))
                            if code in chunk:  # Only process codes we're interested in
                                name = element.get('name', {}).get('localized', {}).get('en_US', code)
                                resolved[code] = name
                                self._add_to_cache(code, name)
                            
            except Exception as e:
                if "429" in str(e):
                    LOGGER.warning("Rate limit hit while resolving function names. Using codes as fallback.")
                else:
                    LOGGER.warning(f"Failed to resolve function names: {str(e)}")
                
                # Add unresolved codes from this chunk
                for code in chunk:
                    if code not in resolved:
                        resolved[code] = code
        
        return resolved

class TitlesResolver(URNResolver):
    """Resolver for title URNs."""
    
    def resolve(self, urns: Set[str]) -> Dict[str, str]:
        if not urns:
            return {}
            
        codes = {self._extract_code(urn) for urn in urns}
        return self._batch_resolve(codes)
    
    def _batch_resolve(self, codes: Set[str]) -> Dict[str, str]:
        """Resolve title codes in batches."""
        resolved = {}
        chunk_size = 150  # LinkedIn's batch limit
        
        # Split codes into chunks
        code_chunks = [list(codes)[i:i + chunk_size] for i in range(0, len(codes), chunk_size)]
        
        for chunk in code_chunks:
            try:
                batch_params = ','.join(chunk)
                locale_param = f"&locale={self.locale}" if self.locale else ""
                url = f"https://api.linkedin.com/v2/titles?ids=List({batch_params}){locale_param}"
                
                LOGGER.info(f"Making batch request to URL: {url}")
                
                headers = {'X-Restli-Protocol-Version': '2.0.0'}
                response = self.client.get(url=url, endpoint="titles", headers=headers)
                
                if response and 'results' in response:
                    for code, result in response['results'].items():
                        if isinstance(result, dict):
                            # Try to get the localized name first, fall back to default name
                            name = (result.get('name', {}).get('localized', {}).get('en_US') or 
                                  result.get('name', {}).get('default') or 
                                  code)
                            resolved[code] = name
                            self._add_to_cache(code, name)
                        else:
                            resolved[code] = code
                            
            except Exception as e:
                if "429" in str(e):
                    LOGGER.warning("Rate limit hit while batch resolving title names. Using codes as fallback.")
                else:
                    LOGGER.warning(f"Failed to batch resolve title names: {str(e)}")
                
                # Add unresolved codes from this chunk
                for code in chunk:
                    if code not in resolved:
                        resolved[code] = code
        
        return resolved

class GeoResolver(URNResolver):
    """Resolver for geo URNs."""
    
    def resolve(self, urns: Set[str]) -> Dict[str, str]:
        if not urns:
            return {}
            
        codes = {self._extract_code(urn) for urn in urns}
        return self._batch_resolve(codes)
    
    def _batch_resolve(self, codes: Set[str]) -> Dict[str, str]:
        """Resolve geo codes in batches."""
        resolved = {}
        chunk_size = 150  # LinkedIn's batch limit
        
        # Split codes into chunks
        code_chunks = [list(codes)[i:i + chunk_size] for i in range(0, len(codes), chunk_size)]
        
        for chunk in code_chunks:
            try:
                batch_params = ','.join(chunk)
                locale_param = f"&locale={self.locale}" if self.locale else ""
                url = f"https://api.linkedin.com/v2/geo?ids=List({batch_params}){locale_param}"
                
                LOGGER.info(f"Making batch request to URL: {url}")
                
                headers = {'X-Restli-Protocol-Version': '2.0.0'}
                response = self.client.get(url=url, endpoint="geo", headers=headers)
                
                if response and 'results' in response:
                    for code, result in response['results'].items():
                        if isinstance(result, dict):
                            name = result.get('defaultLocalizedName', {}).get('value', code)
                            resolved[code] = name
                        else:
                            resolved[code] = code
                            
            except Exception as e:
                if "429" in str(e):
                    LOGGER.warning("Rate limit hit while batch resolving geo names. Using codes as fallback.")
                else:
                    LOGGER.warning(f"Failed to batch resolve geo names: {str(e)}")
                
                # Add unresolved codes from this chunk
                for code in chunk:
                    if code not in resolved:
                        resolved[code] = code
        
        return resolved

class IndustriesResolver(URNResolver):
    """Resolver for industry URNs."""
    
    def resolve(self, urns: Set[str]) -> Dict[str, str]:
        if not urns:
            return {}
            
        codes = {self._extract_code(urn) for urn in urns}
        return self._batch_resolve(codes)
    
    def _batch_resolve(self, codes: Set[str]) -> Dict[str, str]:
        """Resolve industry codes in batches."""
        resolved = {}
        chunk_size = 150  # LinkedIn's batch limit
        
        # Split codes into chunks
        code_chunks = [list(codes)[i:i + chunk_size] for i in range(0, len(codes), chunk_size)]
        
        for chunk in code_chunks:
            try:
                batch_params = ','.join(chunk)
                # For industries, we need to use a different locale format
                locale_param = f"&locale=(language:en,country:US)"  # Always use en_US for industries
                url = f"https://api.linkedin.com/v2/industries?ids=List({batch_params}){locale_param}"
                
                LOGGER.info(f"Making batch request to URL: {url}")
                
                headers = {'X-Restli-Protocol-Version': '2.0.0'}
                response = self.client.get(url=url, endpoint="industries", headers=headers)
                
                if response and 'results' in response:
                    for code, result in response['results'].items():
                        if isinstance(result, dict):
                            name = result.get('name', {}).get('localized', {}).get('en_US', code)
                            resolved[code] = name
                            self._add_to_cache(code, name)
                        else:
                            resolved[code] = code
                            
            except Exception as e:
                if "429" in str(e):
                    LOGGER.warning("Rate limit hit while batch resolving industry names. Using codes as fallback.")
                else:
                    LOGGER.warning(f"Failed to batch resolve industry names: {str(e)}")
                
                # Add unresolved codes from this chunk
                for code in chunk:
                    if code not in resolved:
                        resolved[code] = code
        
        return resolved

class OrganizationsResolver(URNResolver):
    """Resolver for organization URNs."""
    
    def resolve(self, urns: Set[str]) -> Dict[str, str]:
        if not urns:
            return {}
            
        codes = {self._extract_code(urn) for urn in urns}
        return self._batch_resolve(codes)
    
    def _batch_resolve(self, codes: Set[str]) -> Dict[str, str]:
        """Resolve organization codes in batches."""
        resolved = {}
        chunk_size = 150  # LinkedIn's batch limit
        
        # Split codes into chunks
        code_chunks = [list(codes)[i:i + chunk_size] for i in range(0, len(codes), chunk_size)]
        
        for chunk in code_chunks:
            try:
                batch_params = ','.join(chunk)
                # For organizations, we don't need the locale parameter
                url = f"https://api.linkedin.com/rest/organizationsLookup?ids=List({batch_params})"
                
                LOGGER.info(f"Making batch request to URL: {url}")
                
                headers = {'X-Restli-Protocol-Version': '2.0.0'}
                response = self.client.get(url=url, endpoint="organizations", headers=headers)
                
                if response and 'results' in response:
                    for code, result in response['results'].items():
                        if isinstance(result, dict):
                            # Try to get the localized name first, fall back to default name
                            name = (result.get('name', {}).get('localized', {}).get('en_US') or 
                                  result.get('localizedName') or 
                                  code)
                            resolved[code] = name
                            self._add_to_cache(code, name)
                        else:
                            resolved[code] = code
                            
            except Exception as e:
                if "429" in str(e):
                    LOGGER.warning("Rate limit hit while batch resolving organization names. Using codes as fallback.")
                else:
                    LOGGER.warning(f"Failed to batch resolve organization names: {str(e)}")
                
                # Add unresolved codes from this chunk
                for code in chunk:
                    if code not in resolved:
                        resolved[code] = code
        
        return resolved

class SenioritiesResolver(URNResolver):
    """Resolver for seniority URNs."""
    def resolve(self, urns: Set[str]) -> Dict[str, str]:
        if not urns:
            return {}
        codes = {self._extract_code(urn) for urn in urns}
        return self._batch_resolve(codes)

    def _batch_resolve(self, codes: Set[str]) -> Dict[str, str]:
        resolved = {}
        try:
            # Remove locale parameters, use default endpoint
            url = "https://api.linkedin.com/v2/seniorities"
            LOGGER.info(f"Making request to URL: {url}")
            headers = {'X-Restli-Protocol-Version': '2.0.0'}
            response = self.client.get(url=url, endpoint="seniorities", headers=headers)
            LOGGER.info(f"Raw seniorities response: {response}")

            if response and isinstance(response, dict):
                elements = response.get('elements', [])
                for element in elements:
                    if isinstance(element, dict):
                        code = str(element.get('id'))
                        if code in codes:
                            name = element.get('name', {}).get('localized', {}).get('en_US', code)
                            resolved[code] = name
                            self._add_to_cache(code, name)
                            LOGGER.info(f"Resolved seniority {code} to {name}")
        except Exception as e:
            LOGGER.warning(f"Failed to resolve seniority names: {str(e)}")
            import traceback
            LOGGER.warning(f"Traceback: {traceback.format_exc()}")
            for code in codes:
                if code not in resolved:
                    resolved[code] = code
        return resolved

class URNResolverFactory:
    """Factory for creating appropriate URN resolvers."""
    
    @staticmethod
    def create_resolver(endpoint: str, client: Any, locale: Optional[str] = None) -> URNResolver:
        """Create a resolver for the given endpoint."""
        resolvers = {
            'functions': FunctionsResolver,
            'titles': TitlesResolver,
            'geo': GeoResolver,
            'industries': IndustriesResolver,
            'organizations': OrganizationsResolver,
            'seniorities': SenioritiesResolver
        }
        
        resolver_class = resolvers.get(endpoint)
        if not resolver_class:
            raise ValueError(f"Unsupported endpoint: {endpoint}")
            
        return resolver_class(client, locale)

def resolve_urns(client: Any, urns: Set[str], endpoint: str, locale: Optional[str] = None) -> Dict[str, str]:
    """
    Resolve URNs to their names using the appropriate resolver.
    
    Args:
        client: API client
        urns: Set of URNs to resolve
        endpoint: API endpoint to use ('geo', 'functions', or 'titles')
        locale: Optional locale parameter for the API
        
    Returns:
        Dictionary mapping codes to resolved names
    """
    resolver = URNResolverFactory.create_resolver(endpoint, client, locale)
    return resolver.resolve(urns) 