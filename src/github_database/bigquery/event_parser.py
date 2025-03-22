"""Parser for GitHub Archive event data from BigQuery."""

import json
from datetime import datetime
from typing import Dict, Any, Optional

class EventParser:
    """Parse and normalize GitHub Archive event data."""
    
    def _extract_json_field(self, data: Dict[str, Any], field_name: str) -> Dict[str, Any]:
        """Extract and parse JSON field from event data."""
        try:
            if isinstance(data.get(field_name), str):
                return json.loads(data[field_name])
            return data.get(field_name, {})
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def _extract_id(self, data: Dict[str, Any], field: str) -> int:
        """Extract ID from a JSON field."""
        try:
            json_data = self._extract_json_field(data, field)
            return int(json_data.get('id', 0))
        except (ValueError, TypeError):
            return 0
            
    def _extract_login(self, data: Dict[str, Any], field: str) -> str:
        """Extract login from a JSON field."""
        json_data = self._extract_json_field(data, field)
        return json_data.get('login', '')
    
    def parse_push_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse PushEvent data."""
        payload = self._extract_json_field(event_data, 'payload')
        repo_id = self._extract_id(event_data, 'repo')
        actor_id = self._extract_id(event_data, 'actor')
        actor_login = self._extract_login(event_data, 'actor')
        
        return {
            'event_id': f"push_{repo_id}_{event_data['created_at']}",
            'repo_id': repo_id,
            'type': 'push',
            'actor_id': actor_id,
            'actor_login': actor_login,
            'created_at': event_data['created_at'],
            'commit_count': len(payload.get('commits', [])),
            'ref': payload.get('ref', ''),
            'before': payload.get('before', ''),
            'head': payload.get('head', ''),
            'size': payload.get('size', 0)
        }
        
    def parse_pull_request_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse PullRequestEvent data."""
        payload = self._extract_json_field(event_data, 'payload')
        repo_id = self._extract_id(event_data, 'repo')
        actor_id = self._extract_id(event_data, 'actor')
        actor_login = self._extract_login(event_data, 'actor')
        pr = payload.get('pull_request', {})
        
        return {
            'event_id': f"pr_{repo_id}_{payload.get('number', 0)}",
            'repo_id': repo_id,
            'type': 'pull_request',
            'actor_id': actor_id,
            'actor_login': actor_login,
            'created_at': event_data['created_at'],
            'number': payload.get('number', 0),
            'action': payload.get('action', ''),
            'state': pr.get('state', ''),
            'title': pr.get('title', ''),
            'additions': pr.get('additions', 0),
            'deletions': pr.get('deletions', 0),
            'changed_files': pr.get('changed_files', 0),
            'merged': pr.get('merged', False),
            'merged_at': pr.get('merged_at', None),
            'merge_commit_sha': pr.get('merge_commit_sha', '')
        }
        
    def parse_issues_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse IssuesEvent data."""
        payload = self._extract_json_field(event_data, 'payload')
        repo_id = self._extract_id(event_data, 'repo')
        actor_id = self._extract_id(event_data, 'actor')
        actor_login = self._extract_login(event_data, 'actor')
        issue = payload.get('issue', {})
        
        return {
            'event_id': f"issue_{repo_id}_{payload.get('issue', {}).get('number', 0)}",
            'repo_id': repo_id,
            'type': 'issue',
            'actor_id': actor_id,
            'actor_login': actor_login,
            'created_at': event_data['created_at'],
            'number': issue.get('number', 0),
            'action': payload.get('action', ''),
            'state': issue.get('state', ''),
            'title': issue.get('title', ''),
            'body': issue.get('body', ''),
            'comments': issue.get('comments', 0),
            'closed_at': issue.get('closed_at', None)
        }
        
    def parse_fork_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse ForkEvent data."""
        payload = self._extract_json_field(event_data, 'payload')
        repo_id = self._extract_id(event_data, 'repo')
        actor_id = self._extract_id(event_data, 'actor')
        actor_login = self._extract_login(event_data, 'actor')
        forkee = payload.get('forkee', {})
        
        return {
            'event_id': f"fork_{repo_id}_{forkee.get('id', 0)}",
            'repo_id': repo_id,
            'type': 'fork',
            'actor_id': actor_id,
            'actor_login': actor_login,
            'created_at': event_data['created_at'],
            'fork_id': forkee.get('id', 0),
            'fork_full_name': forkee.get('full_name', ''),
            'fork_stars': forkee.get('stargazers_count', 0),
            'fork_forks': forkee.get('forks_count', 0)
        }
        
    def parse_watch_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse WatchEvent (star) data."""
        repo_id = self._extract_id(event_data, 'repo')
        actor_id = self._extract_id(event_data, 'actor')
        actor_login = self._extract_login(event_data, 'actor')
        payload = self._extract_json_field(event_data, 'payload')
        
        return {
            'event_id': f"watch_{repo_id}_{actor_id}_{event_data['created_at']}",
            'repo_id': repo_id,
            'type': 'watch',
            'actor_id': actor_id,
            'actor_login': actor_login,
            'created_at': event_data['created_at'],
            'action': payload.get('action', '')
        }
        
    def parse_event(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse any GitHub event based on its type.
        
        Args:
            event_data: Raw event data from BigQuery
            
        Returns:
            Parsed event data or None if event type is not supported
        """
        event_type = event_data.get('type')  
        parser_map = {
            'PushEvent': self.parse_push_event,
            'PullRequestEvent': self.parse_pull_request_event,
            'IssuesEvent': self.parse_issues_event,
            'ForkEvent': self.parse_fork_event,
            'WatchEvent': self.parse_watch_event
        }
        
        parser = parser_map.get(event_type)
        if parser:
            return parser(event_data)
        return None
