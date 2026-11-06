"""Inactive campaigns are skipped for analytics, and pivot names are looked up once per run."""
import datetime
import unittest
from unittest import mock

from tap_linkedin_ads import streams, urn_resolver


class FakeClient:
    def __init__(self, pages=None, error=None):
        self.pages = pages or []
        self.error = error
        self.urls = []

    def get(self, url, endpoint, headers=None):
        self.urls.append(url)
        if self.error:
            raise self.error
        return self.pages.pop(0) if self.pages else {'elements': []}


class TestActiveCampaigns(unittest.TestCase):
    def test_lists_campaigns_with_rows_for_the_account_and_window(self):
        client = FakeClient([{'elements': [
            {'pivotValues': ['urn:li:sponsoredCampaign:11'], 'impressions': 5},
            {'pivotValues': ['urn:li:sponsoredCampaign:22'], 'impressions': 0},
        ]}])
        active = streams.active_campaign_ids(client, '999', datetime.date(2026, 10, 1), datetime.date(2026, 11, 5))
        self.assertEqual(active, {11, 22})
        url = client.urls[0]
        self.assertIn('pivot=CAMPAIGN', url)
        self.assertIn('timeGranularity=ALL', url)
        self.assertIn('accounts[0]=urn:li:sponsoredAccount:999', url)
        self.assertIn('dateRange.start.day=1&dateRange.start.month=10&dateRange.start.year=2026', url)

    def test_a_failed_call_means_no_skipping(self):
        client = FakeClient(error=RuntimeError('429'))
        self.assertIsNone(streams.active_campaign_ids(client, '999', datetime.date(2026, 10, 1), datetime.date(2026, 11, 5)))

    def test_window_starts_seven_days_before_the_earliest_analytics_bookmark(self):
        bookmarks = {
            'ad_analytics_by_campaign': '2026-10-20T00:00:00Z',
            'ad_analytics_by_member_industry': '2026-10-01T00:00:00Z',
            'creatives': '2020-01-01T00:00:00Z',
        }
        self.assertEqual(streams.analytics_window_start(bookmarks, '2025-01-01T00:00:00Z'), datetime.date(2026, 9, 24))
        self.assertEqual(streams.analytics_window_start({}, '2026-01-08T00:00:00Z'), datetime.date(2026, 1, 1))


class TestResolverCache(unittest.TestCase):
    def setUp(self):
        urn_resolver._RESOLVED.clear()

    def test_names_are_looked_up_once_per_run(self):
        calls = []

        class Fake:
            def resolve(self, urns):
                calls.append(set(urns))
                return {u.split(':')[-1]: 'name ' + u.split(':')[-1] for u in urns}

        with mock.patch.object(urn_resolver.URNResolverFactory, 'create_resolver', return_value=Fake()):
            first = urn_resolver.resolve_urns(None, {'urn:li:title:1', 'urn:li:title:2'}, 'titles')
            second = urn_resolver.resolve_urns(None, {'urn:li:title:2', 'urn:li:title:3'}, 'titles')
        self.assertEqual(first, {'1': 'name 1', '2': 'name 2'})
        self.assertEqual(second, {'2': 'name 2', '3': 'name 3'})
        self.assertEqual(calls, [{'urn:li:title:1', 'urn:li:title:2'}, {'urn:li:title:3'}])

    def test_failed_lookups_are_retried(self):
        class Fallback:
            def resolve(self, urns):
                return {u.split(':')[-1]: u.split(':')[-1] for u in urns}

        with mock.patch.object(urn_resolver.URNResolverFactory, 'create_resolver', return_value=Fallback()) as factory:
            urn_resolver.resolve_urns(None, {'urn:li:title:7'}, 'titles')
            self.assertEqual(urn_resolver.resolve_urns(None, {'urn:li:title:7'}, 'titles'), {'7': '7'})
        self.assertEqual(factory.call_count, 2)


if __name__ == '__main__':
    unittest.main()
