"""
MEMBER_* (demographic / firmographic) streams pulled with one account-wide call per
window instead of one call per campaign, with concurrent requests.

Everything runs against a synthetic in-memory LinkedIn API: no network, no tokens.
"""
import calendar
import datetime
import json
import random
import threading
import time
import unittest
import zlib
from unittest import mock

from singer import metadata

from tap_linkedin_ads import streams
from tap_linkedin_ads.client import LinkedinClient, RateLimitGate
from tap_linkedin_ads.discover import discover

MEMBER_STREAMS = streams.MEMBER_PIVOT_STREAMS

# account id -> campaign ids the campaigns stream lists for it
ACCOUNTS = {1001: [11, 12, 13], 1002: [21]}
# Campaign 99 has analytics rows in account 1001 but is not listed by the campaigns
# stream; the per-campaign path never asked for it, so neither may the new one.
UNLISTED = {99: 1001}
MEMBERS = ['urn:li:testMember:{}'.format(n) for n in range(1, 5)]


def fake_names(client, urns, endpoint, name_path, locale=None):
    return {urn.split(':')[-1]: 'name {}'.format(urn.split(':')[-1]) for urn in urns}


def selected_catalog(stream_names):
    catalog = discover()
    for name in stream_names:
        entry = catalog.get_stream(name)
        mdata = metadata.to_map(entry.metadata)
        for breadcrumb in mdata:
            mdata[breadcrumb]['selected'] = True
        entry.metadata = metadata.to_list(mdata)
    return catalog


def parse_query(url):
    path, _, query = url.partition('?')
    params = {}
    for part in query.split('&'):
        if part:
            key, _, value = part.partition('=')
            params[key] = value
    return path, params


def day_value(*parts):
    return zlib.crc32('|'.join(str(p) for p in parts).encode()) % 7


class FakeLinkedIn:
    """
    Answers the tap's adCampaigns and adAnalytics requests from synthetic daily data,
    the way LinkedIn does: rows only where there is data, MONTHLY rows clipped to the
    requested range, only the requested fields, pivotValues in pivot order.
    """

    def __init__(self, today, schema_types, row_limit=None, delay=None):
        self.today = today
        self.schema_types = schema_types
        self.row_limit = row_limit
        self.delay = delay
        self.lock = threading.Lock()
        self.urls = []
        self.in_flight = 0
        self.max_in_flight = 0

    def campaign_account(self, campaign_id):
        for account_id, campaign_ids in ACCOUNTS.items():
            if campaign_id in campaign_ids:
                return account_id
        return UNLISTED.get(campaign_id)

    def has_data(self, campaign_id, member, day):
        return day_value(campaign_id, member, day) % 3 != 0

    def get(self, url, endpoint=None, headers=None):
        with self.lock:
            self.urls.append(url)
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.delay:
                time.sleep(self.delay())
            return self.answer(url)
        finally:
            with self.lock:
                self.in_flight -= 1

    def answer(self, url):
        path, params = parse_query(url)
        if path.endswith('/adCampaigns'):
            account_id = int(path.split('/adAccounts/')[1].split('/')[0])
            return {'elements': [{'id': c, 'account': 'urn:li:sponsoredAccount:{}'.format(account_id),
                                  'name': 'campaign {}'.format(c)} for c in ACCOUNTS[account_id]],
                    'metadata': {}}
        assert path.endswith('/adAnalytics'), path

        if params['q'] == 'analytics':
            pivots = [params['pivot']]
            campaign_ids = [int(params['campaigns[0]'].split(':')[-1])]
        else:
            assert params['q'] == 'statistics'
            pivots = [params['pivots[0]'], params['pivots[1]']]
            account_id = int(params['accounts[0]'].split(':')[-1])
            if 'campaigns[0]' in params:
                campaign_ids = [int(params['campaigns[0]'].split(':')[-1])]
            else:
                campaign_ids = sorted(c for c in list(ACCOUNTS[account_id]) + list(UNLISTED)
                                      if self.campaign_account(c) == account_id)
        assert params['timeGranularity'] == 'MONTHLY'
        fields = params['fields'].split(',')
        assert len(fields) <= 20
        start = datetime.date(int(params['dateRange.start.year']), int(params['dateRange.start.month']),
                              int(params['dateRange.start.day']))
        end = datetime.date(int(params['dateRange.end.year']), int(params['dateRange.end.month']),
                            int(params['dateRange.end.day']))

        elements = []
        for campaign_id in campaign_ids:
            for member in MEMBERS:
                # one row per calendar month, clipped to the requested range
                bucket_start = start
                while bucket_start <= end:
                    month_end = bucket_start.replace(
                        day=calendar.monthrange(bucket_start.year, bucket_start.month)[1])
                    bucket_end = min(month_end, end)
                    days = [bucket_start + datetime.timedelta(days=n)
                            for n in range((bucket_end - bucket_start).days + 1)]
                    days = [d for d in days if self.has_data(campaign_id, member, d)]
                    if days:
                        elements.append(self.row(pivots, campaign_id, member, bucket_start, bucket_end,
                                                 days, fields))
                    bucket_start = bucket_end + datetime.timedelta(days=1)
        # LinkedIn's answer order is not something the tap may rely on
        random.Random(url).shuffle(elements)
        if self.row_limit is not None:
            elements = elements[:self.row_limit]
        return {'elements': elements, 'paging': {'count': 10000, 'start': 0, 'links': []}}

    def row(self, pivots, campaign_id, member, start, end, days, fields):
        pivot_values = []
        for pivot in pivots:
            pivot_values.append('urn:li:sponsoredCampaign:{}'.format(campaign_id) if pivot == 'CAMPAIGN' else member)
        row = {}
        for field in fields:
            if field == 'dateRange':
                row['dateRange'] = {'start': {'year': start.year, 'month': start.month, 'day': start.day},
                                    'end': {'year': end.year, 'month': end.month, 'day': end.day}}
            elif field == 'pivotValues':
                row['pivotValues'] = pivot_values
            else:
                total = sum(day_value(campaign_id, member, field, d) for d in days)
                row[field] = str(total) if self.schema_types.get(field) == 'string' else total
        return row


def schema_types(catalog):
    types = {}
    for name in MEMBER_STREAMS:
        for prop, schema in catalog.get_stream(name).schema.properties.items():
            typ = schema.type if isinstance(schema.type, list) else [schema.type]
            types[streams.snake_case_to_camel_case(prop)] = 'string' if 'string' in typ else 'number'
    return types


class Capture:
    """Collects singer records and state written during a sync."""

    def __init__(self):
        self.records = {}
        self.states = []

    def write_record(self, stream_name, record, time_extracted=None):
        self.records.setdefault(stream_name, []).append(json.dumps(record, sort_keys=True))

    def write_state(self, state):
        self.states.append(json.loads(json.dumps(state)))


def run_campaigns_sync(client, catalog, state, start_date, member_pivots_by_account, max_concurrency=1,
                       window=30):
    capture = Capture()
    with mock.patch('singer.write_record', side_effect=capture.write_record), \
            mock.patch('singer.write_schema'), \
            mock.patch('singer.write_state', side_effect=capture.write_state), \
            mock.patch.object(streams, 'batch_resolve_urns', side_effect=fake_names):
        streams.Campaigns().sync_endpoint(
            client=client, catalog=catalog, state=state, page_size=100, start_date=start_date,
            selected_streams=list(MEMBER_STREAMS), date_window_size=window,
            account_list=[str(a) for a in ACCOUNTS], skip_inactive_campaigns=False,
            member_pivots_by_account=member_pivots_by_account, max_concurrency=max_concurrency)
    return capture


class MemberPivotTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = selected_catalog(MEMBER_STREAMS)
        cls.types = schema_types(cls.catalog)
        cls.today = datetime.date.today()
        cls.start_date = (cls.today - datetime.timedelta(days=70)).strftime('%Y-%m-%dT00:00:00Z')

    def setUp(self):
        # a clean copy of the class-level params the per-campaign path mutates
        self._params = {name: dict(streams.STREAMS[name].params) for name in MEMBER_STREAMS}

    def tearDown(self):
        for name, params in self._params.items():
            streams.STREAMS[name].params = params

    def fake(self, **kwargs):
        return FakeLinkedIn(self.today, self.types, **kwargs)


class TestPivotToCampaignMapping(MemberPivotTestCase):
    def test_rows_get_campaign_and_member_like_the_per_campaign_call(self):
        rows = [[
            {'pivotValues': ['urn:li:sponsoredCampaign:12', 'urn:li:seniority:3'],
             'dateRange': {'start': {'year': 2026, 'month': 10, 'day': 1}}, 'impressions': 4},
            {'pivotValues': ['urn:li:sponsoredCampaign:11', 'urn:li:seniority:3'],
             'dateRange': {'start': {'year': 2026, 'month': 10, 'day': 1}}, 'impressions': 7},
            {'pivotValues': ['urn:li:sponsoredCampaign:99', 'urn:li:seniority:3'],
             'dateRange': {'start': {'year': 2026, 'month': 10, 'day': 1}}, 'impressions': 1},
        ], [
            {'pivotValues': ['urn:li:sponsoredCampaign:11', 'urn:li:seniority:3'],
             'dateRange': {'start': {'year': 2026, 'month': 10, 'day': 1}}, 'clicks': 2},
        ]]
        with mock.patch.object(streams, 'batch_resolve_urns', side_effect=fake_names):
            records = streams.merge_account_responses(
                'MEMBER_SENIORITY', rows, object(), 'ad_analytics_by_member_seniority', [11, 12])

        self.assertEqual(len(records), 2)  # campaign 99 is not listed: dropped
        first, second = records             # campaign order follows the campaigns stream
        self.assertEqual(first['campaign'], 'urn:li:sponsoredCampaign:11')
        self.assertEqual(first['pivotValues'], ['urn:li:seniority:3'])
        self.assertEqual(first['pivot'], 'MEMBER_SENIORITY')
        self.assertEqual(first['pivot_value'], 'urn:li:seniority:3')
        self.assertEqual(first['pivot_value_name'], 'name 3')
        self.assertEqual((first['impressions'], first['clicks']), (7, 2))  # chunks merged
        self.assertEqual(second['campaign'], 'urn:li:sponsoredCampaign:12')

        transformed = streams.transform_json({'elements': records}, 'ad_analytics_by_member_seniority')['elements']
        self.assertEqual([r['campaign_id'] for r in transformed], [11, 12])

    def test_campaign_is_found_whatever_position_linkedin_puts_it_in(self):
        rows = [[{'pivotValues': ['urn:li:title:9', 'urn:li:sponsoredCampaign:11'],
                  'dateRange': {'start': {'year': 2026, 'month': 10, 'day': 1}}}]]
        records = streams.merge_account_responses('MEMBER_JOB_TITLE', rows, None, 'x', [11])
        self.assertEqual(records[0]['campaign'], 'urn:li:sponsoredCampaign:11')
        self.assertEqual(records[0]['pivot_value'], 'urn:li:title:9')

    def test_request_is_one_account_wide_statistics_call(self):
        params = streams.member_pivot_params(
            'ad_analytics_by_member_company', '1001',
            (datetime.date(2026, 9, 1), datetime.date(2026, 9, 30)), ['dateRange', 'pivotValues'])
        self.assertEqual(params['q'], 'statistics')
        self.assertEqual((params['pivots[0]'], params['pivots[1]']), ('CAMPAIGN', 'MEMBER_COMPANY'))
        self.assertEqual(params['accounts[0]'], 'urn:li:sponsoredAccount:1001')
        self.assertEqual(params['timeGranularity'], 'MONTHLY')
        self.assertNotIn('campaigns[0]', params)

    def test_windows_match_the_per_campaign_path(self):
        today = datetime.date.today()
        for days_back, size in ((70, 30), (365, 30), (3, 30), (40, 7), (0, 30)):
            last = (today - datetime.timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00Z')
            fake = self.fake()
            stream = streams.STREAMS['ad_analytics_by_member_industry']()
            stream.params = {**stream.params, 'campaigns[0]': 'urn:li:sponsoredCampaign:11'}
            with mock.patch.object(streams, 'batch_resolve_urns', side_effect=fake_names), \
                    mock.patch('singer.write_record'):
                stream.sync_ad_analytics(fake, self.catalog, last, size, parent_id=11)
            requested = []
            for url in fake.urls:
                _, p = parse_query(url)
                window = (datetime.date(int(p['dateRange.start.year']), int(p['dateRange.start.month']),
                                        int(p['dateRange.start.day'])),
                          datetime.date(int(p['dateRange.end.year']), int(p['dateRange.end.month']),
                                        int(p['dateRange.end.day'])))
                if window not in requested:
                    requested.append(window)
            self.assertEqual(streams.analytics_windows(last, size, today), requested, (days_back, size))


class TestParityWithPerCampaignPath(MemberPivotTestCase):
    def test_identical_records_and_bookmarks(self):
        old_fake = self.fake()
        old = run_campaigns_sync(old_fake, self.catalog, {}, self.start_date, member_pivots_by_account=False)
        new_fake = self.fake()
        new = run_campaigns_sync(new_fake, self.catalog, {}, self.start_date, member_pivots_by_account=True,
                                 max_concurrency=4)

        self.assertEqual(sorted(old.records), sorted(MEMBER_STREAMS))
        for name in MEMBER_STREAMS:
            self.assertTrue(old.records[name], name)
            self.assertEqual(sorted(old.records[name]), sorted(new.records[name]), name)
            for record in map(json.loads, new.records[name]):
                self.assertIn(record['campaign_id'], (11, 12, 13, 21))
        self.assertEqual(old.states[-1], new.states[-1])
        self.assertEqual(set(new.states[-1]['bookmarks']), set(MEMBER_STREAMS))

        # 2 campaign listings each; the analytics calls go from 4 campaigns to 2 accounts
        self.assertEqual(len(old_fake.urls) - 2, 2 * (len(new_fake.urls) - 2))

    def test_identical_records_with_a_bookmark(self):
        state = {'bookmarks': {name: (self.today - datetime.timedelta(days=20)).strftime('%Y-%m-%dT00:00:00Z')
                               for name in MEMBER_STREAMS}}
        old = run_campaigns_sync(self.fake(), self.catalog, json.loads(json.dumps(state)), self.start_date,
                                 member_pivots_by_account=False, window=7)
        new = run_campaigns_sync(self.fake(), self.catalog, json.loads(json.dumps(state)), self.start_date,
                                 member_pivots_by_account=True, max_concurrency=8, window=7)
        for name in MEMBER_STREAMS:
            self.assertEqual(sorted(old.records[name]), sorted(new.records[name]), name)
        self.assertEqual(old.states[-1], new.states[-1])

    def test_capped_answers_are_refetched_per_campaign(self):
        # Pretend LinkedIn caps answers at 12 rows: every account-wide answer for account
        # 1001 is "full", so those windows are redone one campaign at a time.
        old = run_campaigns_sync(self.fake(), self.catalog, {}, self.start_date, member_pivots_by_account=False)
        capped_fake = self.fake(row_limit=12)
        with mock.patch.object(streams, 'ANALYTICS_ROW_LIMIT', 12):
            new = run_campaigns_sync(capped_fake, self.catalog, {}, self.start_date,
                                     member_pivots_by_account=True, max_concurrency=4)
        for name in MEMBER_STREAMS:
            self.assertEqual(sorted(old.records[name]), sorted(new.records[name]), name)
        self.assertTrue(any('campaigns[0]=' in url and 'q=statistics' in url for url in capped_fake.urls))


class TestConcurrencyOrdering(MemberPivotTestCase):
    def test_same_output_order_whatever_order_calls_finish_in(self):
        serial = run_campaigns_sync(self.fake(), self.catalog, {}, self.start_date,
                                    member_pivots_by_account=True, max_concurrency=1)
        for seed in range(3):
            rng = random.Random(seed)
            lock = threading.Lock()

            def jitter():
                with lock:
                    return rng.uniform(0, 0.004)
            fake = self.fake(delay=jitter)
            concurrent = run_campaigns_sync(fake, self.catalog, {}, self.start_date,
                                            member_pivots_by_account=True, max_concurrency=8)
            self.assertGreater(fake.max_in_flight, 1)
            self.assertLessEqual(fake.max_in_flight, 8)
            self.assertEqual(serial.records, concurrent.records)  # same records, same order
            self.assertEqual(serial.states, concurrent.states)

    def test_run_ordered_keeps_input_order_and_bounds_workers(self):
        lock = threading.Lock()
        running = [0, 0]

        def work(n):
            with lock:
                running[0] += 1
                running[1] = max(running[1], running[0])
            time.sleep(0.002 * (10 - n % 10))
            with lock:
                running[0] -= 1
            return n * n

        self.assertEqual(list(streams.run_ordered(work, range(30), 4)), [n * n for n in range(30)])
        self.assertLessEqual(running[1], 4)
        self.assertGreater(running[1], 1)

    def test_run_ordered_raises_the_first_error(self):
        def work(n):
            if n == 3:
                raise ValueError('boom')
            return n
        with self.assertRaises(ValueError):
            list(streams.run_ordered(work, range(10), 4))

    def test_max_concurrency_setting(self):
        self.assertEqual(streams.normalize_max_concurrency(None), 4)
        self.assertEqual(streams.normalize_max_concurrency(''), 4)
        self.assertEqual(streams.normalize_max_concurrency('6'), 6)
        self.assertEqual(streams.normalize_max_concurrency(0), 1)
        self.assertEqual(streams.normalize_max_concurrency(50), 8)
        self.assertEqual(streams.normalize_max_concurrency('lots'), 4)


class FakeClock:
    def __init__(self):
        self.lock = threading.Lock()
        self.now = 0.0

    def __call__(self):
        with self.lock:
            return self.now

    def sleep(self, seconds):
        with self.lock:
            entered = self.now
            self.now = max(self.now, entered + seconds)
        time.sleep(0.001)


class FakeResponse:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}

    def json(self):
        return self._body


PAUSE = 0.3  # seconds; real time, so every worker really has to sit it out


class FakeSession:
    """The requests.Session under LinkedinClient: the first analytics call gets a 429."""

    def __init__(self, api, clock, tripped):
        self.api = api
        self.clock = clock
        self.tripped = tripped
        self.lock = threading.Lock()
        self.calls = 0
        self.sent_at = []
        self.answered_429 = False

    def request(self, method, url, timeout=None, data=None, headers=None):
        analytics = url.endswith('/adAnalytics')
        with self.lock:
            self.calls += 1
            self.sent_at.append(self.clock())
            first_analytics = analytics and not self.answered_429
            self.answered_429 = self.answered_429 or analytics
        if first_analytics:
            return FakeResponse(429, {'message': 'Too many requests'}, {'Retry-After': str(PAUSE)})
        if analytics:
            # Hold the calls already in flight until the 429 has closed the gate, so
            # the other workers' next requests come after it.
            self.tripped.wait(5)
        return FakeResponse(200, self.api.get('{}?{}'.format(url, data)))

    def close(self):
        pass


class TestRateLimitBackoffUnderConcurrency(MemberPivotTestCase):
    def test_a_429_pauses_every_worker_and_the_call_is_retried(self):
        expected = run_campaigns_sync(self.fake(), self.catalog, {}, self.start_date,
                                      member_pivots_by_account=True, max_concurrency=1)

        clock = time.monotonic
        tripped = threading.Event()
        client = LinkedinClient('id', 'secret', None, 'token', '1001,1002', None)
        session = FakeSession(self.fake(), clock, tripped)
        client._LinkedinClient__session = session  # pylint: disable=protected-access
        gate = client.rate_gate

        waits = []
        real_wait, real_trip = gate.wait, gate.trip

        def wait():
            entered_after_trip = tripped.is_set()
            waited = real_wait()
            waits.append((threading.current_thread().name, entered_after_trip, waited, clock()))
            return waited

        def trip(retry_after=None):
            pause = real_trip(retry_after)
            tripped.set()
            return pause

        gate.wait, gate.trip = wait, trip

        with mock.patch('backoff._sync.time.sleep'):
            got = run_campaigns_sync(client, self.catalog, {}, self.start_date,
                                     member_pivots_by_account=True, max_concurrency=4)

        self.assertEqual(got.records, expected.records)  # nothing lost, same order
        self.assertEqual(got.states, expected.states)
        resume_at = gate._resume_at  # pylint: disable=protected-access

        after = [w for w in waits if w[1]]
        self.assertTrue(after)
        # every request that started after the 429 went out once the pause was over
        self.assertTrue(all(now >= resume_at for _, _, _, now in after))
        # and the pause held back every worker in the pool, not just the one that got it
        paused_workers = {name for name, _, waited, _ in after if waited > 0 and 'linkedin-analytics' in name}
        self.assertEqual(len(paused_workers), 4, waits)
        # one retry on top of the calls the serial run made
        self.assertEqual(session.calls, len(self.fake_calls_for(expected)) + 1)

    def fake_calls_for(self, _expected):
        fake = self.fake()
        run_campaigns_sync(fake, self.catalog, {}, self.start_date, member_pivots_by_account=True)
        return fake.urls

    def test_gate_pause_grows_with_repeated_429s_and_resets(self):
        clock = FakeClock()
        gate = RateLimitGate(sleep=clock.sleep, clock=clock)
        self.assertEqual(gate.trip(), 2)
        self.assertEqual(gate.trip(), 4)
        self.assertEqual(gate.wait(), 4)
        gate.clear()
        self.assertEqual(gate.trip(), 2)
        self.assertEqual(gate.trip(30), 30)
        for _ in range(10):
            gate.trip()
        self.assertEqual(gate.trip(), RateLimitGate.MAX_PAUSE)


if __name__ == '__main__':
    unittest.main()
