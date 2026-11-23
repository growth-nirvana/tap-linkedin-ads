"""
Lead Sync streams (lead_forms, lead_form_responses): pages, per-account bookmarks, and the
skip when LinkedIn refuses lead access (missing scope or role) for an account.

A fake LinkedIn answers the tap's leadForms / leadFormResponses requests: no network, no tokens.
"""
import datetime
import json
import unittest
from unittest import mock

from singer import metadata

from tap_linkedin_ads import leads, streams
from tap_linkedin_ads.client import LinkedInForbiddenError, LinkedInUnauthorizedError
from tap_linkedin_ads.discover import discover
from tap_linkedin_ads.schema import get_abs_path
from tap_linkedin_ads.sync import sync

DAY_MS = 86400 * 1000
NOW = datetime.datetime(2026, 11, 20, 12, 0, tzinfo=datetime.timezone.utc)
NOW_MS = int(NOW.timestamp() * 1000)

SCOPE_403 = LinkedInForbiddenError(
    'HTTP-error-code: 403, Error: Not enough permissions to access: leadFormResponses.FINDER-owner.20260101')
ROLE_401 = LinkedInUnauthorizedError(
    'HTTP-error-code: 401, Error: Member is not authorized to fetch LeadGenForms under a SponsoredAccount')
ROLE_403 = LinkedInForbiddenError('HTTP-error-code: 403, Error: Unpermitted fields present in PARAMETER')
TOKEN_401 = LinkedInUnauthorizedError('HTTP-error-code: 401, Error: Invalid access token')


def selected_catalog(stream_names):
    catalog = discover()
    for name in stream_names:
        entry = catalog.get_stream(name)
        mdata = metadata.to_map(entry.metadata)
        for breadcrumb in mdata:
            mdata[breadcrumb]['selected'] = True
        entry.metadata = metadata.to_list(mdata)
    return catalog


def parse(url):
    path, _, query = url.partition('?')
    params = dict(part.partition('=')[::2] for part in query.split('&') if part)
    return path.rsplit('/', 1)[-1], params


def account_of(params):
    return params['owner'].rsplit('%3A', 1)[-1].rstrip(')')


def form(form_id, account_id, modified_ms):
    return {
        'id': form_id,
        'versionId': 2,
        'owner': {'sponsoredAccount': 'urn:li:sponsoredAccount:{}'.format(account_id)},
        'name': 'Form {}'.format(form_id),
        'state': 'PUBLISHED',
        'created': modified_ms - DAY_MS,
        'lastModified': modified_ms,
        'creationLocale': {'country': 'US', 'language': 'en'},
        'hiddenFields': [{'name': 'utm_source', 'value': 'linkedin'}],
        'reviewInfo': {'reviewStatus': 'REJECTED', 'lastUpdated': modified_ms, 'rejectionReasons': ['CRYPTOCURRENCY']},
        'content': {
            'headline': {'localized': {'en_US': 'Headline', 'es_ES': 'Titular'}},
            'questions': [
                {'questionId': 1, 'name': 'email', 'question': {'localized': {'en_US': 'Email'}},
                 'predefinedField': 'EMAIL', 'responseRequired': True,
                 'questionDetails': {'textQuestionDetails': {'maxResponseLength': 300}}},
                {'questionId': 2, 'name': 'interest', 'question': {'localized': {'en_US': 'Interested?'}},
                 'questionDetails': {'multipleChoiceQuestionDetails': {'options': [
                     {'id': 0, 'text': {'localized': {'en_US': 'Yes'}}},
                     {'id': 1, 'text': {'localized': {'en_US': 'No'}}}]}}},
            ],
            'legalInfo': {'legalInfoId': 9, 'privacyPolicyUrl': 'https://example.com/privacy',
                          'consents': [{'id': 1, 'checkRequired': False,
                                        'consent': {'localized': {'en_US': 'You may email me'}}}]},
            'postSubmissionInfo': {'message': {'localized': {'en_US': 'Thanks'}},
                                   'callToAction': {'callToActionLabel': 'LEARN_MORE',
                                                    'callToActionTarget': {'landingPageUrl': 'https://example.com'}}},
        },
    }


def response(response_id, account_id, submitted_ms):
    return {
        'id': response_id,
        'owner': {'sponsoredAccount': 'urn:li:sponsoredAccount:{}'.format(account_id)},
        'submitter': 'urn:li:person:abc',
        'versionedLeadGenFormUrn': 'urn:li:versionedLeadGenForm:(urn:li:leadGenForm:3162,2)',
        'leadMetadata': {'sponsoredLeadMetadata': {'campaign': 'urn:li:sponsoredCampaign:367378525'}},
        'associatedEntity': {'associatedCreative': 'urn:li:sponsoredCreative:121608865'},
        'leadType': 'SPONSORED',
        'testLead': False,
        'submittedAt': submitted_ms,
        'formResponse': {
            'answers': [
                {'questionId': 1, 'answerDetails': {'textQuestionAnswer': {'answer': 'a@example.com'}}},
                {'questionId': 2, 'answerDetails': {'multipleChoiceAnswer': {'options': [0]}}},
            ],
            'consentResponses': [{'consentId': 1, 'accepted': True}],
        },
    }


class FakeLinkedIn:
    """leadForms / leadFormResponses by owner, with start/count pages and
    submittedAtTimeRange, as LinkedIn answers them. `refuse` maps account id -> error."""

    def __init__(self, forms=None, responses=None, refuse=None, total_in_paging=True):
        self.forms = forms or {}
        self.responses = responses or {}
        self.refuse = refuse or {}
        self.total_in_paging = total_in_paging
        self.calls = []

    def get(self, url=None, path=None, endpoint=None, headers=None, **kwargs):
        resource, params = parse(url)
        self.calls.append((resource, params, headers))
        assert headers.get('X-Restli-Protocol-Version') == '2.0.0'
        assert params['q'] == 'owner'
        account = account_of(params)
        if account in self.refuse:
            raise self.refuse[account]
        if resource == 'leadForms':
            rows = self.forms.get(account, [])
        else:
            assert params['leadType'] == '(leadType:SPONSORED)'
            rows = self.responses.get(account, [])
            window = params.get('submittedAtTimeRange')
            if window:
                start, end = [int(part.split(':')[1]) for part in window.strip('()').split(',')]
                rows = [r for r in rows if start <= r['submittedAt'] <= end]
        start, count = int(params['start']), int(params['count'])
        page = rows[start:start + count]
        paging = {'start': start, 'count': count, 'links': []}
        if self.total_in_paging:
            paging['total'] = len(rows)
        return {'elements': page, 'paging': paging}


class LeadSyncTestCase(unittest.TestCase):

    def setUp(self):
        self.written = []
        patcher = mock.patch.object(
            streams.LinkedInAds, 'write_record',
            lambda stream, record, time_extracted: self.written.append((stream.tap_stream_id, record)))
        patcher.start()
        self.addCleanup(patcher.stop)
        now = mock.patch.object(streams.utils, 'now', return_value=NOW)
        now.start()
        self.addCleanup(now.stop)

    def records(self, stream_name):
        return [record for name, record in self.written if name == stream_name]

    def sync_responses(self, client, state=None, accounts=('1', '2'), page_size=100, scope=True):
        catalog = selected_catalog(['lead_form_responses'])
        return streams.LeadFormResponses().sync_endpoint(
            client=client, catalog=catalog, state=state or {}, page_size=page_size,
            start_date='2026-11-01T00:00:00Z', selected_streams=['lead_form_responses'],
            date_window_size=30, account_list=list(accounts), lead_scope_granted=scope)


class TestRecords(LeadSyncTestCase):

    def test_records_match_their_schemas(self):
        for name, record in (('lead_forms', leads.lead_form_record(form(7, 1, NOW_MS))),
                             ('lead_form_responses', leads.lead_form_response_record(response('r', 1, NOW_MS)))):
            with open(get_abs_path('schemas/{}.json'.format(name)), encoding='utf-8') as handle:
                schema = json.load(handle)
            self.assertEqual(set(record), set(schema['properties']), name)

    def test_form_record(self):
        record = leads.lead_form_record(form(7, 1, NOW_MS))
        self.assertEqual(record['account_id'], '1')
        self.assertEqual(record['sponsored_account_id'], 1)
        self.assertEqual(record['versioned_lead_gen_form'], 'urn:li:versionedLeadGenForm:(urn:li:leadGenForm:7,2)')
        self.assertEqual(record['headline'], 'Headline')
        self.assertEqual(json.loads(record['headline_localized']), {'en_US': 'Headline', 'es_ES': 'Titular'})
        self.assertEqual(record['questions'][0]['question_type'], 'TEXT')
        self.assertEqual([o['text'] for o in record['questions'][1]['options']], ['Yes', 'No'])
        self.assertEqual(record['hidden_fields'], [{'index': 0, 'name': 'utm_source', 'value': 'linkedin'}])
        self.assertEqual(record['review_info_rejection_reasons'], ['CRYPTOCURRENCY'])
        self.assertEqual(record['post_submission_info_call_to_action_landing_page_url'], 'https://example.com')

    def test_response_record(self):
        record = leads.lead_form_response_record(response('r1', 1, NOW_MS))
        self.assertEqual((record['lead_form_id'], record['lead_form_version_id']), (3162, 2))
        self.assertEqual(record['campaign_id'], '367378525')
        self.assertEqual(record['creative_id'], '121608865')
        self.assertEqual(record['answers'][0]['text_question_answer'], 'a@example.com')
        self.assertEqual(record['answers'][1]['multiple_choice_options'], [0])
        self.assertEqual(record['consent_responses'], [{'consent_id': 1, 'consent': None, 'accepted': True}])


class TestPagination(LeadSyncTestCase):

    def test_forms_follow_start_and_count(self):
        client = FakeLinkedIn(forms={'1': [form(n, 1, NOW_MS - n) for n in range(250)]})
        total, bookmark = streams.LeadForms().sync_endpoint(
            client=client, catalog=selected_catalog(['lead_forms']), state={}, page_size=100,
            start_date='2026-11-01T00:00:00Z', selected_streams=['lead_forms'], date_window_size=30,
            account_list=['1'])
        self.assertEqual(total, 250)
        self.assertEqual([int(p['start']) for _, p, _ in client.calls], [0, 100, 200])
        self.assertEqual(sorted(r['id'] for r in self.records('lead_forms')), list(range(250)))
        self.assertTrue(bookmark.startswith('2026-11-20T12:00:00'))

    def test_responses_page_without_a_total(self):
        rows = [response('r{}'.format(n), 1, NOW_MS - n * 1000) for n in range(200)]
        client = FakeLinkedIn(responses={'1': rows}, total_in_paging=False)
        total, _ = self.sync_responses(client, accounts=['1'])
        self.assertEqual(total, 200)
        # A full page asks for the next one; the empty page after it ends the account.
        self.assertEqual([int(p['start']) for _, p, _ in client.calls], [0, 100, 200])
        self.assertEqual(len({r['id'] for r in self.records('lead_form_responses')}), 200)


class TestBookmarks(LeadSyncTestCase):

    def test_one_bookmark_per_account_and_a_lookback(self):
        bookmark_1 = '2026-11-15T00:00:00.000000Z'
        bookmark_1_ms = int(datetime.datetime(2026, 11, 15, tzinfo=datetime.timezone.utc).timestamp() * 1000)
        client = FakeLinkedIn(responses={
            '1': [response('old', 1, bookmark_1_ms - 3 * DAY_MS),     # before the lookback: not asked for
                  response('again', 1, bookmark_1_ms - DAY_MS // 2),  # inside the lookback: pulled again
                  response('new', 1, bookmark_1_ms + 2 * DAY_MS)],
            '2': [response('b', 2, NOW_MS - DAY_MS)],
        })
        total, bookmarks = self.sync_responses(client, state={'bookmarks': {'lead_form_responses': {'1': bookmark_1}}})
        windows = {account_of(p): p['submittedAtTimeRange'] for _, p, _ in client.calls}
        self.assertEqual(windows['1'], '(start:{},end:{})'.format(bookmark_1_ms - DAY_MS, NOW_MS))
        # A new account starts from start_date (less the lookback).
        start_ms = int(datetime.datetime(2026, 10, 31, tzinfo=datetime.timezone.utc).timestamp() * 1000)
        self.assertEqual(windows['2'], '(start:{},end:{})'.format(start_ms, NOW_MS))
        self.assertEqual(total, 3)
        self.assertEqual(sorted(r['id'] for r in self.records('lead_form_responses')), ['again', 'b', 'new'])
        self.assertTrue(bookmarks['1'].startswith('2026-11-17T00:00:00'))
        self.assertTrue(bookmarks['2'].startswith('2026-11-19T12:00:00'))

    def test_an_account_without_leads_keeps_no_bookmark(self):
        _, bookmarks = self.sync_responses(FakeLinkedIn(), accounts=['1'])
        self.assertEqual(bookmarks, {})


class TestLeadAccessSkip(LeadSyncTestCase):

    def test_a_refused_account_is_skipped_and_keeps_its_bookmark(self):
        client = FakeLinkedIn(responses={'2': [response('b', 2, NOW_MS - DAY_MS)]}, refuse={'1': SCOPE_403})
        state = {'bookmarks': {'lead_form_responses': {'1': '2026-11-10T00:00:00.000000Z'}}}
        with self.assertLogs(leads.LOGGER, level='WARNING') as logs:
            total, bookmarks = self.sync_responses(client, state=state)
        self.assertEqual(total, 1)
        self.assertEqual(bookmarks['1'], '2026-11-10T00:00:00.000000Z')
        self.assertIn('2', bookmarks)
        line = logs.output[0]
        self.assertIn('LEAD ACCESS SKIPPED stream=lead_form_responses account=1 reason=scope', line)
        # Never the status code: a runner must not read it as an expired sign-in.
        self.assertNotIn('401', line)
        self.assertNotIn('403', line)

    def test_role_refusals(self):
        for error in (ROLE_401, ROLE_403):
            client = FakeLinkedIn(forms={'2': [form(5, 2, NOW_MS)]}, refuse={'1': error})
            with self.assertLogs(leads.LOGGER, level='WARNING') as logs:
                total, _ = streams.LeadForms().sync_endpoint(
                    client=client, catalog=selected_catalog(['lead_forms']), state={}, page_size=100,
                    start_date='2026-11-01T00:00:00Z', selected_streams=['lead_forms'], date_window_size=30,
                    account_list=['1', '2'])
            self.assertEqual(total, 1)
            self.assertIn('stream=lead_forms account=1 reason=role', logs.output[0])

    def test_a_bad_token_still_stops_the_stream(self):
        client = FakeLinkedIn(refuse={'1': TOKEN_401})
        with self.assertRaises(LinkedInUnauthorizedError):
            self.sync_responses(client, accounts=['1'])

    def test_scope_known_missing_asks_nothing(self):
        client = FakeLinkedIn(responses={'1': [response('a', 1, NOW_MS)]})
        state = {'bookmarks': {'lead_form_responses': {'1': '2026-11-10T00:00:00.000000Z'}}}
        with self.assertLogs(leads.LOGGER, level='WARNING') as logs:
            total, bookmarks = self.sync_responses(client, state=state, scope=False)
        self.assertEqual((total, client.calls), (0, []))
        self.assertEqual(bookmarks, {'1': '2026-11-10T00:00:00.000000Z'})
        self.assertIn('stream=lead_form_responses account=* reason=scope', logs.output[0])
        self.assertIn('sign in again to grant lead access', logs.output[0])

    def test_classification(self):
        self.assertEqual(leads.lead_access_denial(SCOPE_403), 'scope')
        self.assertEqual(leads.lead_access_denial(ROLE_401), 'role')
        self.assertEqual(leads.lead_access_denial(ROLE_403), 'role')
        self.assertEqual(
            leads.lead_access_denial(LinkedInUnauthorizedError("Error: The member doesn't have lead access permission.")),
            'role')
        self.assertIsNone(leads.lead_access_denial(TOKEN_401))

    def test_whole_sync_finishes_and_saves_the_other_accounts(self):
        """Through sync(): a refused account fails nothing (no StreamSyncErrors) and the
        state holds the per-account bookmarks."""
        client = FakeLinkedIn(
            forms={'2': [form(5, 2, NOW_MS)]},
            responses={'2': [response('b', 2, NOW_MS - DAY_MS)]},
            refuse={'1': SCOPE_403})
        state = {}
        config = {'start_date': '2026-11-01T00:00:00Z', 'accounts': '1,2'}
        with mock.patch('singer.write_state'), mock.patch('singer.write_schema'):
            sync(client, config, selected_catalog(['lead_forms', 'lead_form_responses']), state)
        self.assertEqual(len(self.records('lead_forms')), 1)
        self.assertEqual(len(self.records('lead_form_responses')), 1)
        self.assertEqual(set(state['bookmarks']['lead_form_responses']), {'2'})

    def test_config_flag(self):
        self.assertTrue(leads.scope_granted({}))
        self.assertTrue(leads.scope_granted({'lead_sync_scope_granted': True}))
        self.assertFalse(leads.scope_granted({'lead_sync_scope_granted': False}))
        self.assertFalse(leads.scope_granted({'lead_sync_scope_granted': 'false'}))


if __name__ == '__main__':
    unittest.main()
