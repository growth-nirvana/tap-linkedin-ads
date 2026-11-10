import urllib.parse
import re
import copy
import datetime
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
import singer
from singer import metrics, metadata, utils
from singer import Transformer, should_sync_field, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING
from singer.utils import strptime_to_utc, strftime
from tap_linkedin_ads.transform import transform_json, snake_case_to_camel_case
from tap_linkedin_ads.urn_resolver import resolve_urns
from tap_linkedin_ads.client import LinkedInBadRequestError, LinkedInForbiddenError, LinkedInNotFoundError
import json

LOGGER = singer.get_logger()

def batch_resolve_urns(client, urns, endpoint, name_path, locale=None):
    """
    Generic function to resolve URNs in batch.
    Args:
        client: API client
        urns: Set of URNs to resolve
        endpoint: API endpoint to use ('geo', 'functions', or 'titles')
        name_path: Path to extract name from response (e.g. ['defaultLocalizedName', 'value'] or ['name', 'localized', 'en_US'])
        locale: Optional locale parameter for the API
    """
    return resolve_urns(client, urns, endpoint, locale)

# Below fields are a list of foreign keys(primary key of a parent) and replication keys that API can not accept in the parameters.
# We will skip these fields while passing selected fields in the API parameters.
FIELDS_UNAVAILABLE_FOR_AD_ANALYTICS = {
    'campaign',
    'campaignId',
    'startAt',
    'endAt',
    'creative',
    'creativeId',
    # `pivot` and `pivotValue` is not supported anymore, adding values within the code
    'pivot',
    'pivotValue',
    'pivotValueName'  # This field is not available in the API schema
}

ANALYTICS_STREAMS = [
    "ad_analytics_by_campaign",
    "ad_analytics_by_creative",
    "ad_analytics_by_member_company_size",
    "ad_analytics_by_member_industry",
    "ad_analytics_by_member_seniority",
    "ad_analytics_by_member_job_title",
    "ad_analytics_by_member_job_function",
    "ad_analytics_by_member_country_v2",
    "ad_analytics_by_member_region_v2",
    "ad_analytics_by_member_company",
    "ad_analytics_by_placement_name"
]

# Demographic and firmographic streams. `member_pivots_by_account` would pull these with
# one adAnalytics `statistics` call per account, pivoted by CAMPAIGN and the MEMBER_*
# value, but only for a stream whose pivot LinkedIn's statistics finder accepts (see
# STATISTICS_PIVOTS). None of the MEMBER_* pivots is accepted, so every one of these
# streams takes one `analytics` call per campaign.
MEMBER_PIVOT_STREAMS = [
    "ad_analytics_by_member_company_size",
    "ad_analytics_by_member_industry",
    "ad_analytics_by_member_seniority",
    "ad_analytics_by_member_job_title",
    "ad_analytics_by_member_job_function",
    "ad_analytics_by_member_country_v2",
    "ad_analytics_by_member_region_v2",
    "ad_analytics_by_member_company",
]

# The pivots LinkedIn's statistics finder (q=statistics, the only finder taking more than
# one pivot) accepts, from "Statistics Finder Query Parameters" in
# https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting-schema
# (versions 202511 to 202610). The MEMBER_* demographic pivots are listed only for the
# analytics finder (one pivot per call); asking statistics for one gets HTTP 400
# '"MEMBER_COMPANY_SIZE" is not an enum symbol'. So a demographic row can only be tied to
# its campaign by asking one campaign at a time.
STATISTICS_PIVOTS = frozenset([
    'COMPANY', 'ACCOUNT', 'SHARE', 'CAMPAIGN', 'CREATIVE', 'CAMPAIGN_GROUP', 'CONVERSION',
    'CONVERSATION_NODE', 'CONVERSATION_NODE_OPTION_INDEX', 'SERVING_LOCATION', 'OBJECTIVE_TYPE',
    'CARD_INDEX', 'PLACEMENT_NAME', 'IMPRESSION_DEVICE_TYPE', 'EVENT_STAGE',
])

# API errors that are about one stream's request (a bad parameter, no access to that
# report). One of these ends that stream only; the others keep syncing and the tap exits
# non-zero at the end (StreamSyncErrors). Sign-in, rate limit and server errors still stop
# the run, as every stream would hit them.
STREAM_ERRORS = (LinkedInBadRequestError, LinkedInForbiddenError, LinkedInNotFoundError)


class StreamSyncErrors(Exception):
    """Raised once every other stream is done when one or more streams failed.

    The first line names the failed streams (`Streams failed: a, b`); one line per stream
    follows with its error.
    """

    def __init__(self, failed_streams):
        self.failed_streams = dict(failed_streams)
        lines = ['Streams failed: {}'.format(', '.join(sorted(self.failed_streams)))]
        lines += ['{}: {}'.format(name, self.failed_streams[name]) for name in sorted(self.failed_streams)]
        super().__init__('\n'.join(lines))


def record_stream_failure(failed_streams, stream_name, error):
    """Note that stream_name failed (the first error is kept) and log it."""
    if failed_streams is None or stream_name in failed_streams:
        return
    failed_streams[stream_name] = str(error) or error.__class__.__name__
    LOGGER.error('%s failed; skipping the rest of this stream and syncing the others: %s',
                 stream_name, failed_streams[stream_name])


def pivots_by_account(stream_name):
    """True when stream_name can be pulled with one account-wide statistics call."""
    return (stream_name in MEMBER_PIVOT_STREAMS
            and STREAMS[stream_name].params.get('pivot') in STATISTICS_PIVOTS)


# adAnalytics answers are not paged and LinkedIn caps them (15,000 rows); the tap also
# asks for count=10000. A page that comes back this full may be cut short, so the
# account-wide call for that window is redone one campaign at a time (the old path).
ANALYTICS_ROW_LIMIT = 10000

DEFAULT_MAX_CONCURRENCY = 4
MAX_CONCURRENCY_LIMIT = 8

# LinkedIn has a max of 20 fields per request. We cap the chunks at 18
# to make sure there's always room for us to append `dateRange`, and `pivotValues`
MAX_CHUNK_LENGTH = 18

CAMPAIGN_URN_PREFIX = 'urn:li:sponsoredCampaign:'

CURSOR_BASED_PAGINATION_STREAMS = ["accounts", "campaign_groups", "campaigns", "creatives"]
NEW_PATH_STREAMS = ["campaign_groups", "campaigns", "creatives"]
BASE_URL = 'https://api.linkedin.com/rest'

def write_bookmark(state, value, stream_name):
    """
    Write the bookmark in the state corresponding to the stream.
    """
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream_name] = value
    LOGGER.info('Write state for stream: %s, value: %s', stream_name, value)
    singer.write_state(state)

def selected_fields(catalog_for_stream):
    """
    Get all selected fields of given streams
    """
    mdata = metadata.to_map(catalog_for_stream.metadata)
    fields = catalog_for_stream.schema.properties.keys()

    selected_fields_list = []
    # Loop through all fields of the given stream
    for field in fields:
        field_metadata = mdata.get(('properties', field))
        if should_sync_field(field_metadata.get('inclusion'), field_metadata.get('selected')):
            selected_fields_list.append(field)

    return selected_fields_list

def split_into_chunks(fields, chunk_length):
    """
    Return list of chunk_length fields for total fields.
    Example:

    Args: fields = [1, 2, 3, 4, 5], chunk_length = 2
    Return: [[1, 2], [3, 4], [5]]
    """
    return (fields[x:x+chunk_length] for x in range(0, len(fields), chunk_length))

def sync_analytics_endpoint(client, stream_name, path, query_string):
    """
    Call API for analytics endpoint and return all pages of records.
    """
    page = 1
    next_url = 'https://api.linkedin.com/rest/{}?{}'.format(path, query_string)

    # Loop until the last page
    while next_url:
        LOGGER.info('URL for %s: %s', stream_name, next_url)

        data = client.get(url=next_url, endpoint=stream_name)
        yield data
        # Fetch next page
        next_url = get_next_url(stream_name, next_url, data)

        LOGGER.info('%s: Synced page %s', stream_name, page)
        page = page + 1

def active_campaign_ids(client, account_id, window_start, today):
    """
    Ids of the account's campaigns with any analytics rows from window_start to today.

    One account-level adAnalytics call (pivot CAMPAIGN, granularity ALL). LinkedIn only
    returns rows for campaigns that have data, so a campaign missing here has nothing
    for any analytics stream in the window and its per-campaign calls can be skipped.
    Returns None when the call fails; the caller then syncs every campaign, as it
    also does when the answer is empty.
    """
    params = {
        'q': 'analytics',
        'pivot': 'CAMPAIGN',
        'timeGranularity': 'ALL',
        'accounts[0]': 'urn:li:sponsoredAccount:{}'.format(account_id),
        'dateRange.start.day': window_start.day,
        'dateRange.start.month': window_start.month,
        'dateRange.start.year': window_start.year,
        'dateRange.end.day': today.day,
        'dateRange.end.month': today.month,
        'dateRange.end.year': today.year,
        'fields': 'pivotValues,impressions,clicks,costInLocalCurrency',
        'count': 10000,
    }
    query_string = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
    try:
        active = set()
        for page in sync_analytics_endpoint(client, 'active_campaigns', 'adAnalytics', query_string):
            for element in page.get('elements') or []:
                for urn in element.get('pivotValues') or []:
                    campaign_id = urn.split(':')[-1]
                    if campaign_id.isdigit():
                        active.add(int(campaign_id))
        return active
    except Exception as err:  # pylint: disable=broad-except
        LOGGER.warning('Could not list active campaigns for account %s (%s); syncing every campaign', account_id, err)
        return None


def analytics_window_start(child_bookmarks, start_date):
    """The earliest day any analytics child will request (its bookmark minus 7 days)."""
    bookmarks = [value for name, value in child_bookmarks.items() if name in ANALYTICS_STREAMS and value]
    earliest = min((strptime_to_utc(value) for value in bookmarks), default=strptime_to_utc(start_date))
    return (earliest - timedelta(days=7)).date()


def get_next_url(stream_name, next_url, data):
    """
    Prepare and return the URL to fetch the next page of records.
    """
    if stream_name in CURSOR_BASED_PAGINATION_STREAMS:
        next_page_token = data.get('metadata', {}).get('nextPageToken', None)
        if next_page_token:
            if 'pageToken=' in next_url:
                next_url = re.sub(r'pageToken=[^&]+', 'pageToken={}'.format(next_page_token), next_url)
            else:
                next_url = next_url + "&pageToken={}".format(next_page_token)
        else:
            next_url = None
    else:
        # handles index based paination
        next_url = None
        links = data.get('paging', {}).get('links', [])
        for link in links:
            rel = link.get('rel')
            if rel == 'next':
                href = link.get('href')
                if href:
                    # url must be kept encoded for the creatives endpoint.
                    # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#sample-request-3
                    if "rest/creatives" in href or "rest/posts" in href:
                        return 'https://api.linkedin.com{}'.format(href)
                    # Prepare next page URL
                    next_url = 'https://api.linkedin.com{}'.format(urllib.parse.unquote(href))
    return next_url

def shift_sync_window(params, today, date_window_size, forced_window_size=None):
    """
    Move ahead date window by date_window_size and update params with the new date window.

    LinkedIn's adAnalytics `dateRange` includes its end date, so the next window
    starts the day after the current window's end. Starting on the end date would
    request (and emit) the boundary day twice.

    Once the current window already ends on `today`, the returned start is after
    the returned end; callers stop on `start > end`.
    """
    current_end = datetime.date(
        year=params['dateRange.end.year'],
        month=params['dateRange.end.month'],
        day=params['dateRange.end.day'],
    )

    new_start = current_end + timedelta(days=1)
    new_end = min(today, current_end + timedelta(days=(forced_window_size if forced_window_size else date_window_size)))

    new_params = {**params,
                  'dateRange.start.day': new_start.day,
                  'dateRange.start.month': new_start.month,
                  'dateRange.start.year': new_start.year,
                  'dateRange.end.day': new_end.day,
                  'dateRange.end.month': new_end.month,
                  'dateRange.end.year': new_end.year,}
    return new_start, new_end, new_params

# Name lookups for the pivot values of each stream
RESOLUTION_CONFIGS = {
    "ad_analytics_by_member_country_v2": {
        "endpoint": "geo",
        "locale": "(language:en,country:US)"
    },
    "ad_analytics_by_member_region_v2": {
        "endpoint": "geo",
        "locale": "(language:en,country:US)"
    },
    "ad_analytics_by_member_job_function": {
        "endpoint": "functions",
        "name_path": ['name', 'localized', 'en_US'],
        "locale": "en_US"
    },
    "ad_analytics_by_member_job_title": {
        "endpoint": "titles",
        "name_path": ['name', 'localized', 'en_US'],
        "locale": "en_US"
    },
    "ad_analytics_by_member_industry": {
        "endpoint": "industries",
        "name_path": ['name', 'localized', 'en_US'],
        "locale": "(language:en,country:US)"
    },
    "ad_analytics_by_member_company": {
        "endpoint": "organizations",
        "name_path": ['name', 'localized', 'en_US']
    },
    "ad_analytics_by_member_seniority": {
        "endpoint": "seniorities",
        "name_path": ["name", "localized", "en_US"],
        "locale": None
    },
}


def merge_responses(pivot, data, client=None, stream_name=None, parent_id=None):
    """
    Prepare map with key as primary key and value as the record itself for analytics streams.
    The primary key is a combination of pivotValue and start date fields value.
    Update existing records with the same primary key value.

    When ``parent_id`` is supplied (the sponsored campaign id from the parent
    ``campaigns`` sync), each element is tagged with its campaign URN so the
    downstream URN -> _id transforms produce ``campaign`` / ``campaign_id`` for
    the demographic and firmographic pivots (which do not carry the campaign in
    the API response itself).
    """
    full_records = {}
    urns_to_resolve = set()

    campaign_urn = 'urn:li:sponsoredCampaign:{}'.format(parent_id) if parent_id else None

    for page in data:
        # Loop through each record of the page
        for element in page:
            temp_start = element['dateRange']['start']
            temp_pivotValue = element['pivotValues'][0]
            # adding pivot and pivot_value to make it compatible with the previous tap version
            element['pivot'] = pivot
            element["pivot_value"] = temp_pivotValue
            # Demographic/firmographic pivots (MEMBER_*) do not include the
            # campaign in the API response. Stamp the parent campaign URN so
            # `transform_urn` derives `campaign_id` (matching the schema).
            if campaign_urn and 'campaign' not in element:
                element['campaign'] = campaign_urn

            # Collect URNs for resolution
            if client and stream_name in RESOLUTION_CONFIGS:
                urns_to_resolve.add(temp_pivotValue)

            string_start = '{}-{}-{}'.format(temp_start['year'], temp_start['month'], temp_start['day'])
            primary_key = (temp_pivotValue, string_start)
            if primary_key in full_records:
                # Update existing record with same primary key
                full_records[primary_key].update(element)
            else:
                full_records[primary_key] = element

    # Resolve names if needed
    if client and stream_name in RESOLUTION_CONFIGS:
        resolve_pivot_value_names(client, stream_name, urns_to_resolve, full_records.values())

    return full_records


def resolve_pivot_value_names(client, stream_name, urns_to_resolve, records):
    """Set `pivot_value_name` on each record from the stream's name lookup."""
    config = RESOLUTION_CONFIGS[stream_name]
    resolved_names = batch_resolve_urns(
        client,
        urns_to_resolve,
        config["endpoint"],
        config.get("name_path"),
        config.get("locale")
    )
    # Update records with resolved names
    for record in records:
        code = record["pivot_value"].split(':')[-1]
        record["pivot_value_name"] = resolved_names.get(code, code)

def analytics_field_chunks(catalog, stream_name):
    """
    The `fields` lists for one analytics window: a first call for just `dateRange` and
    `pivotValues`, then the selected metrics in groups of MAX_CHUNK_LENGTH, each with
    `dateRange` and `pivotValues` added so the answers can be merged on them.
    """
    # Here, valid_selected_fields is a list of fields that the user has selected.
    # API accepts these fields in the parameter and returns its value in the response.
    valid_selected_fields = [snake_case_to_camel_case(field)
                             for field in selected_fields(catalog.get_stream(stream_name))
                             if snake_case_to_camel_case(field) not in FIELDS_UNAVAILABLE_FOR_AD_ANALYTICS]

    # When testing the API, if the fields in `field` all return `0` then
    # the API returns its empty response.

    # However, the API distinguishes between a day with non-null values
    # (even if this means the values are all `0`) and a day with null
    # values. We found that requesting these fields gives you the days with
    # non-null values
    first_chunk = [['dateRange', 'pivotValues']]

    chunks = first_chunk + list(split_into_chunks(valid_selected_fields, MAX_CHUNK_LENGTH))

    # We have to append these fields in order to ensure we get them back
    # so that we can create the composite primary key for the record and
    # to merge the multiple responses based on this primary key
    for chunk in chunks:
        for field in ['dateRange', 'pivotValues']:
            if field not in chunk:
                chunk.append(field)
    return chunks


def analytics_windows(last_datetime, date_window_size, today):
    """
    The (start, end) date ranges `sync_ad_analytics` requests, in order: from 7 days
    before the bookmark to today, the first window date_window_size + 1 days long and
    the rest date_window_size days (both ends are included by LinkedIn).
    """
    window_start = (strptime_to_utc(last_datetime) - timedelta(days=7)).date()
    window_end = min(window_start + timedelta(days=date_window_size), today)
    window_start = min(window_start, today, window_end)
    windows = []
    while window_end <= today:
        windows.append((window_start, window_end))
        if window_end >= today:
            break
        window_start = window_end + timedelta(days=1)
        window_end = min(today, window_end + timedelta(days=date_window_size))
    return windows


def normalize_max_concurrency(value):
    """`max_concurrency` from the config as an int from 1 to MAX_CONCURRENCY_LIMIT."""
    if value in (None, ''):
        return DEFAULT_MAX_CONCURRENCY
    try:
        value = int(value)
    except (TypeError, ValueError):
        LOGGER.warning('Invalid max_concurrency %r; using %s', value, DEFAULT_MAX_CONCURRENCY)
        return DEFAULT_MAX_CONCURRENCY
    if value > MAX_CONCURRENCY_LIMIT:
        LOGGER.warning('max_concurrency %s is above %s; using %s', value, MAX_CONCURRENCY_LIMIT, MAX_CONCURRENCY_LIMIT)
    return max(1, min(value, MAX_CONCURRENCY_LIMIT))


def run_ordered(func, items, max_concurrency):
    """
    Yield func(item) for each item, in the order of `items`, running up to
    max_concurrency calls at once. Results are handed back in input order whatever
    order the calls finish in, so everything downstream (records, bookmarks) stays
    deterministic. The first error is raised and the calls not yet started are dropped.
    """
    items = list(items)
    if max_concurrency <= 1 or len(items) <= 1:
        for item in items:
            yield func(item)
        return
    with ThreadPoolExecutor(max_workers=max_concurrency, thread_name_prefix='linkedin-analytics') as pool:
        futures = [pool.submit(func, item) for item in items]
        try:
            for future in futures:
                yield future.result()
        finally:
            for future in futures:
                future.cancel()


def campaign_id_of(urn):
    """The numeric id of a sponsoredCampaign URN, else None."""
    if isinstance(urn, str) and urn.startswith(CAMPAIGN_URN_PREFIX):
        campaign_id = urn[len(CAMPAIGN_URN_PREFIX):]
        if campaign_id.isdigit():
            return int(campaign_id)
    return None


def member_pivot_params(stream_name, account_id, window, fields, campaign_id=None):
    """
    adAnalytics `statistics` query for one MEMBER_* stream, account and window,
    pivoted by CAMPAIGN and the stream's member pivot (Rest.li 1.0 query syntax, as
    the tap's other adAnalytics calls). `campaign_id` narrows it to one campaign.
    """
    stream_params = STREAMS[stream_name].params
    window_start, window_end = window
    params = {
        'q': 'statistics',
        'pivots[0]': 'CAMPAIGN',
        'pivots[1]': stream_params['pivot'],
        'timeGranularity': stream_params['timeGranularity'],
        'accounts[0]': 'urn:li:sponsoredAccount:{}'.format(account_id),
    }
    if campaign_id is not None:
        params['campaigns[0]'] = '{}{}'.format(CAMPAIGN_URN_PREFIX, campaign_id)
    params.update({
        'dateRange.start.day': window_start.day,
        'dateRange.start.month': window_start.month,
        'dateRange.start.year': window_start.year,
        'dateRange.end.day': window_end.day,
        'dateRange.end.month': window_end.month,
        'dateRange.end.year': window_end.year,
        'fields': ','.join(fields),
        'start': 0,
        'count': stream_params.get('count', ANALYTICS_ROW_LIMIT),
    })
    return params


def fetch_analytics_rows(client, stream_name, params):
    """
    All rows of one adAnalytics query, following `next` links if LinkedIn sends any.
    Returns (rows, maybe_truncated): a page holding ANALYTICS_ROW_LIMIT rows or more
    with no next link may have been cut short by LinkedIn.
    """
    query_string = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
    rows = []
    maybe_truncated = False
    for page in sync_analytics_endpoint(client, stream_name, 'adAnalytics', query_string):
        elements = page.get('elements') or []
        rows.extend(elements)
        if len(elements) >= ANALYTICS_ROW_LIMIT and not get_next_url(stream_name, '', page):
            maybe_truncated = True
    return rows, maybe_truncated


def fetch_member_window(client, stream_name, account_id, window, chunks, campaign_ids):
    """
    Every field chunk of one MEMBER_* stream for one account and window, as a list of
    row lists (one per chunk, in chunk order) for merge_account_responses.

    One account-wide call per chunk covers all campaigns. If any chunk may have hit
    LinkedIn's row cap, the window is fetched again one campaign at a time instead.
    (Splitting the date range would change the MONTHLY rows' start dates; splitting by
    campaign gives the same rows the per-campaign path did.)
    """
    responses = []
    for chunk in chunks:
        rows, maybe_truncated = fetch_analytics_rows(
            client, stream_name, member_pivot_params(stream_name, account_id, window, chunk))
        if maybe_truncated:
            LOGGER.warning('%s: account %s, %s to %s reached %s rows; fetching it per campaign',
                           stream_name, account_id, window[0], window[1], ANALYTICS_ROW_LIMIT)
            return fetch_member_window_per_campaign(client, stream_name, account_id, window, chunks, campaign_ids)
        responses.append(rows)
    return responses


def fetch_member_window_per_campaign(client, stream_name, account_id, window, chunks, campaign_ids):
    """The fallback of fetch_member_window: the same query once per campaign."""
    responses = []
    for campaign_id in campaign_ids:
        for chunk in chunks:
            rows, maybe_truncated = fetch_analytics_rows(
                client, stream_name,
                member_pivot_params(stream_name, account_id, window, chunk, campaign_id=campaign_id))
            if maybe_truncated:
                LOGGER.warning('%s: campaign %s, %s to %s reached %s rows; LinkedIn may have dropped some',
                               stream_name, campaign_id, window[0], window[1], ANALYTICS_ROW_LIMIT)
            responses.append(rows)
    return responses


def merge_account_responses(pivot, data, client, stream_name, campaign_ids):
    """
    merge_responses for the account-wide MEMBER_* calls, whose rows carry
    pivotValues [campaign URN, member URN].

    Each row is reshaped to what the per-campaign call returned: `pivotValues` holds
    just the member URN, `pivot` / `pivot_value` are the member pivot, and `campaign`
    is the campaign URN (so `campaign_id` is filled). Rows are merged on
    (campaign, member value, start date). Only campaigns listed by the campaigns
    stream are kept, as the per-campaign path only asked for those.

    Returns the records in a fixed order: campaign (in campaign_ids order), then
    start date, then member value.
    """
    order = {campaign_id: index for index, campaign_id in enumerate(campaign_ids)}
    full_records = {}
    urns_to_resolve = set()
    dropped = 0

    for page in data:
        for element in page:
            pivot_values = element.get('pivotValues') or []
            campaign_urn = next((v for v in pivot_values if campaign_id_of(v) is not None), None)
            member_values = [v for v in pivot_values if v != campaign_urn]
            campaign_id = campaign_id_of(campaign_urn)
            if campaign_id not in order or not member_values:
                dropped += 1
                continue
            member_value = member_values[0]
            element['pivotValues'] = [member_value]
            element['pivot'] = pivot
            element['pivot_value'] = member_value
            if 'campaign' not in element:
                element['campaign'] = campaign_urn

            if client and stream_name in RESOLUTION_CONFIGS:
                urns_to_resolve.add(member_value)

            temp_start = element['dateRange']['start']
            start_key = (temp_start['year'], temp_start['month'], temp_start['day'])
            primary_key = (order[campaign_id], start_key, member_value)
            if primary_key in full_records:
                full_records[primary_key].update(element)
            else:
                full_records[primary_key] = element

    if dropped:
        LOGGER.info('%s: skipped %s rows for campaigns the campaigns stream did not list', stream_name, dropped)

    records = [full_records[key] for key in sorted(full_records)]
    if client and stream_name in RESOLUTION_CONFIGS:
        resolve_pivot_value_names(client, stream_name, urns_to_resolve, records)
    return records


def sync_member_analytics_by_account(client, catalog, state, start_date, stream_names,
                                     account_id, campaign_ids, date_window_size,
                                     max_concurrency=DEFAULT_MAX_CONCURRENCY):
    """
    Sync the MEMBER_* streams for one account with account-wide calls.

    Every (stream, window) is fetched on a pool of up to max_concurrency threads; the
    results are taken back in stream then window order, and the records are written
    from this thread only, so output order does not depend on which call finishes
    first. Returns {stream_name: (records written, max bookmark)}; the caller writes
    the bookmarks once every account is done, as for the per-campaign streams.
    """
    today = datetime.date.today()
    plans = []
    for stream_name in stream_names:
        stream_obj = STREAMS[stream_name]()
        last_datetime = stream_obj.get_bookmark(state, start_date)
        chunks = analytics_field_chunks(catalog, stream_name)
        for window in analytics_windows(last_datetime, date_window_size, today):
            plans.append((stream_obj, last_datetime, chunks, window))

    LOGGER.info('Account %s: %s member-pivot windows for %s campaigns, %s at a time',
                account_id, len(plans), len(campaign_ids), max_concurrency)

    def fetch(plan):
        stream_obj, _, chunks, window = plan
        return fetch_member_window(client, stream_obj.tap_stream_id, account_id, window, chunks, campaign_ids)

    results = {}
    for plan, responses in zip(plans, run_ordered(fetch, plans, max_concurrency)):
        stream_obj, last_datetime, _, window = plan
        stream_name = stream_obj.tap_stream_id
        total_records, max_bookmark_value = results.get(stream_name, (0, last_datetime))

        records = merge_account_responses(stream_obj.params['pivot'], responses, client, stream_name, campaign_ids)
        time_extracted = utils.now()
        transformed_data = transform_json({stream_obj.data_key: records}, stream_name)[stream_obj.data_key]
        if transformed_data:
            last_datetime_dt = strptime_to_utc(last_datetime) - timedelta(days=7)
            window_bookmark, record_count = stream_obj.process_records(
                catalog=catalog,
                records=transformed_data,
                time_extracted=time_extracted,
                bookmark_field=next(iter(stream_obj.replication_keys)),
                max_bookmark_value=last_datetime,
                last_datetime=strftime(last_datetime_dt))
            if strptime_to_utc(window_bookmark) > strptime_to_utc(max_bookmark_value):
                max_bookmark_value = window_bookmark
            total_records += record_count
            LOGGER.info('%s: account %s, %s to %s: %s records', stream_name, account_id,
                        window[0], window[1], record_count)
        results[stream_name] = (total_records, max_bookmark_value)
    return results


class LinkedInAds:
    """
    A base class representing tap-linkedin-ads streams
    properties:

        tap_stream_id        : stream name for the endpoint
        replicaiton_method   : replication method of given streams. Possible values: FULL_TABLE, INCREMENTAL
        replicaion_keys      : Replications keys for an incremental stream
        key_properties       : Primary keys for a given stream
        path                 : API endpoint relative path, when added to the base URL, creates the full path
        account_filter       : Method for Account filtering. Each uses a different query pattern/parameter:
            search_id_values_param, search_account_values_param, accounts_param
        params               : Query, sort, and other endpoint specific parameters
        data_key             : JSON element containing the records for the endpoint
        bookmark_query_field : Typically a date-time field is used for filtering the query
        bookmark_field       : Replication key field, typically a date-time, used for filtering the results
            and setting the state
        foreign_key          : Primary key of the Parent stream.
        children             : A collection of child endpoints (where the endpoint path includes the parent id)
        parent               : On each of the children, name of the parent stream

    """
    tap_stream_id = None
    replicaiton_method = None
    replication_keys = None
    key_properties = []
    foreign_key = None
    account_filter = None
    path = None
    parent = None
    data_key = None
    children = []
    count = None
    params = {}
    headers = {}
    def write_schema(self, catalog):
        """
        Write the schema for the selected stream.
        """
        stream = catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()
        try:
            singer.write_schema(self.tap_stream_id, schema, stream.key_properties)
        except OSError as err:
            LOGGER.info('OS Error writing schema for: %s', self.tap_stream_id)
            raise err

    def write_record(self, record, time_extracted):
        """
        Write the record for the selected stream.
        """
        try:
            singer.write_record(self.tap_stream_id, record, time_extracted=time_extracted)
        except OSError as err:
            LOGGER.info('OS Error writing record for: %s', self.tap_stream_id)
            LOGGER.info('record: %s', record)
            raise err

    def get_bookmark(self, state, default):
        """
        Return bookmark value if available in the state otherwise return start date
        """
        if (state is None) or ('bookmarks' not in state):
            return default
        return (
            state
            .get('bookmarks', {})
            .get(self.tap_stream_id, default)
        )

    # pylint: disable=too-many-arguments,too-many-locals
    def process_records(self,
                        catalog,
                        records,
                        time_extracted,
                        bookmark_field=None,
                        max_bookmark_value=None,
                        last_datetime=None,
                        parent_id=None):
        """
        Transform and write a record if the replication key value is greater than the last bookmark.
        Update maximum bookmark value to write in the state.
        """
        stream = catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in records:
                # If child object, add parent_id to record
                if parent_id and self.parent:
                    record[self.parent + '_id'] = parent_id

                # Transform record for Singer.io
                with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) \
                    as transformer:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)
                    
                    # For FULL_TABLE replication, always write the record
                    if self.replication_method == "FULL_TABLE":
                        self.write_record(transformed_record, time_extracted=time_extracted)
                        counter.increment()
                        # Still track max bookmark for state
                        if bookmark_field and (bookmark_field in transformed_record):
                            if max_bookmark_value is None or strptime_to_utc(transformed_record[bookmark_field]) > strptime_to_utc(max_bookmark_value):
                                max_bookmark_value = transformed_record[bookmark_field]
                    else:
                        # For INCREMENTAL replication, check bookmark values
                        if bookmark_field and (bookmark_field in transformed_record):
                            # Reset max_bookmark_value to new value if higher
                            if max_bookmark_value is None or strptime_to_utc(transformed_record[bookmark_field]) > strptime_to_utc(max_bookmark_value):
                                max_bookmark_value = transformed_record[bookmark_field]

                            last_dttm = strptime_to_utc(last_datetime)
                            bookmark_dttm = strptime_to_utc(transformed_record[bookmark_field])
                            # Keep only records whose bookmark is after the last_datetime
                            if bookmark_dttm >= last_dttm:
                                self.write_record(transformed_record, time_extracted=time_extracted)
                                counter.increment()
                        else:
                            # Write record if replication key is not available in the record
                            self.write_record(transformed_record, time_extracted=time_extracted)
                            counter.increment()

            return max_bookmark_value, counter.value

    # pylint: disable=too-many-branches,too-many-statements,too-many-arguments,too-many-locals,too-many-nested-blocks
    def sync_endpoint(self,
                      client,
                      catalog,
                      state,
                      page_size,
                      start_date,
                      selected_streams,
                      date_window_size,
                      parent_id=None,
                      account_list=None,
                      skip_inactive_campaigns=False,
                      member_pivots_by_account=False,
                      max_concurrency=1,
                      failed_streams=None):
        """
        Sync a specific parent or child endpoint.

        A child stream whose request LinkedIn rejects (STREAM_ERRORS) is noted in
        failed_streams and skipped from then on; its bookmark is left as it was. The
        parent and its other children carry on.

        With skip_inactive_campaigns, analytics children are only requested for the
        campaigns active in their window (see active_campaign_ids).

        With member_pivots_by_account, the MEMBER_* analytics children of `campaigns`
        are synced with account-wide calls once the account's campaigns are listed
        (see sync_member_analytics_by_account), up to max_concurrency calls at once. Only
        streams whose pivot the statistics finder accepts go that way (pivots_by_account):
        none of the MEMBER_* ones do, so they take the per-campaign calls.
        """
        if failed_streams is None:
            failed_streams = {}
        # Get the latest bookmark for the stream and set the last_datetime
        last_datetime = self.get_bookmark(state, start_date)
        max_bookmark_value = last_datetime
        LOGGER.info('%s: bookmark last_datetime = %s', self.tap_stream_id, max_bookmark_value)

        bookmark_field = next(iter(self.replication_keys))
        # Initialize child_max_bookmarks
        child_max_bookmarks = {}
        children = self.children
        # Loop through all children
        for child_stream_name in children:

            if child_stream_name in selected_streams:
                child_obj = STREAMS[child_stream_name]()
                # Write schema for each child stream
                child_obj.write_schema(catalog)
                child_bookmark_field = child_obj.replication_keys
                if child_bookmark_field:
                    child_last_datetime = child_obj.get_bookmark(state, start_date)
                    # Add the last bookmark of child stream in the `child_max_bookmarks` map
                    child_max_bookmarks[child_stream_name] = child_last_datetime

        # Pagination reference:
        # https://docs.microsoft.com/en-us/linkedin/shared/api-guide/concepts/pagination?context=linkedin/marketing/context
        # Each page has a "start" (offset value) and a "count" (batch size, number of records)
        # Increase the "start" by the "count" for each batch.
        # Continue until the "start" exceeds the total_records.
        start = 0 # Starting offset value for each batch API call
        total_records = 0
        page = 1

        if self.tap_stream_id in CURSOR_BASED_PAGINATION_STREAMS:
            # hardcoding the pagesize to 1000 for stream - accounts, as search and pageToken param can't be present at the same time.
            if self.tap_stream_id == "accounts":
                page_size = 1000
            endpoint_params = {
                'pageSize': page_size,
                **self.params
            }
        else:
            endpoint_params = {
                'start': start,
                'count': page_size,
                **self.params # adds in endpoint specific, sort, filter params
            }

        querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in endpoint_params.items()])

        urllist = []
        if self.tap_stream_id in NEW_PATH_STREAMS:
            # As per the latest linkedin version, few url formats are modified, it expects advertiser
            # account_id in each url path
            for account in account_list:
                url = "{}/adAccounts/{}/{}?{}".format(BASE_URL, account, self.path, querystring)
                urllist.append((account, url))
        else:
            if self.path == 'posts':
                url = '{}/{}?{}&dscAdAccount=urn%3Ali%3AsponsoredAccount%3A{}'.format(BASE_URL, self.path, querystring, parent_id)
            else:
                url = '{}/{}?{}'.format(BASE_URL, self.path, querystring)
            urllist.append((None, url))

        account_level_children = []
        if member_pivots_by_account and self.tap_stream_id == 'campaigns':
            account_level_children = [c for c in children if c in selected_streams and pivots_by_account(c)]
        analytics_children = [c for c in children if c in selected_streams and c in ANALYTICS_STREAMS
                              and c not in account_level_children]
        for acct_id, next_url in urllist:
            account_campaign_ids = []
            active_campaigns = None
            if skip_inactive_campaigns and self.tap_stream_id == 'campaigns' and analytics_children and acct_id:
                window_start = analytics_window_start(child_max_bookmarks, start_date)
                active_campaigns = active_campaign_ids(client, acct_id, window_start, datetime.date.today())
                if not active_campaigns:
                    # Nothing listed: sync every campaign rather than trust an empty answer.
                    active_campaigns = None
                if active_campaigns is not None:
                    LOGGER.info('Account %s: %s campaigns active since %s; analytics only for those',
                                acct_id, len(active_campaigns), window_start)
            skipped = 0
            while next_url: #pylint: disable=too-many-nested-blocks
                LOGGER.info('URL for %s: %s', self.tap_stream_id, next_url)

                # Get data, API request
                data = client.get(
                    url=next_url,
                    endpoint=self.tap_stream_id,
                    headers=self.headers)
                # time_extracted: datetime when the data was extracted from the API
                time_extracted = utils.now()

                # Transform data with transform_json from transform.py
                #  This function converts unix datetimes, de-nests audit fields,
                #  tranforms URNs to IDs, tranforms/abstracts variably named fields,
                #  converts camelCase to snake_case for fieldname keys.
                # For the Linkedin Ads API, 'elements' is always the root data_key for records.
                # The data_key identifies the collection of records below the <root> element
                transformed_data = [] # initialize the record list
                if self.data_key in data:
                    transformed_data = transform_json(data, self.tap_stream_id)[self.data_key]
                if not transformed_data or transformed_data is None:
                    LOGGER.info('No transformed_data')
                    break # No data results

                pre_singer_transformed_data = copy.deepcopy(transformed_data)
                if account_level_children:
                    account_campaign_ids.extend(
                        record.get('id') for record in pre_singer_transformed_data
                        if record.get('id') is not None)
                if self.tap_stream_id in selected_streams:
                    # Process records and gets the max_bookmark_value and record_count for the set of records
                    max_bookmark_value, record_count = self.process_records(
                        catalog=catalog,
                        records=transformed_data,
                        time_extracted=time_extracted,
                        bookmark_field=bookmark_field,
                        max_bookmark_value=max_bookmark_value,
                        last_datetime=last_datetime,
                        parent_id=parent_id)
                    LOGGER.info('%s, records processed: %s', self.tap_stream_id, record_count)
                    total_records = total_records + record_count

                # Loop thru parent batch records for each children objects
                for child_stream_name in children:
                    if (child_stream_name in selected_streams and child_stream_name not in account_level_children
                            and child_stream_name not in failed_streams):
                        # For each parent record
                        child_obj = STREAMS[child_stream_name]()

                        if self.tap_stream_id == 'campaigns' and child_stream_name in MEMBER_PIVOT_STREAMS:
                            skipped += self.sync_member_analytics_per_campaign(
                                client, catalog, state, start_date, date_window_size, child_obj,
                                pre_singer_transformed_data, active_campaigns, child_max_bookmarks,
                                failed_streams, max_concurrency)
                            continue

                        for record in pre_singer_transformed_data:

                            parent_id = record.get(child_obj.foreign_key)
                            if (active_campaigns is not None and child_stream_name in ANALYTICS_STREAMS
                                    and str(parent_id).isdigit() and int(parent_id) not in active_campaigns):
                                skipped += 1
                                continue

                            child_stream_params = child_obj.params
                            # Add children filter params based on parent IDs
                            if self.tap_stream_id == 'accounts':
                                account = 'urn:li:sponsoredAccount:{}'.format(parent_id)
                            elif self.tap_stream_id == 'campaigns':
                                campaign = 'urn:li:sponsoredCampaign:{}'.format(parent_id)
                                if child_stream_name == 'creatives':
                                    # The value of the campaigns in the query params should be passed in the encoded format.
                                    # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#sample-request-3
                                    child_stream_params['campaigns'] = 'List(urn%3Ali%3AsponsoredCampaign%3A{})'.format(parent_id)
                                elif child_stream_name in (ANALYTICS_STREAMS):
                                    child_stream_params['campaigns[0]'] = campaign

                            # Update params for the child stream
                            child_obj.params = child_stream_params
                            LOGGER.info('Syncing: %s, parent_stream: %s, parent_id: %s',
                                        child_stream_name,
                                        self.tap_stream_id,
                                        parent_id)

                            # Call sync method for the child stream
                            try:
                                if child_stream_name in ANALYTICS_STREAMS:
                                    child_total_records, child_batch_bookmark_value = child_obj.sync_ad_analytics(
                                        client=client,
                                        catalog=catalog,
                                        last_datetime=child_obj.get_bookmark(state, start_date),
                                        date_window_size=date_window_size,
                                        parent_id=parent_id)
                                else:
                                    child_total_records, child_batch_bookmark_value = child_obj.sync_endpoint(
                                        client=client,
                                        catalog=catalog,
                                        state=state,
                                        page_size=page_size,
                                        start_date=start_date,
                                        selected_streams=selected_streams,
                                        date_window_size=date_window_size,
                                        parent_id=parent_id,
                                        account_list=[acct_id],
                                        failed_streams=failed_streams)
                            except STREAM_ERRORS as error:
                                record_stream_failure(failed_streams, child_stream_name, error)
                                break

                            child_batch_bookmark_dttm = strptime_to_utc(child_batch_bookmark_value)
                            child_max_bookmark = child_max_bookmarks.get(child_stream_name)
                            child_max_bookmark_dttm = strptime_to_utc(child_max_bookmark)
                            if child_batch_bookmark_dttm > child_max_bookmark_dttm:
                                # Update bookmark for child stream.
                                child_max_bookmarks[child_stream_name] = strftime(child_batch_bookmark_dttm)

                            LOGGER.info('Synced: %s, parent_id: %s, total_records: %s',
                                        child_stream_name,
                                        parent_id,
                                        child_total_records)
                            LOGGER.info('FINISHED Syncing: %s', child_stream_name)

                # Pagination: Get next_url
                next_url = get_next_url(self.tap_stream_id, next_url, data)

                if self.tap_stream_id in selected_streams:
                    LOGGER.info('%s: Synced page %s, this page: %s. Total records processed: %s',
                                self.tap_stream_id,
                                page,
                                record_count,
                                total_records)
                page = page + 1

            if active_campaigns is not None:
                LOGGER.info('Account %s: skipped %s analytics requests for inactive campaigns', acct_id, skipped)

            pending_account_children = [c for c in account_level_children if c not in failed_streams]
            if pending_account_children and account_campaign_ids:
                try:
                    account_results = sync_member_analytics_by_account(
                        client=client,
                        catalog=catalog,
                        state=state,
                        start_date=start_date,
                        stream_names=pending_account_children,
                        account_id=acct_id,
                        campaign_ids=account_campaign_ids,
                        date_window_size=date_window_size,
                        max_concurrency=max_concurrency)
                except STREAM_ERRORS as error:
                    # The streams of one account are fetched together: the error is
                    # charged to each of them.
                    for child_stream_name in pending_account_children:
                        record_stream_failure(failed_streams, child_stream_name, error)
                    account_results = {}
                for child_stream_name, (child_total_records, child_batch_bookmark_value) in account_results.items():
                    child_batch_bookmark_dttm = strptime_to_utc(child_batch_bookmark_value)
                    if child_batch_bookmark_dttm > strptime_to_utc(child_max_bookmarks.get(child_stream_name)):
                        child_max_bookmarks[child_stream_name] = strftime(child_batch_bookmark_dttm)
                    LOGGER.info('Synced: %s, account: %s, total_records: %s',
                                child_stream_name, acct_id, child_total_records)

        # Write child stream's bookmarks (not for a child that failed: it starts from the
        # same place next run)
        for key, val in list(child_max_bookmarks.items()):
            if key not in failed_streams:
                write_bookmark(state, val, key)

        return total_records, max_bookmark_value

    # pylint: disable=too-many-arguments,too-many-locals
    def sync_member_analytics_per_campaign(self, client, catalog, state, start_date, date_window_size,
                                           child_obj, campaign_records, active_campaigns,
                                           child_max_bookmarks, failed_streams, max_concurrency):
        """
        One MEMBER_* child of `campaigns` for one page of campaigns: one adAnalytics
        `analytics` call per campaign, window and field chunk, as sync_ad_analytics makes.

        The calls of up to max_concurrency campaigns run at once on worker threads
        (fetch_ad_analytics, which writes nothing); the records are written from this
        thread in campaign order (write_ad_analytics), so records and state are the same
        as one campaign at a time, whatever order the calls finish in. A 429 pauses every
        worker (the client's rate gate). A STREAM_ERRORS error ends this stream: the
        campaigns before it are written, the rest are not asked.

        Returns how many campaigns were skipped as inactive.
        """
        child_stream_name = child_obj.tap_stream_id
        skipped = 0
        campaign_ids = []
        for record in campaign_records:
            parent_id = record.get(child_obj.foreign_key)
            if (active_campaigns is not None and str(parent_id).isdigit()
                    and int(parent_id) not in active_campaigns):
                skipped += 1
                continue
            campaign_ids.append(parent_id)

        # Child bookmarks are only written once every account is done, so this is the
        # same for every campaign.
        child_last_datetime = child_obj.get_bookmark(state, start_date)
        base_params = dict(child_obj.params)

        def fetch(parent_id):
            params = {**base_params, 'campaigns[0]': '{}{}'.format(CAMPAIGN_URN_PREFIX, parent_id)}
            return child_obj.fetch_ad_analytics(client, catalog, child_last_datetime, date_window_size,
                                                parent_id=parent_id, params=params)

        try:
            for parent_id, windows in zip(campaign_ids, run_ordered(fetch, campaign_ids, max_concurrency)):
                LOGGER.info('Syncing: %s, parent_stream: %s, parent_id: %s',
                            child_stream_name, self.tap_stream_id, parent_id)
                child_total_records, child_batch_bookmark_value = child_obj.write_ad_analytics(
                    client, catalog, child_last_datetime, windows, parent_id=parent_id)

                child_batch_bookmark_dttm = strptime_to_utc(child_batch_bookmark_value)
                if child_batch_bookmark_dttm > strptime_to_utc(child_max_bookmarks.get(child_stream_name)):
                    child_max_bookmarks[child_stream_name] = strftime(child_batch_bookmark_dttm)

                LOGGER.info('Synced: %s, parent_id: %s, total_records: %s',
                            child_stream_name, parent_id, child_total_records)
                LOGGER.info('FINISHED Syncing: %s', child_stream_name)
        except STREAM_ERRORS as error:
            record_stream_failure(failed_streams, child_stream_name, error)
        return skipped

    # pylint: disable=too-many-branches,too-many-statements,unused-argument
    def sync_ad_analytics(self, client, catalog, last_datetime, date_window_size, parent_id=None):
        """
        Sync method for ad_analytics_by_campaign, ad_analytics_by_creative
        """
        windows = self.fetch_ad_analytics(client, catalog, last_datetime, date_window_size,
                                          parent_id=parent_id, params=self.params)
        return self.write_ad_analytics(client, catalog, last_datetime, windows, parent_id=parent_id)

    def fetch_ad_analytics(self, client, catalog, last_datetime, date_window_size, parent_id=None, params=None):
        """
        The API calls of sync_ad_analytics: every window's answers for one parent, as a list
        of (pivot, responses) in window order. Writes nothing, so it can run on a worker
        thread (`params` is this parent's own copy).
        """
        last_datetime_dt = strptime_to_utc(last_datetime) - timedelta(days=7)

        # Prepare date window for API call
        window_start_date = last_datetime_dt.date()
        window_end_date = window_start_date + timedelta(days=date_window_size)
        today = datetime.date.today()

        # Ensure end_date is not in the future
        if window_end_date > today:
            window_end_date = today
        # Ensure start_date is not in the future
        if window_start_date > today:
            window_start_date = today
        # Ensure start_date is not after end_date
        if window_start_date > window_end_date:
            window_start_date = window_end_date

        # Override the default start and end dates
        static_params = {**(self.params if params is None else params),
                         'dateRange.start.day': window_start_date.day,
                         'dateRange.start.month': window_start_date.month,
                         'dateRange.start.year': window_start_date.year,
                         'dateRange.end.day': window_end_date.day,
                         'dateRange.end.month': window_end_date.month,
                         'dateRange.end.year': window_end_date.year,}

        chunks = analytics_field_chunks(catalog, self.tap_stream_id)

        ############### PAGINATION (for these 2 streams) ###############
        # The Tap requests LinkedIn with one Campaign ID at one time.
        # 1 Campaign permits 100 Ads
        # Considering, 1 Ad is active and the existing behavior of the tap uses 30 Day window size
        #       and timeGranularity = DAILY(Results grouped by day) we get 30 records in one API response
        # Considering the maximum permitted size of Ads are created, "3000" records will be returned in an API response.
        # If "count=100" and records=100 in the API are the same then the next URL will be returned and if we hit that URL, 400 error code will be returned.
        # This case is unreachable because here "count" is 10000 and at maximum, only 3000 records will be returned in an API response.

        windows = []
        while window_end_date <= today:
            responses = []
            for chunk in chunks:
                static_params['fields'] = ','.join(chunk)
                query_params = {"start": 0,
                                **static_params}
                query_string = '&'.join(['%s=%s' % (key, value) for (key, value) in query_params.items()])
                LOGGER.info('Syncing %s from %s to %s', parent_id, window_start_date, window_end_date)
                for page in sync_analytics_endpoint(client, self.tap_stream_id, self.path, query_string):
                    if page.get(self.data_key):
                        responses.append(page.get(self.data_key))
            pivot = query_params["pivot"] if "pivot" in query_params.keys() else None
            windows.append((pivot, responses))

            window_start_date, window_end_date, static_params = shift_sync_window(static_params, today, date_window_size)

            # The window just synced ended on today: nothing is left to request.
            if window_start_date > window_end_date:
                break
        return windows

    def write_ad_analytics(self, client, catalog, last_datetime, windows, parent_id=None):
        """
        The writing half of sync_ad_analytics: merge, transform and write each window's
        answers (from fetch_ad_analytics) in window order. Returns (records, bookmark).
        """
        bookmark_field = next(iter(self.replication_keys))
        max_bookmark_value = last_datetime
        last_datetime_dt = strptime_to_utc(last_datetime) - timedelta(days=7)

        total_records = 0
        for pivot, responses in windows:
            raw_records = merge_responses(pivot, responses, client, self.tap_stream_id, parent_id)
            time_extracted = utils.now()

            # While we broke the ad_analytics streams out from
            # `sync_endpoint()`, we want to process them the same. And
            # transform_json() expects a dictionary with a key equal to
            # `data_key` and its value is the response from the API

            # Note that `transform_json()` returns the same structure we pass
            # in. `sync_endpoint()` grabs `data_key` from the return value, so
            # we mirror that here
            transformed_data = transform_json({self.data_key: list(raw_records.values())},
                                              self.tap_stream_id)[self.data_key]
            if not transformed_data:
                LOGGER.info('No transformed_data')
            else:
                max_bookmark_value, record_count = self.process_records(
                    catalog=catalog,
                    records=transformed_data,
                    time_extracted=time_extracted,
                    bookmark_field=bookmark_field,
                    max_bookmark_value=last_datetime,
                    last_datetime=strftime(last_datetime_dt),
                    parent_id=parent_id)
                LOGGER.info('%s, records processed: %s', self.tap_stream_id, record_count)
                LOGGER.info('%s: max_bookmark: %s', self.tap_stream_id, max_bookmark_value)
                total_records += record_count

        return total_records, max_bookmark_value

class Accounts(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts
    """
    tap_stream_id = "accounts"
    replication_method = "FULL_TABLE"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_id_values_param"
    path = "adAccounts"
    data_key = "elements"
    children = ["video_ads"]
    params = {
        "q": "search"
    }
    headers = {'X-Restli-Protocol-Version': "2.0.0"}

class VideoAds(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video#finders
    """
    tap_stream_id = "video_ads"
    replication_keys = ["last_modified_time"]
    replication_method = "INCREMENTAL"
    key_properties = ["content_reference"]
    foreign_key = "id"
    path = "posts"
    data_key = "elements"
    parent = "accounts"
    params = {
        "q": "dscAdAccount",
        "dscAdTypes": "List(VIDEO)",
        "count":100
    }
    headers = {'X-Restli-Protocol-Version': "2.0.0"}

    def sync_endpoint(self, *args, **kwargs):
        try:
            return super().sync_endpoint(*args, **kwargs)
        except Exception as error:
            if "Not enough permissions to access: partnerApiPostsExternal" in str(error):
                LOGGER.warning("Access to the video-ads API is denied due to insufficient permissions. Please reauthenticate or verify the required permissions.")
                LOGGER.error(error)
                # total record count (zero), initial bookmark returned to supress this failure
                return 0, self.get_bookmark(kwargs.get("state"), kwargs.get("start_date"))
            raise error

class AccountUsers(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts
    """
    tap_stream_id = "account_users"
    replication_keys = ["last_modified_time"]
    replication_method = "FULL_TABLE"
    key_properties = ["account_id", "user_person_id"]
    account_filter = "accounts_param"
    path = "adAccountUsers"
    data_key = "elements"
    params = {
        "q": "accounts"
    }

class CampaignGroups(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups#search-for-campaign-groups
    """
    tap_stream_id = "campaign_groups"
    replication_method = "FULL_TABLE"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_account_values_param"
    path = "adCampaignGroups"
    data_key = "elements"
    params = {
        "q": "search"
    }

class Campaigns(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns
    """
    tap_stream_id = "campaigns"
    replication_method = "FULL_TABLE"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_account_values_param"
    path = "adCampaigns"
    data_key = "elements"
    children = ["creatives"] + ANALYTICS_STREAMS
    params = {
        "q": "search",
        "search.status.values[0]": "ACTIVE",
        "search.status.values[1]": "PAUSED",
        "search.status.values[2]": "ARCHIVED",
        "search.status.values[3]": "COMPLETED",
        "search.status.values[4]": "CANCELED",
        "search.status.values[5]": "DRAFT",
        "search.status.values[6]": "PENDING_DELETION",
        "search.status.values[7]": "REMOVED"
    }

class Creatives(LinkedInAds):
    """
    https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives
    """
    tap_stream_id = "creatives"
    replication_method = "FULL_TABLE"
    replication_keys = ["last_modified_at"]
    key_properties = ["id"]
    path = "creatives"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    # The value of the campaigns in the query params should be passed in the encoded format.
    # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#sample-request-3
    params = {
        "q": "criteria",
        "campaigns": "List(urn%3Ali%3AsponsoredCampaign%3A{})",
        "sortOrder": "ASCENDING"
    }
    # Requires this specific headers for creatives endpoint.
    # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives
    headers = {'X-Restli-Protocol-Version': "2.0.0",
               "X-RestLi-Method": "FINDER"}

class AdAnalyticsByCampaign(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    tap_stream_id = "ad_analytics_by_campaign"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "CAMPAIGN",
        "timeGranularity": "DAILY",
        "count": 10000
    }

class AdAnalyticsByCreative(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    tap_stream_id = "ad_analytics_by_creative"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["creative_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "CREATIVE",
        "timeGranularity": "DAILY",
        "count": 10000
    }

class AdAnalyticsByMemberCompanySize(LinkedInAds):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    tap_stream_id = "ad_analytics_by_member_company_size"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "MEMBER_COMPANY_SIZE",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

class AdAnalyticsByMemberIndustry(LinkedInAds):
    tap_stream_id = "ad_analytics_by_member_industry"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "MEMBER_INDUSTRY",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

class AdAnalyticsByMemberSeniority(LinkedInAds):
    tap_stream_id = "ad_analytics_by_member_seniority"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "MEMBER_SENIORITY",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

class AdAnalyticsByMemberJobTitle(LinkedInAds):
    tap_stream_id = "ad_analytics_by_member_job_title"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "MEMBER_JOB_TITLE",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

class AdAnalyticsByMemberJobFunction(LinkedInAds):
    tap_stream_id = "ad_analytics_by_member_job_function"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "MEMBER_JOB_FUNCTION",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

class AdAnalyticsByMemberCountryV2(LinkedInAds):
    tap_stream_id = "ad_analytics_by_member_country_v2"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "MEMBER_COUNTRY_V2",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

class AdAnalyticsByMemberRegionV2(LinkedInAds):
    tap_stream_id = "ad_analytics_by_member_region_v2"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "MEMBER_REGION_V2",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

class AdAnalyticsByMemberCompany(LinkedInAds):
    tap_stream_id = "ad_analytics_by_member_company"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "MEMBER_COMPANY",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

class AdAnalyticsByPlacementName(LinkedInAds):
    tap_stream_id = "ad_analytics_by_placement_name"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "PLACEMENT_NAME",
        "timeGranularity": "MONTHLY",
        "count": 10000
    }

# Dictionary of the stream classes
STREAMS = {
    "accounts": Accounts,
    "video_ads": VideoAds,
    "account_users": AccountUsers,
    "campaign_groups": CampaignGroups,
    "campaigns": Campaigns,
    "creatives": Creatives,
    "ad_analytics_by_campaign": AdAnalyticsByCampaign,
    "ad_analytics_by_creative": AdAnalyticsByCreative,
    "ad_analytics_by_member_company_size": AdAnalyticsByMemberCompanySize,
    "ad_analytics_by_member_industry": AdAnalyticsByMemberIndustry,
    "ad_analytics_by_member_seniority": AdAnalyticsByMemberSeniority,
    "ad_analytics_by_member_job_title": AdAnalyticsByMemberJobTitle,
    "ad_analytics_by_member_job_function": AdAnalyticsByMemberJobFunction,
    "ad_analytics_by_member_country_v2": AdAnalyticsByMemberCountryV2,
    "ad_analytics_by_member_region_v2": AdAnalyticsByMemberRegionV2,
    "ad_analytics_by_member_company": AdAnalyticsByMemberCompany,
    "ad_analytics_by_placement_name": AdAnalyticsByPlacementName
}
