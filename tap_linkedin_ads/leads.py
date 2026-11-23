"""
Lead Sync: lead forms and lead form responses (LinkedIn's Lead Sync API).

https://learn.microsoft.com/en-us/linkedin/marketing/lead-sync/leadsync
https://learn.microsoft.com/en-us/linkedin/marketing/lead-sync/lead-sync-schema

Access, from LinkedIn's Permissions table:
- `leadForms` of an ad account (owner sponsoredAccount) are readable with `r_ads` and any
  ad account role (VIEWER and up).
- `leadFormResponses` (the leads) need the Lead Sync program's
  `r_marketing_leadgen_automation` scope, and the member needs an ad account role AND a
  role on the company page tied to the ad account (ADMINISTRATOR, LEAD_GEN_FORMS_MANAGER,
  CURATOR, CONTENT_ADMINISTRATOR or ANALYST).

LinkedIn answers a missing scope with 403 ("Not enough permissions to access: ...") and a
missing role with 403, or 401 "Member is not authorized to fetch LeadGenForms ..." /
"The member doesn't have lead access permission." Either way the tap skips that stream for
that account, logs one LEAD_ACCESS_LOG line and keeps going: the run does not fail.

This module only reshapes records and classifies errors; the streams are in streams.py.
"""
import json
import re

import singer

from tap_linkedin_ads.client import LinkedInForbiddenError, LinkedInUnauthorizedError

LOGGER = singer.get_logger()

# The OAuth scope LinkedIn's Lead Sync program grants (r_ads_leadgen_automation is its
# older name; a token holding either can read leads).
LEAD_SYNC_SCOPES = ('r_marketing_leadgen_automation', 'r_ads_leadgen_automation')

LEAD_STREAMS = ('lead_forms', 'lead_form_responses')

# One line per skipped (stream, account). Read by whatever runs the tap (Aria's connector turns
# it into a run notice), so keep the shape: stream=<name> account=<id or *> reason=<scope|role>.
LEAD_ACCESS_LOG = 'LEAD ACCESS SKIPPED'

# Responses are pulled again from this long before each account's bookmark.
LEAD_RESPONSE_LOOKBACK_DAYS = 1

_HTTP_PREFIX = re.compile(r'^HTTP-error-code: \d+, Error: ')


def lead_access_denial(error):
    """'scope' or 'role' when `error` is LinkedIn refusing lead access, else None.

    A 403 "Not enough permissions to access: <resource>" is the token lacking the scope;
    any other 403, and a 401 that talks about leads, is the member lacking a role. A 401
    that does not mention leads is a bad token and is not ours to swallow.
    """
    message = str(error)
    if isinstance(error, LinkedInForbiddenError):
        return 'scope' if 'not enough permissions to access' in message.lower() else 'role'
    if isinstance(error, LinkedInUnauthorizedError) and 'lead' in message.lower():
        return 'role'
    return None


def log_lead_access_skip(stream_name, account, reason, detail):
    """The one line a skipped (stream, account) leaves. LinkedIn's status code is left out
    on purpose: a runner reading the log must not take it for an expired sign-in."""
    detail = _HTTP_PREFIX.sub('', str(detail or '')).strip()
    LOGGER.warning('%s stream=%s account=%s reason=%s: %s', LEAD_ACCESS_LOG, stream_name, account, reason, detail)


def scope_granted(config):
    """False only when whoever ran the tap knows the token lacks the Lead Sync scope
    (config lead_sync_scope_granted: false). Unknown: try, and let LinkedIn say no."""
    value = config.get('lead_sync_scope_granted', True)
    return str(value).lower() not in ('false', '0', 'no')


def owner_param(account_id):
    """The Rest.li 2.0 owner value for an ad account, URN encoded as LinkedIn asks."""
    return '(sponsoredAccount:urn%3Ali%3AsponsoredAccount%3A{})'.format(account_id)


def urn_id(urn):
    """The last part of a URN ('urn:li:sponsoredCampaign:123' -> '123'), else None."""
    if isinstance(urn, str) and urn.startswith('urn:'):
        return urn.rsplit(':', 1)[-1] or None
    return None


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def localized(text, locale=None):
    """The string of a MultiLocaleString in the form's creation locale, else its first one."""
    if not isinstance(text, dict):
        return None
    values = text.get('localized')
    if not isinstance(values, dict) or not values:
        return None
    if isinstance(locale, dict) and locale.get('language'):
        key = locale['language'] + ('_' + locale['country'] if locale.get('country') else '')
        if key in values:
            return values[key]
    return next(iter(values.values()))


def localized_json(text):
    """Every locale of a MultiLocaleString, as a JSON string (the locales vary per form)."""
    if not isinstance(text, dict) or not isinstance(text.get('localized'), dict):
        return None
    return json.dumps(text['localized'], sort_keys=True, ensure_ascii=False)


def versioned_form_urn(form_id, version_id):
    if form_id is None or version_id is None:
        return None
    return 'urn:li:versionedLeadGenForm:(urn:li:leadGenForm:{},{})'.format(form_id, version_id)


_VERSIONED = re.compile(r'urn:li:versionedLeadGenForm:\(urn:li:leadGenForm:(\d+),\s*(\d+)\)')


def parse_versioned_form(urn):
    """(form id, version) of a versionedLeadGenForm URN, else (None, None)."""
    match = _VERSIONED.match(urn or '')
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _question(question, locale):
    details = question.get('questionDetails') or {}
    text = details.get('textQuestionDetails')
    choice = details.get('multipleChoiceQuestionDetails')
    criteria = (question.get('questionSubmissionCriteria') or {}).get('workEmailSubmissionCriteria') or {}
    return {
        'question_id': question.get('questionId'),
        'name': question.get('name'),
        'label': question.get('label'),
        'question': localized(question.get('question'), locale),
        'question_localized': localized_json(question.get('question')),
        'text_prompt': localized(question.get('textPrompt'), locale),
        'predefined_field': question.get('predefinedField'),
        'response_editable': question.get('responseEditable'),
        'response_required': question.get('responseRequired'),
        'question_type': 'TEXT' if text is not None else ('MULTIPLE_CHOICE' if choice is not None else None),
        'text_question_details_default_response': (text or {}).get('defaultResponse'),
        'text_question_details_max_response_length': (text or {}).get('maxResponseLength'),
        'multiple_choice_question_details_default_selected_option': (choice or {}).get('defaultSelectedOption'),
        'questions_submission_criteria_freemium_email_allowed': criteria.get('freemiumEmailAllowed'),
        'options': [
            {
                'id': option.get('id'),
                'label': option.get('label'),
                'text': localized(option.get('text'), locale),
                'text_localized': localized_json(option.get('text')),
            }
            for option in (choice or {}).get('options') or []
        ],
    }


def lead_form_record(form):
    """One leadForms element as a flat row (Fivetran's LEAD_FORM_HISTORY columns), with the
    questions, options, consents, hidden fields and rejection reasons as nested arrays
    (Fivetran's LEAD_FORM_QUESTION / _MULTIPLE_CHOICE_QUESTION_OPTION / _CONSENT /
    _HIDDEN_FIELD / _REJECTION_REASON tables)."""
    owner = form.get('owner') or {}
    locale = form.get('creationLocale') or {}
    content = form.get('content') or {}
    legal = content.get('legalInfo') or {}
    post = content.get('postSubmissionInfo') or {}
    call_to_action = post.get('callToAction') or {}
    review = form.get('reviewInfo') or {}
    return {
        'id': form.get('id'),
        'version_id': form.get('versionId'),
        'version_tag': form.get('versionTag'),
        'versioned_lead_gen_form': versioned_form_urn(form.get('id'), form.get('versionId')),
        'owner': owner.get('sponsoredAccount') or owner.get('organization'),
        'account_id': urn_id(owner.get('sponsoredAccount')),
        'sponsored_account_id': _int_or_none(urn_id(owner.get('sponsoredAccount'))),
        'organization_id': _int_or_none(urn_id(owner.get('organization'))),
        'name': form.get('name'),
        'state': form.get('state'),
        'created': form.get('created'),
        'last_modified': form.get('lastModified'),
        'locale_country': locale.get('country'),
        'locale_language': locale.get('language'),
        'headline': localized(content.get('headline'), locale),
        'headline_localized': localized_json(content.get('headline')),
        'description': localized(content.get('description'), locale),
        'description_localized': localized_json(content.get('description')),
        'legal_info_legal_info_id': legal.get('legalInfoId'),
        'legal_info_privacy_policy_url': legal.get('privacyPolicyUrl'),
        'legal_info_legal_disclaimer': localized(legal.get('legalDisclaimer'), locale),
        'legal_info_legal_disclaimer_localized': localized_json(legal.get('legalDisclaimer')),
        'post_submission_info_message': localized(post.get('message'), locale),
        'post_submission_info_message_localized': localized_json(post.get('message')),
        'post_submission_info_call_to_action_label': call_to_action.get('callToActionLabel'),
        'post_submission_info_call_to_action_landing_page_url':
            (call_to_action.get('callToActionTarget') or {}).get('landingPageUrl'),
        'review_info_review_status': review.get('reviewStatus'),
        'review_info_last_updated': review.get('lastUpdated'),
        'review_info_rejection_reasons': list(review.get('rejectionReasons') or []),
        'hidden_fields': [
            {'index': index, 'name': field.get('name'), 'value': field.get('value')}
            for index, field in enumerate(form.get('hiddenFields') or [])
        ],
        'questions': [_question(q, locale) for q in content.get('questions') or []],
        'consents': [
            {
                'id': consent.get('id'),
                'label': consent.get('label'),
                'consent': localized(consent.get('consent'), locale),
                'consent_localized': localized_json(consent.get('consent')),
                'check_required': consent.get('checkRequired'),
            }
            for consent in legal.get('consents') or []
        ],
    }


def lead_form_response_record(response):
    """One leadFormResponses element as a flat row (Fivetran's LEAD_FORM_RESPONSE columns,
    plus the campaign and creative), with the answers and consent responses as nested
    arrays (Fivetran's LEAD_FORM_RESPONSE_ANSWER / _MULTIPLE_CHOICE_ANSWERS / _CONSENT)."""
    owner = response.get('owner') or {}
    versioned = response.get('versionedLeadGenFormUrn')
    form_id, form_version = parse_versioned_form(versioned)
    metadata = (response.get('leadMetadata') or {}).get('sponsoredLeadMetadata') or {}
    campaign_info = ((response.get('leadMetadataInfo') or {}).get('sponsoredLeadMetadataInfo') or {}).get('campaign') or {}
    associated = response.get('associatedEntity') or {}
    associated_urn = next((value for value in associated.values() if isinstance(value, str)), None)
    form_response = response.get('formResponse') or {}
    answers = []
    for answer in form_response.get('answers') or []:
        details = answer.get('answerDetails') or {}
        text = details.get('textQuestionAnswer')
        choice = details.get('multipleChoiceAnswer')
        answers.append({
            'question_id': answer.get('questionId'),
            'name': answer.get('name'),
            'text_question_answer': (text or {}).get('answer'),
            'multiple_choice_options': list((choice or {}).get('options') or []),
        })
    return {
        'id': response.get('id'),
        'response_id': response.get('responseId'),
        'lead_type': response.get('leadType'),
        'test_lead': response.get('testLead'),
        'submitted_at': response.get('submittedAt'),
        'submitter': response.get('submitter'),
        'owner': owner.get('sponsoredAccount') or owner.get('organization'),
        'account_id': urn_id(owner.get('sponsoredAccount')),
        'sponsored_account_id': _int_or_none(urn_id(owner.get('sponsoredAccount'))),
        'organization_id': _int_or_none(urn_id(owner.get('organization'))),
        'versioned_lead_gen_form': versioned,
        'lead_form_id': form_id,
        'lead_form_version_id': form_version,
        'sponsored_lead_metadata_campaign': metadata.get('campaign'),
        'campaign_id': urn_id(metadata.get('campaign')),
        'campaign_name': campaign_info.get('name'),
        'campaign_type': campaign_info.get('type'),
        'associated_entity': associated_urn,
        'creative_id': urn_id(associated.get('associatedCreative')),
        'answers': answers,
        'consent_responses': [
            {'consent_id': c.get('consentId'), 'consent': c.get('consent'), 'accepted': c.get('accepted')}
            for c in form_response.get('consentResponses') or []
        ],
    }


def page_is_last(data, start, count):
    """True when a start/count page is the last: fewer rows than asked for, or LinkedIn's
    total says nothing is left."""
    elements = data.get('elements') or []
    total = (data.get('paging') or {}).get('total')
    if len(elements) < count:
        return True
    return isinstance(total, int) and start + len(elements) >= total
