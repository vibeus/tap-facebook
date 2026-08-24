#!/usr/bin/env python3
import json
import os
import os.path
import sys
import time

import datetime
from datetime import timezone
import dateutil

import attr
import pendulum
import requests
import backoff

import sys

import re
import singer
import singer.metrics as metrics
from singer import utils, metadata
from singer import SingerConfigurationError, SingerDiscoveryError, SingerSyncError
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer, _transform_datetime)
from singer.catalog import Catalog, CatalogEntry

from functools import partial

import facebook_business
from facebook_business import FacebookAdsApi
import facebook_business.adobjects.adcreative as adcreative
import facebook_business.adobjects.ad as fb_ad
import facebook_business.adobjects.adset as adset
import facebook_business.adobjects.campaign as fb_campaign
import facebook_business.adobjects.adsinsights as adsinsights
import facebook_business.adobjects.user as fb_user
import facebook_business.adobjects.lead as fb_lead

from facebook_business.exceptions import FacebookError, FacebookRequestError, FacebookBadObjectError

from requests.exceptions import ConnectionError, Timeout

API = None

INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60
INSIGHTS_MAX_ASYNC_SLEEP_SECONDS = 5 * 60

RESULT_RETURN_LIMIT = 100

REQUEST_TIMEOUT = 300
DEFAULT_PK_VALUE = "00:00:00 - 00:59:59"

STREAMS = [
    'adcreative',
    'ads',
    'adsets',
    'campaigns',
    'ads_insights',
    'ads_insights_age_and_gender',
    'ads_insights_country',
    'ads_insights_platform_and_device',
    'ads_insights_region',
    'ads_insights_dma',
    'ads_insights_hourly_advertiser',
    #'leads',
]

REQUIRED_CONFIG_KEYS = ['start_date', 'account_id', 'access_token']
UPDATED_TIME_KEY = 'updated_time'
CREATED_TIME_KEY = 'created_time'
START_DATE_KEY = 'date_start'

BOOKMARK_KEYS = {
    'ads': UPDATED_TIME_KEY,
    'adsets': UPDATED_TIME_KEY,
    'campaigns': UPDATED_TIME_KEY,
    'ads_insights': START_DATE_KEY,
    'ads_insights_age_and_gender': START_DATE_KEY,
    'ads_insights_country': START_DATE_KEY,
    'ads_insights_platform_and_device': START_DATE_KEY,
    'ads_insights_region': START_DATE_KEY,
    'ads_insights_dma': START_DATE_KEY,
    'ads_insights_hourly_advertiser': START_DATE_KEY,
    'leads': CREATED_TIME_KEY,
}

LOGGER = singer.get_logger()

CONFIG = {}

def _install_response_header_logger():
    """
    Monkey-patch FacebookAdsApi.call to log response headers for the
    three ads_insights-related API calls.

    Header selection:
      - POST /act_X/insights   (job submit): x-business-use-case-usage, x-fb-ads-insights-throttle
      - GET  /{job_id}          (job poll):  x-business-use-case-usage
      - GET  /{job_id}/insights (result):    x-business-use-case-usage
    """
    import facebook_business.api as _fb_api
    if getattr(_fb_api.FacebookAdsApi.call, '_vibeus_header_logger', False):
        return  # already patched; idempotent

    _original_call = _fb_api.FacebookAdsApi.call

    def _logged_call(self, method, path, params=None, headers=None, files=None,
                     url_override=None, api_version=None):
        response = _original_call(self, method, path, params, headers, files,
                                  url_override, api_version)

        path_str = "/".join(str(p) for p in path) if not isinstance(path, str) else path

        is_submit = method == 'POST' and path_str.endswith('/insights')
        is_result = method == 'GET'  and path_str.endswith('/insights')
        is_poll   = method == 'GET'  and '/insights' not in path_str

        if is_submit or is_result or is_poll:
            try:
                hdrs = dict(response.headers()) if response.headers() else {}
            except Exception:
                hdrs = {}

            if is_submit:
                tag = 'SUBMIT'
                keys = ('x-business-use-case-usage', 'x-fb-ads-insights-throttle')
            else:
                tag = 'POLL' if is_poll else 'RESULT'
                keys = ('x-business-use-case-usage',)

            subset = {k: hdrs[k] for k in keys if k in hdrs}
            LOGGER.info(
                "API RESP [%s] %s %s -> HTTP %s | watched: %s",
                tag, method, path_str,
                getattr(response, '_http_status', '?'),
                subset if subset else '(none)')

        return response

    _logged_call._vibeus_header_logger = True
    _fb_api.FacebookAdsApi.call = _logged_call


def retry_on_summary_param_error(backoff_type, exception, **wait_gen_kwargs):
    """
    At times, the Facebook Graph API exhibits erratic behavior, 
    triggering errors related to the Summary parameter with a status code of 400. 
    However, upon retrying, the API functions as expected.
    """
    def log_retry_attempt(details):
        _, exception, _ = sys.exc_info()
        LOGGER.info('Caught Summary param error after %s tries. Waiting %s more seconds then retrying...',
                    details["tries"],
                    details["wait"])

    def should_retry_api_error(exception):

        # Define the regular expression pattern
        pattern = r'\(#100\) Cannot include [\w, ]+ in summary param because they weren\'t there while creating the report run(?:\. All available values are: )?'
        if isinstance(exception, FacebookRequestError):
            return (exception.http_status()==400 and re.match(pattern, exception._error['message']))
        return False

    return backoff.on_exception(
        backoff_type,
        exception,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=lambda exc: not should_retry_api_error(exc),
        **wait_gen_kwargs
    )

original_call = FacebookAdsApi.call

@retry_on_summary_param_error(backoff.expo, (FacebookRequestError), max_tries=5, factor=5)
def call_with_retry(self, method, path, params=None, headers=None, files=None, url_override=None, api_version=None,):
    """
    Adding the retry decorator on the original function call
    """
    return original_call(
        self,
        method,
        path,
        params,
        headers,
        files,
        url_override,
        api_version,)

FacebookAdsApi.call = call_with_retry


class TapFacebookException(Exception):
    pass

class InsightsJobTimeout(TapFacebookException):
    pass

def transform_datetime_string(dts):
    parsed_dt = dateutil.parser.parse(dts)
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    else:
        parsed_dt = parsed_dt.astimezone(timezone.utc)
    return singer.strftime(parsed_dt)

def iter_delivery_info_filter(stream_type):
    filt = {
        "field": stream_type + ".delivery_info",
        "operator": "IN",
    }

    filt_values = [
        "active", "archived", "completed",
        "limited", "not_delivering", "deleted",
        "not_published", "pending_review", "permanently_deleted",
        "recently_completed", "recently_rejected", "rejected",
        "scheduled", "inactive"]

    sub_list_length = 3
    for i in range(0, len(filt_values), sub_list_length):
        filt['value'] = filt_values[i:i+sub_list_length]
        yield filt

def raise_from(singer_error, fb_error):
    """Makes a pretty error message out of FacebookError object

    FacebookRequestError is the only class with more info than the exception string so we pull more
    info out of it
    """
    if isinstance(fb_error, FacebookRequestError):
        http_method = fb_error.request_context().get('method', 'Unknown HTTP Method')
        error_message = '{}: {} Message: {}'.format(
            http_method,
            fb_error.http_status(),
            fb_error.body().get('error', {}).get('message') 
                if isinstance(fb_error.body(), dict) else str(fb_error.body())
        )
    else:
        # All other facebook errors are `FacebookError`s and we handle
        # them the same as a python error
        error_message = str(fb_error)
    raise singer_error(error_message) from fb_error

def retry_pattern(backoff_type, exception, **wait_gen_kwargs):
    def log_retry_attempt(details):
        _, exception, _ = sys.exc_info()
        LOGGER.info(exception)
        LOGGER.info('Caught retryable error after %s tries. Waiting %s more seconds then retrying...',
                    details["tries"],
                    details["wait"])

        if isinstance(exception, TypeError) and str(exception) == "string indices must be integers":
            LOGGER.info('TypeError due to bad JSON response')
    def should_retry_api_error(exception):
        if isinstance(exception, FacebookBadObjectError) or isinstance(exception, Timeout) or isinstance(exception, ConnectionError) or isinstance(exception, AttributeError):
            return True
        elif isinstance(exception, FacebookRequestError):
            return (exception.api_transient_error()
                    or exception.api_error_subcode() == 99
                    or exception.http_status() in (500, 503)
                    # This subcode corresponds to a race condition between AdsInsights job creation and polling
                    or exception.api_error_subcode() == 33
                    )
        elif isinstance(exception, InsightsJobTimeout):
            return True
        elif isinstance(exception, TypeError) and str(exception) == "string indices must be integers":
            return True
        return False

    return backoff.on_exception(
        backoff_type,
        exception,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=lambda exc: not should_retry_api_error(exc),
        **wait_gen_kwargs
    )

@attr.s
class Stream(object):
    name = attr.ib()
    account = attr.ib()
    stream_alias = attr.ib()
    catalog_entry = attr.ib()
    replication_method = 'FULL_TABLE'

    def automatic_fields(self):
        fields = set()
        if self.catalog_entry:
            props = metadata.to_map(self.catalog_entry.metadata)
            for breadcrumb, data in props.items():
                if len(breadcrumb) != 2:
                    continue # Skip root and nested metadata

                if data.get('inclusion') == 'automatic':
                    fields.add(breadcrumb[1])
        return fields


    def fields(self):
        fields = set()
        if self.catalog_entry:
            props = metadata.to_map(self.catalog_entry.metadata)
            for breadcrumb, data in props.items():
                if len(breadcrumb) != 2:
                    continue # Skip root and nested metadata

                if data.get('selected') or data.get('inclusion') == 'automatic':
                    fields.add(breadcrumb[1])
        return fields

@attr.s
class IncrementalStream(Stream):

    state = attr.ib()
    replication_method = 'INCREMENTAL'

    def __attrs_post_init__(self):
        self.current_bookmark = get_start(self, UPDATED_TIME_KEY)

    def _iterate(self, generator, record_preparation):
        max_bookmark = None
        for recordset in generator:
            for record in recordset:
                updated_at = pendulum.parse(record[UPDATED_TIME_KEY])

                if self.current_bookmark and self.current_bookmark >= updated_at:
                    continue
                if not max_bookmark or updated_at > max_bookmark:
                    max_bookmark = updated_at

                time.sleep(15)
                record = record_preparation(record)
                yield {'record': record}

            if max_bookmark:
                yield {'state': advance_bookmark(self, UPDATED_TIME_KEY, str(max_bookmark))}


def batch_record_success(response, stream=None, transformer=None, schema=None):
    '''A success callback for the FB Batch endpoint used when syncing AdCreatives. Needs the stream
    to resolve schema refs and transform the successful response object.'''
    rec = response.json()
    record = transformer.transform(rec, schema)
    singer.write_record(stream.name, record, stream.stream_alias, utils.now())


def batch_record_failure(response):
    '''A failure callback for the FB Batch endpoint used when syncing AdCreatives. Raises the error
    so it fails the sync process.'''
    raise response.error()

# AdCreative is not an iterable stream as it uses the batch endpoint
class AdCreative(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
    '''

    # Added retry_pattern to handle AttributeError raised from api_batch.execute() below
    @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
    def sync_batches(self, stream_objects):
        refs = load_shared_schema_refs()
        schema = singer.resolve_schema_references(self.catalog_entry.schema.to_dict(), refs)
        transformer = Transformer(pre_hook=transform_date_hook)

        # Create the initial batch
        api_batch = API.new_batch()
        batch_count = 0

        # This loop syncs minimal fb objects
        for obj in stream_objects:
            # Execute and create a new batch for every 50 added
            if batch_count % 50 == 0:
                api_batch.execute()
                api_batch = API.new_batch()

            # Add a call to the batch with the full object
            obj.api_get(fields=self.fields(),
                        batch=api_batch,
                        success=partial(batch_record_success, stream=self, transformer=transformer, schema=schema),
                        failure=batch_record_failure)
            batch_count += 1

        # Ensure the final batch is executed
        api_batch.execute()

    key_properties = ['id']

    @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
    # Added retry_pattern to handle AttributeError raised from account.get_ad_creatives() below
    @retry_pattern(backoff.expo, (FacebookRequestError, TypeError, AttributeError), max_tries=5, factor=5)
    def get_adcreatives(self):
        time.sleep(15)
        return self.account.get_ad_creatives(params={'limit': RESULT_RETURN_LIMIT})

    def sync(self):
        adcreatives = self.get_adcreatives()
        self.sync_batches(adcreatives)


class Ads(IncrementalStream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup
    '''

    key_properties = ['id', 'updated_time']

    @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
    # Added retry_pattern to handle AttributeError raised from account.get_ads() below
    @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
    def _call_get_ads(self, params):
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        return self.account.get_ads(fields=self.automatic_fields(), params=params) # pylint: disable=no-member

    def __iter__(self):
        def do_request():
            params = {'limit': RESULT_RETURN_LIMIT}
            if self.current_bookmark:
                params.update({'filtering': [{'field': 'ad.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp}]})
            yield self._call_get_ads(params)

        def do_request_multiple():
            params = {'limit': RESULT_RETURN_LIMIT}
            bookmark_params = []
            if self.current_bookmark:
                bookmark_params.append({'field': 'ad.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp})
            for del_info_filt in iter_delivery_info_filter('ad'):
                params.update({'filtering': [del_info_filt] + bookmark_params})
                filt_ads = self._call_get_ads(params)
                yield filt_ads

        @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
        # Added retry_pattern to handle AttributeError raised from ad.api_get() below
        @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
        def prepare_record(ad):
            return ad.api_get(fields=self.fields()).export_all_data()

        if CONFIG.get('include_deleted', 'false').lower() == 'true':
            ads = do_request_multiple()
        else:
            ads = do_request()
        COUNT = 0
        for message in self._iterate(ads, prepare_record):
            COUNT += 1
            LOGGER.info(f"---------------------- Ads api request count {COUNT} ----------------------")
            yield message


class AdSets(IncrementalStream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign
    '''

    key_properties = ['id', 'updated_time']

    @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
    # Added retry_pattern to handle AttributeError raised from account.get_ad_sets() below
    @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
    def _call_get_ad_sets(self, params):
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        return self.account.get_ad_sets(fields=self.automatic_fields(), params=params) # pylint: disable=no-member

    def __iter__(self):
        def do_request():
            params = {'limit': RESULT_RETURN_LIMIT}
            if self.current_bookmark:
                params.update({'filtering': [{'field': 'adset.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp}]})
            yield self._call_get_ad_sets(params)

        def do_request_multiple():
            params = {'limit': RESULT_RETURN_LIMIT}
            bookmark_params = []
            if self.current_bookmark:
                bookmark_params.append({'field': 'adset.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp})
            for del_info_filt in iter_delivery_info_filter('adset'):
                params.update({'filtering': [del_info_filt] + bookmark_params})
                filt_adsets = self._call_get_ad_sets(params)
                yield filt_adsets

        @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
        # Added retry_pattern to handle AttributeError raised from ad_set.api_get() below
        @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
        def prepare_record(ad_set):
            return ad_set.api_get(fields=self.fields()).export_all_data()

        if CONFIG.get('include_deleted', 'false').lower() == 'true':
            ad_sets = do_request_multiple()
        else:
            ad_sets = do_request()
        COUNT = 0
        for message in self._iterate(ad_sets, prepare_record):
            COUNT += 1
            LOGGER.info(f"---------------------- AdSets api request count {COUNT} ----------------------")
            yield message

class Campaigns(IncrementalStream):

    key_properties = ['id']

    @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
    # Added retry_pattern to handle AttributeError raised from account.get_campaigns() below
    @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
    def _call_get_campaigns(self, params):
        """
        This is necessary because the functions that call this endpoint return
        a generator, whose calls need decorated with a backoff.
        """
        return self.account.get_campaigns(fields=self.automatic_fields(), params=params) # pylint: disable=no-member

    def __iter__(self):
        # ads is not a field under campaigns in the SDK. To add ads to this stream, we have to make a separate request
        props = self.fields()
        fields = [k for k in props if k != 'ads']
        pull_ads = 'ads' in props

        def do_request():
            params = {'limit': RESULT_RETURN_LIMIT}
            if self.current_bookmark:
                params.update({'filtering': [{'field': 'campaign.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp}]})
            yield self._call_get_campaigns(params)

        def do_request_multiple():
            params = {'limit': RESULT_RETURN_LIMIT}
            bookmark_params = []
            if self.current_bookmark:
                bookmark_params.append({'field': 'campaign.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp})
            for del_info_filt in iter_delivery_info_filter('campaign'):
                params.update({'filtering': [del_info_filt] + bookmark_params})
                filt_campaigns = self._call_get_campaigns(params)
                yield filt_campaigns

        @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
        # Added retry_pattern to handle AttributeError raised from request call below
        @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
        def prepare_record(campaign):
            """If campaign.ads is selected, make the request and insert the data here"""
            campaign_out = campaign.api_get(fields=fields).export_all_data()
            if pull_ads:
                campaign_out['ads'] = {'data': []}
                ids = [ad['id'] for ad in campaign.get_ads()]
                for ad_id in ids:
                    campaign_out['ads']['data'].append({'id': ad_id})
            return campaign_out

        if CONFIG.get('include_deleted', 'false').lower() == 'true':
            campaigns = do_request_multiple()
        else:
            campaigns = do_request()
        COUNT = 0
        for message in self._iterate(campaigns, prepare_record):
            COUNT += 1
            LOGGER.info(f"---------------------- Campaigns api request count {COUNT} ----------------------")
            yield message

@attr.s
class Leads(Stream):
    state = attr.ib()
    replication_key = "created_time"

    key_properties = ['id']
    replication_method = 'INCREMENTAL'

    def compare_lead_created_times(self, leadA, leadB):
        if leadA is None:
            return leadB
        timestampA = pendulum.parse(leadA[self.replication_key])
        timestampB = pendulum.parse(leadB[self.replication_key])
        if timestampB > timestampA:
            return leadB
        else:
            return leadA

    # Added retry_pattern to handle AttributeError raised from api_batch.execute() below
    @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
    def sync_batches(self, stream_objects):
        refs = load_shared_schema_refs()
        schema = singer.resolve_schema_references(self.catalog_entry.schema.to_dict(), refs)
        transformer = Transformer(pre_hook=transform_date_hook)

        # Create the initial batch
        api_batch = API.new_batch()
        batch_count = 0

        # Keep track of most recent, lead for bookmarking
        latest_lead = None

        # This loop syncs minimal fb objects
        for obj in stream_objects:

            latest_lead = self.compare_lead_created_times(latest_lead, obj)

            # Execute and create a new batch for every 50 added
            if batch_count % 50 == 0:
                api_batch.execute()
                api_batch = API.new_batch()

            # Add a call to the batch with the full object
            obj.api_get(fields=self.fields(),
                        batch=api_batch,
                        success=partial(batch_record_success, stream=self, transformer=transformer, schema=schema),
                        failure=batch_record_failure)
            batch_count += 1

        # Ensure the final batch is executed
        api_batch.execute()
        return str(pendulum.parse(latest_lead[self.replication_key]))

    @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
    # Added retry_pattern to handle AttributeError raised from account.get_ads() below
    @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
    def get_ads(self):
        params = {'limit': RESULT_RETURN_LIMIT}
        yield from self.account.get_ads(params=params)

    @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
    # Added retry_pattern to handle AttributeError raised from ad.get_leads() below
    @retry_pattern(backoff.expo, (FacebookRequestError, AttributeError), max_tries=5, factor=5)
    def get_leads(self, ads, start_time, previous_start_time):
        start_time = int(start_time.timestamp()) # Get unix timestamp
        params = {'limit': RESULT_RETURN_LIMIT,
                  'filtering': [{'field': 'time_created',
                                  'operator': 'GREATER_THAN',
                                  'value': previous_start_time - 1},
                                {'field': 'time_created',
                                  'operator': 'LESS_THAN',
                                  'value': start_time}]}
        for ad in ads:
            yield from ad.get_leads(params=params)

    def sync(self):
        start_time = pendulum.utcnow()
        previous_start_time = self.state.get("bookmarks", {}).get("leads", {}).get(self.replication_key, CONFIG.get('start_date'))

        previous_start_time = pendulum.parse(previous_start_time)
        ads = self.get_ads()
        leads = self.get_leads(ads, start_time, int(previous_start_time.timestamp()))
        latest_lead_time = self.sync_batches(leads)

        if not latest_lead_time is None:
            singer.write_bookmark(self.state, 'leads', self.replication_key, latest_lead_time)
            singer.write_state(self.state)


ALL_ACTION_ATTRIBUTION_WINDOWS = [
    '1d_click',
    '7d_click',
    '28d_click',
    '1d_view',
    '7d_view',
    '28d_view'
]

ALL_ACTION_BREAKDOWNS = [
    'action_type',
    'action_target_id',
    'action_destination'
]

def _normalize_account_ids(raw):
    """Accept either a single string or a list of strings for config.account_id."""
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list) and all(isinstance(s, str) and s for s in raw):
        if not raw:
            raise TapFacebookException("account_id list is empty")
        return list(raw)
    raise SingerConfigurationError(
        "config.account_id must be a string or a list of strings, got: {!r}".format(raw))

def _read_bookmark(state, stream_name, account_id, replication_key):
    """
    Read a bookmark value in new shape (state.bookmarks[stream_name][account_id][replication_key])
    to adapt to multi-account data pulling.
    Legacy shape (state.bookmarks[stream_name][replication_key]) will not be supported.
    A manual edit of the state file in AWS S3 might be needed.
    """
    bms = (state or {}).get('bookmarks') or {}
    stream_bms = bms.get(stream_name) or {}
    per_account = stream_bms.get(account_id)
    if isinstance(per_account, dict) and replication_key in per_account:
        return per_account[replication_key]
    return None

def _write_bookmark(state, stream_name, account_id, replication_key, value):
    """Write a bookmark value in the new shape."""
    state = state if state is not None else {}
    bms = state.setdefault('bookmarks', {})
    stream_bms = bms.setdefault(stream_name, {})
    per_account = stream_bms.setdefault(account_id, {})
    per_account[replication_key] = value
    return state

def get_start(stream, bookmark_key):
    tap_stream_id = stream.name
    state = stream.state or {}
    account_id = stream.account['account_id']
    current_bookmark = _read_bookmark(state, tap_stream_id, account_id, bookmark_key)
    if current_bookmark is None:
        if isinstance(stream, IncrementalStream):
            return None
        else:
            LOGGER.info("no bookmark found for %s/%s, using start_date instead...%s",
                        tap_stream_id, account_id, CONFIG['start_date'])
            return pendulum.parse(CONFIG['start_date'])
    LOGGER.info("found current bookmark for %s/%s:  %s",
                tap_stream_id, account_id, current_bookmark)
    return pendulum.parse(current_bookmark)

def advance_bookmark(stream, bookmark_key, date):
    tap_stream_id = stream.name
    state = stream.state or {}
    account_id = stream.account['account_id']
    LOGGER.info('advance(%s/%s, %s)', tap_stream_id, account_id, date)
    date = pendulum.parse(date) if date else None
    current_bookmark = get_start(stream, bookmark_key)

    if date is None:
        LOGGER.info('Did not get a date for stream %s/%s, ' +
                    'not advancing bookmark',
                    tap_stream_id, account_id)
        return state
    if not current_bookmark or date > current_bookmark:
        LOGGER.info('Bookmark for stream %s/%s is currently %s, ' +
                    'advancing to %s',
                    tap_stream_id, account_id, current_bookmark, date)
        return _write_bookmark(state, tap_stream_id, account_id, bookmark_key, str(date))
    LOGGER.info('Bookmark for stream %s/%s is currently %s, ' +
                'not changing to %s',
                tap_stream_id, account_id, current_bookmark, date)
    return state

@attr.s
class AdsInsights(Stream):
    base_properties = ['campaign_id', 'adset_id', 'ad_id', 'date_start']
    replication_method = 'INCREMENTAL'

    state = attr.ib()
    options = attr.ib()
    action_breakdowns = attr.ib(default=ALL_ACTION_BREAKDOWNS)
    level = attr.ib(default='ad')
    action_attribution_windows = attr.ib(
        default=ALL_ACTION_ATTRIBUTION_WINDOWS)
    time_increment = attr.ib(default=1)
    limit = attr.ib(default=RESULT_RETURN_LIMIT)

    bookmark_key = START_DATE_KEY

    # these fields are not defined in the facebook_business library
    # Sending these fields is not allowed, but they are returned by the api
    invalid_insights_fields = ['impression_device', 'publisher_platform', 'platform_position',
                               'age', 'gender', 'country', 'placement', 'region', 'dma', 'hourly_stats_aggregated_by_advertiser_time_zone']
    FACEBOOK_INSIGHTS_RETENTION_PERIOD = 37 # months

    # pylint: disable=no-member,unsubscriptable-object,attribute-defined-outside-init
    def __attrs_post_init__(self):
        self.breakdowns = self.options.get('breakdowns') or []
        self.key_properties = self.base_properties[:]
        if self.options.get('primary-keys'):
            self.key_properties.extend(self.options['primary-keys'])

        self.buffer_days = CONFIG.get('buffer_days')
        if CONFIG.get('insights_buffer_days'):
            self.buffer_days = int(CONFIG.get('insights_buffer_days'))
            # attribution window should only be 1, 7 or 28
            if self.buffer_days not in [1, 7, 28]:
                raise Exception("The attribution window must be 1, 7 or 28.")

    def job_params(self):
        start_date = get_start(self, self.bookmark_key)

        buffered_start_date = start_date.subtract(days=self.buffer_days)
        min_start_date = pendulum.today(pendulum.timezone('UTC')).subtract(months=self.FACEBOOK_INSIGHTS_RETENTION_PERIOD)
        if buffered_start_date < min_start_date:
            LOGGER.warning("%s: Start date is earlier than %s months ago, using %s instead. "
                           "For more information, see https://www.facebook.com/business/help/1695754927158071?id=354406972049255",
                        self.catalog_entry.tap_stream_id,
                        self.FACEBOOK_INSIGHTS_RETENTION_PERIOD,
                        min_start_date.to_date_string())
            buffered_start_date = min_start_date

        end_date = min(buffered_start_date.add(days=self.buffer_days + 7), pendulum.now(pendulum.timezone('UTC')))
        if CONFIG.get('end_date'):
            end_date = pendulum.parse(CONFIG.get('end_date'))

        # Some automatic fields (primary-keys) cannot be used as 'fields' query params.
        while buffered_start_date <= end_date:
            yield {
                'level': self.level,
                'action_breakdowns': list(self.action_breakdowns),
                'breakdowns': list(self.breakdowns),
                'limit': self.limit,
                'fields': list(self.fields().difference(self.invalid_insights_fields)),
                'time_increment': self.time_increment,
                'action_attribution_windows': list(self.action_attribution_windows),
                'time_ranges': [{'since': buffered_start_date.to_date_string(),
                                 'until': buffered_start_date.to_date_string()}]
            }
            buffered_start_date = buffered_start_date.add(days=1)

    @staticmethod
    @retry_pattern(backoff.constant, FacebookRequestError, max_tries=5, interval=1)
    def __api_get_with_retry(job):
        job = job.api_get()
        return job

    @retry_pattern(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
    # Added retry_pattern to handle AttributeError raised from requests call below
    @retry_pattern(backoff.expo, (FacebookRequestError, InsightsJobTimeout, FacebookBadObjectError, TypeError, AttributeError), max_tries=5, factor=5)
    def run_job(self, params):
        LOGGER.info('Starting adsinsights job with params %s', params)
        job = self.account.get_insights( # pylint: disable=no-member
            params=params,
            is_async=True)
        status = None
        time_start = time.time()
        sleep_time = 15#10
        while status != "Job Completed":
            duration = time.time() - time_start
            job = AdsInsights.__api_get_with_retry(job)
            status = job['async_status']
            percent_complete = job['async_percent_completion']

            job_id = job['id']
            LOGGER.info('%s, %d%% done', status, percent_complete)

            if status == "Job Completed":
                return job

            if status == "Job Failed":
                error_code = job.get('error_code')
                error_message = job.get('error_message')
                error_subcode = job.get('error_subcode')
                error_user_title = job.get('error_user_title')
                error_user_msg = job.get('error_user_msg')
                raise TapFacebookException(
                    'Insights job {} failed. error_code={}, error_subcode={}, '
                    'error_user_title={}, error_user_msg={}, error_message={}'.format(
                        job_id, error_code, error_subcode,
                        error_user_title, error_user_msg, error_message))

            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                pretty_error_message = ('Insights job {} did not start after {} seconds. ' +
                                        'This is an intermittent error and may resolve itself on subsequent queries to the Facebook API. ' +
                                        'You should deselect fields from the schema that are not necessary, ' +
                                        'as that may help improve the reliability of the Facebook API.')
                raise InsightsJobTimeout(pretty_error_message.format(job_id, INSIGHTS_MAX_WAIT_TO_START_SECONDS))
            elif duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS and status != "Job Completed":
                pretty_error_message = ('Insights job {} did not complete after {} seconds. ' +
                                        'This is an intermittent error and may resolve itself on subsequent queries to the Facebook API. ' +
                                        'You should deselect fields from the schema that are not necessary, ' +
                                        'as that may help improve the reliability of the Facebook API.')
                raise InsightsJobTimeout(pretty_error_message.format(job_id,
                                                                     INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS//60))

            LOGGER.info("sleeping for %d seconds until job is done", sleep_time)
            time.sleep(sleep_time)
            if sleep_time < INSIGHTS_MAX_ASYNC_SLEEP_SECONDS:
                sleep_time = 2 * sleep_time
        return job

    def __iter__(self):
        for params in self.job_params():
            with metrics.job_timer('insights'):
                job = self.run_job(params)

            min_date_start_for_job = None
            count = 0
            for obj in job.get_result():
                count += 1
                rec = obj.export_all_data()
                if not min_date_start_for_job or rec['date_stop'] < min_date_start_for_job:
                    min_date_start_for_job = rec['date_stop']

                # When the impressions count remains 0 for the specific campaign throughout the day,
                # the API does not return hourly_stats_aggregated_by_advertiser_time_zone in the response.
                # As it is one of the primary keys, we need to ensure it is present.
                if self.name == "ads_insights_hourly_advertiser" and "hourly_stats_aggregated_by_advertiser_time_zone" not in rec:
                    rec["hourly_stats_aggregated_by_advertiser_time_zone"] = DEFAULT_PK_VALUE

                yield {'record': rec}
            LOGGER.info('Got %d results for insights job', count)

            # when min_date_start_for_job stays None, we should
            # still update the bookmark using 'until' in time_ranges
            if min_date_start_for_job is None:
                for time_range in params['time_ranges']:
                    if time_range['until']:
                        min_date_start_for_job = time_range['until']
            yield {'state': advance_bookmark(self, self.bookmark_key,
                                             min_date_start_for_job)} # pylint: disable=no-member


INSIGHTS_BREAKDOWNS_OPTIONS = {
    'ads_insights': {"breakdowns": []},
    'ads_insights_age_and_gender': {"breakdowns": ['age', 'gender'],
                                    "primary-keys": ['age', 'gender']},
    'ads_insights_country': {"breakdowns": ['country'],
                             "primary-keys": ['country']},
    'ads_insights_platform_and_device': {"breakdowns": ['publisher_platform',
                                                        'platform_position', 'impression_device'],
                                         "primary-keys": ['publisher_platform',
                                                          'platform_position', 'impression_device']},
    'ads_insights_region': {'breakdowns': ['region'],
                            'primary-keys': ['region']},
    'ads_insights_dma': {"breakdowns": ['dma'],
                         "primary-keys": ['dma']},
    'ads_insights_hourly_advertiser': {'breakdowns': ['hourly_stats_aggregated_by_advertiser_time_zone'],
                                       "primary-keys": ['hourly_stats_aggregated_by_advertiser_time_zone']},
}


def initialize_stream(account, catalog_entry, state): # pylint: disable=too-many-return-statements

    name = catalog_entry.stream
    stream_alias = catalog_entry.stream_alias

    if name in INSIGHTS_BREAKDOWNS_OPTIONS:
        return AdsInsights(name, account, stream_alias, catalog_entry, state=state,
                           options=INSIGHTS_BREAKDOWNS_OPTIONS[name])
    elif name == 'campaigns':
        return Campaigns(name, account, stream_alias, catalog_entry, state=state)
    elif name == 'adsets':
        return AdSets(name, account, stream_alias, catalog_entry, state=state)
    elif name == 'ads':
        return Ads(name, account, stream_alias, catalog_entry, state=state)
    elif name == 'adcreative':
        return AdCreative(name, account, stream_alias, catalog_entry)
    elif name == 'leads':
        return Leads(name, account, stream_alias, catalog_entry, state=state)
    else:
        raise TapFacebookException('Unknown stream {}'.format(name))


def get_streams_to_sync(account, catalog, state):
    streams = []
    for stream in STREAMS:
        catalog_entry = next((s for s in catalog.streams if s.tap_stream_id == stream), None)
        if catalog_entry and catalog_entry.is_selected():
            # TODO: Don't need name and stream_alias since it's on catalog_entry
            name = catalog_entry.stream
            stream_alias = catalog_entry.stream_alias
            streams.append(initialize_stream(account, catalog_entry, state))
    return streams

def transform_date_hook(data, typ, schema):
    if typ == 'string' and schema.get('format') == 'date-time' and isinstance(data, str):
        transformed = transform_datetime_string(data)
        return transformed
    return data

def do_sync(account, catalog, state):
    streams_to_sync = get_streams_to_sync(account, catalog, state)
    refs = load_shared_schema_refs()
    for stream in streams_to_sync:
        LOGGER.info('Syncing %s, fields %s', stream.name, stream.fields())
        schema = singer.resolve_schema_references(load_schema(stream), refs)
        metadata_map = metadata.to_map(stream.catalog_entry.metadata)
        bookmark_key = BOOKMARK_KEYS.get(stream.name)
        singer.write_schema(stream.name, schema, stream.key_properties, bookmark_key, stream.stream_alias)


        # NB: The AdCreative stream is not an iterator
        if stream.name in {'adcreative', 'leads'}:
            stream.sync()
            continue

        with Transformer(pre_hook=transform_date_hook) as transformer:
            with metrics.record_counter(stream.name) as counter:
                for message in stream:
                    if 'record' in message:
                        counter.increment()
                        time_extracted = utils.now()
                        record = transformer.transform(message['record'], schema, metadata=metadata_map)
                        singer.write_record(stream.name, record, stream.stream_alias, time_extracted)
                    elif 'state' in message:
                        singer.write_state(message['state'])
                    else:
                        raise TapFacebookException('Unrecognized message {}'.format(message))


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(stream):
    path = get_abs_path('schemas/{}.json'.format(stream.name))
    schema = utils.load_json(path)

    return schema


def initialize_streams_for_discovery(): # pylint: disable=invalid-name
    return [initialize_stream(None, CatalogEntry(stream=name), None)
            for name in STREAMS]

def discover_schemas():
    # Load Facebook's shared schemas
    refs = load_shared_schema_refs()

    result = {'streams': []}
    streams = initialize_streams_for_discovery()
    for stream in streams:
        LOGGER.info('Loading schema for %s', stream.name)
        schema = singer.resolve_schema_references(load_schema(stream), refs)

        bookmark_key = BOOKMARK_KEYS.get(stream.name)

        mdata = metadata.to_map(metadata.get_standard_metadata(schema,
                                               key_properties=stream.key_properties,
                                               replication_method=stream.replication_method,
                                               valid_replication_keys=[bookmark_key] if bookmark_key else None))

        if bookmark_key == UPDATED_TIME_KEY or bookmark_key == CREATED_TIME_KEY :
            mdata = metadata.write(mdata, ('properties', bookmark_key), 'inclusion', 'automatic')

        result['streams'].append({'stream': stream.name,
                                  'tap_stream_id': stream.name,
                                  'schema': schema,
                                  'metadata': metadata.to_list(mdata)})
    return result

def load_shared_schema_refs():
    shared_schemas_path = get_abs_path('schemas/shared')

    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs[shared_file] = json.load(data_file)

    return shared_schema_refs

def do_discover():
    LOGGER.info('Loading schemas')
    json.dump(discover_schemas(), sys.stdout, indent=4)


def main_impl():
    try:
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)
        access_token = args.config['access_token']
        account_ids = _normalize_account_ids(args.config['account_id'])

        CONFIG.update(args.config)

        global RESULT_RETURN_LIMIT
        RESULT_RETURN_LIMIT = CONFIG.get('result_return_limit', RESULT_RETURN_LIMIT)

        # Set request timeout with config param `request_timeout`.
        config_request_timeout = CONFIG.get('request_timeout')
        if config_request_timeout and float(config_request_timeout):
            request_timeout = float(config_request_timeout)
        else:
            request_timeout = REQUEST_TIMEOUT # If value is 0,"0","" or not passed then set default to 300 seconds.

        # Discover all ad accounts reachable via this access token (one-shot).
        global API
        API = FacebookAdsApi.init(access_token=access_token, timeout=request_timeout)
        _install_response_header_logger()  # patch FacebookAdsApi.call for insights header logging
        user = fb_user.User(fbid='me')
        all_accounts = {acc['account_id']: acc for acc in user.get_ad_accounts()}

        # Match requested account_ids against visible accounts.
        selected_accounts = []
        missing = []
        for aid in account_ids:
            if aid in all_accounts:
                selected_accounts.append(all_accounts[aid])
            else:
                missing.append(aid)
        if missing:
            raise SingerConfigurationError(
                "Couldn't find account(s): {}".format(", ".join(missing)))
        if not selected_accounts:
            raise SingerConfigurationError(
                "No valid ad accounts selected from account_id={!r}".format(account_ids))
    except FacebookError as fb_error:
        raise_from(SingerConfigurationError, fb_error)

    if args.discover:
        try:
            do_discover()
        except FacebookError as fb_error:
            raise_from(SingerDiscoveryError, fb_error)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        try:
            for account in selected_accounts:
                LOGGER.info("=== Syncing account %s ===", account['account_id'])
                API = FacebookAdsApi.init(access_token=access_token,
                                          timeout=request_timeout)
                do_sync(account, catalog, args.state)
        except FacebookError as fb_error:
            raise_from(SingerSyncError, fb_error)
    else:
        LOGGER.info("No properties were selected")

def main():

    try:
        main_impl()
    except TapFacebookException as e:
        LOGGER.critical(e)
        sys.exit(1)
    except Exception as e:
        LOGGER.exception(e)
        for line in str(e).splitlines():
            LOGGER.critical(line)
        raise e
